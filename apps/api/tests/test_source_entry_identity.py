from datetime import datetime, timedelta, timezone

import pytest
from reader_api.db import Base, engine
from reader_api.models import RawEntry, Source, SourceEntryIdentity, SourceEntryKey
from reader_api.source_entry_identity import (
    SourceEntryIdentityInput,
    SourceEntryResolutionOutcome,
    natural_source_entry_key,
    resolve_source_entry_identity,
)
from reader_api.source_entry_revision import (
    RawEntryRevisionInput,
    calculate_payload_fingerprint,
)
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker
from tests.factories import make_raw_entry


@pytest.fixture
def session() -> Session:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    with SessionLocal() as value:
        yield value


def add_source(session: Session, suffix: str = "identity") -> Source:
    source = Source(
        name=f"Source {suffix}",
        url=f"https://example.com/{suffix}.xml",
    )
    session.add(source)
    session.flush()
    return source


def add_legacy_raw(
    session: Session,
    source: Source,
    *,
    external_id: str,
    url: str = "",
    payload_fingerprint: str | None = None,
) -> RawEntry:
    raw = make_raw_entry(
        source_id=source.id,
        external_id=external_id,
        title=f"Legacy {external_id}",
        url=url,
        payload_fingerprint=payload_fingerprint,
    )
    session.add(raw)
    session.flush()
    return raw


def test_guid_key_uses_canonical_url_and_separates_reused_guid() -> None:
    first = natural_source_entry_key(
        SourceEntryIdentityInput(
            source_guid="  Release-42  ",
            url="HTTPS://EXAMPLE.COM/story/?utm_source=rss#fragment",
        )
    )
    equivalent = natural_source_entry_key(
        SourceEntryIdentityInput(
            source_guid="Release-42",
            url="https://example.com/story",
        )
    )
    reused = natural_source_entry_key(
        SourceEntryIdentityInput(
            source_guid="Release-42",
            url="https://example.com/another-story",
        )
    )
    without_url = natural_source_entry_key(
        SourceEntryIdentityInput(source_guid="Release-42")
    )

    assert first.identity_kind == "guid"
    assert first.identity_key.startswith("guid:")
    assert first == equivalent
    assert reused != first
    assert without_url != first
    assert without_url == natural_source_entry_key(
        SourceEntryIdentityInput(source_guid="Release-42", url="")
    )


def test_url_and_fallback_keys_are_stable_and_conservative() -> None:
    url_key = natural_source_entry_key(
        SourceEntryIdentityInput(
            url="HTTPS://EXAMPLE.COM/story/?gclid=tracking#fragment",
        )
    )
    equivalent_url_key = natural_source_entry_key(
        SourceEntryIdentityInput(url="https://example.com/story")
    )
    published_at = datetime(2026, 7, 10, 8, 30, tzinfo=timezone.utc)
    fallback = natural_source_entry_key(
        SourceEntryIdentityInput(
            title=" Cafe\u0301 launch ",
            author="Author",
            published_at=published_at,
        )
    )
    equivalent_fallback = natural_source_entry_key(
        SourceEntryIdentityInput(
            title="Café launch",
            author="Author",
            published_at=published_at.astimezone(
                timezone(timedelta(hours=8))
            ),
        )
    )

    assert url_key.identity_kind == "url"
    assert url_key.identity_key.startswith("url:")
    assert url_key == equivalent_url_key
    assert fallback.identity_kind == "fallback"
    assert fallback.identity_key.startswith("fallback:")
    assert fallback == equivalent_fallback
    assert fallback != natural_source_entry_key(
        SourceEntryIdentityInput(
            title="Café launch updated",
            author="Author",
            published_at=published_at,
        )
    )


def test_existing_natural_key_is_reused_without_new_mapping(
    session: Session,
) -> None:
    source = add_source(session, "existing-key")
    raw = add_legacy_raw(
        session,
        source,
        external_id="existing-guid",
        url="https://example.com/existing",
    )
    natural_key = natural_source_entry_key(
        SourceEntryIdentityInput(
            source_guid="existing-guid",
            url=raw.url,
        )
    )
    session.add(
        SourceEntryKey(
            source_entry_id=raw.source_entry_id,
            source_id=source.id,
            identity_kind=natural_key.identity_kind,
            identity_key=natural_key.identity_key,
        )
    )
    session.commit()

    result = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="existing-guid",
            url=raw.url,
        ),
    )

    assert result.outcome is SourceEntryResolutionOutcome.EXISTING
    assert result.source_entry is not None
    assert result.source_entry.id == raw.source_entry_id
    assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 2


@pytest.mark.parametrize(
    ("legacy_external_id", "incoming_url", "legacy_url"),
    [
        (
            "release-guid",
            "https://example.com/release",
            "https://example.com/release?utm_source=rss",
        ),
        (
            "release-guid:0123456789ab",
            "https://example.com/release",
            "",
        ),
    ],
)
def test_guid_uniquely_claims_exact_or_legacy_suffix_identity(
    session: Session,
    legacy_external_id: str,
    incoming_url: str,
    legacy_url: str,
) -> None:
    source = add_source(session, legacy_external_id.replace(":", "-"))
    raw = add_legacy_raw(
        session,
        source,
        external_id=legacy_external_id,
        url=legacy_url,
    )

    first = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="release-guid",
            url=incoming_url,
        ),
    )
    repeated = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="release-guid",
            url=incoming_url,
        ),
    )

    assert first.outcome is SourceEntryResolutionOutcome.CLAIMED_LEGACY
    assert first.source_entry is not None
    assert first.source_entry.id == raw.source_entry_id
    assert repeated.outcome is SourceEntryResolutionOutcome.EXISTING
    assert repeated.source_entry is not None
    assert repeated.source_entry.id == raw.source_entry_id
    assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 2


def test_guid_url_conflict_does_not_claim_legacy_identity(
    session: Session,
) -> None:
    source = add_source(session, "guid-conflict")
    raw = add_legacy_raw(
        session,
        source,
        external_id="conflict-guid",
        url="https://example.com/old-story",
    )

    result = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="conflict-guid",
            url="https://example.com/new-story",
        ),
    )

    assert result.outcome is SourceEntryResolutionOutcome.CREATE_NEW
    assert result.source_entry is None
    assert session.get(SourceEntryIdentity, raw.source_entry_id) is not None
    assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 1


def test_empty_url_legacy_identity_rejects_second_guid_generation(
    session: Session,
) -> None:
    source = add_source(session, "guid-empty-url-generation")
    raw = add_legacy_raw(
        session,
        source,
        external_id="generation-guid",
        url="",
    )

    first = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="generation-guid",
            url="https://example.com/generation-a",
        ),
    )
    second = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="generation-guid",
            url="https://example.com/generation-b",
        ),
    )

    assert first.outcome is SourceEntryResolutionOutcome.CLAIMED_LEGACY
    assert first.source_entry is not None
    assert first.source_entry.id == raw.source_entry_id
    assert second.outcome is SourceEntryResolutionOutcome.CREATE_NEW
    assert second.source_entry is None
    assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 2


def test_multiple_guid_candidates_are_ambiguous(
    session: Session,
) -> None:
    source = add_source(session, "guid-ambiguous")
    add_legacy_raw(
        session,
        source,
        external_id="ambiguous-guid",
        url="https://example.com/story",
    )
    add_legacy_raw(
        session,
        source,
        external_id="ambiguous-guid:abcdef012345",
        url="https://example.com/story",
    )

    result = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            source_guid="ambiguous-guid",
            url="https://example.com/story",
        ),
    )

    assert result.outcome is SourceEntryResolutionOutcome.CREATE_NEW
    assert result.source_entry is None
    assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 2


def test_url_claim_requires_one_canonical_match(session: Session) -> None:
    source = add_source(session, "url-claim")
    raw = add_legacy_raw(
        session,
        source,
        external_id="url-legacy",
        url="HTTPS://EXAMPLE.COM/story/?utm_medium=feed#fragment",
    )

    claimed = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(url="https://example.com/story"),
    )

    assert claimed.outcome is SourceEntryResolutionOutcome.CLAIMED_LEGACY
    assert claimed.source_entry is not None
    assert claimed.source_entry.id == raw.source_entry_id

    ambiguous_source = add_source(session, "url-ambiguous")
    add_legacy_raw(
        session,
        ambiguous_source,
        external_id="url-a",
        url="https://example.com/ambiguous",
    )
    add_legacy_raw(
        session,
        ambiguous_source,
        external_id="url-b",
        url="https://example.com/ambiguous/?fbclid=tracking",
    )

    ambiguous = resolve_source_entry_identity(
        session,
        ambiguous_source.id,
        SourceEntryIdentityInput(url="https://example.com/ambiguous"),
    )

    assert ambiguous.outcome is SourceEntryResolutionOutcome.CREATE_NEW
    assert ambiguous.source_entry is None


def test_fallback_claim_requires_one_matching_payload_fingerprint(
    session: Session,
) -> None:
    fingerprint = "a" * 64
    source = add_source(session, "fallback-claim")
    raw = add_legacy_raw(
        session,
        source,
        external_id="fallback-legacy",
        payload_fingerprint=fingerprint,
    )
    input_value = SourceEntryIdentityInput(
        title="Fallback title",
        author="Fallback author",
        published_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        payload_fingerprint=fingerprint,
    )

    claimed = resolve_source_entry_identity(
        session,
        source.id,
        input_value,
    )

    assert claimed.outcome is SourceEntryResolutionOutcome.CLAIMED_LEGACY
    assert claimed.source_entry is not None
    assert claimed.source_entry.id == raw.source_entry_id

    missing_fingerprint_source = add_source(session, "fallback-missing")
    add_legacy_raw(
        session,
        missing_fingerprint_source,
        external_id="fallback-without-fingerprint",
    )
    missing = resolve_source_entry_identity(
        session,
        missing_fingerprint_source.id,
        SourceEntryIdentityInput(
            title="Fallback title",
            author="Fallback author",
        ),
    )

    assert missing.outcome is SourceEntryResolutionOutcome.CREATE_NEW
    assert missing.source_entry is None


def test_fallback_claim_uses_persisted_canonical_fingerprint(
    session: Session,
) -> None:
    source = add_source(session, "fallback-computed")
    raw = add_legacy_raw(
        session,
        source,
        external_id="fallback-computed-legacy",
    )
    incoming_fingerprint = calculate_payload_fingerprint(
        RawEntryRevisionInput.from_raw_entry(raw)
    )
    assert raw.payload_fingerprint == incoming_fingerprint

    claimed = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            title=raw.title,
            author=raw.author,
            published_at=raw.published_at,
            payload_fingerprint=incoming_fingerprint,
        ),
    )

    assert claimed.outcome is SourceEntryResolutionOutcome.CLAIMED_LEGACY
    assert claimed.source_entry is not None
    assert claimed.source_entry.id == raw.source_entry_id
    assert raw.payload_fingerprint == incoming_fingerprint


def test_fallback_multiple_fingerprint_matches_are_ambiguous(
    session: Session,
) -> None:
    fingerprint = "b" * 64
    source = add_source(session, "fallback-ambiguous")
    add_legacy_raw(
        session,
        source,
        external_id="fallback-a",
        payload_fingerprint=fingerprint,
    )
    add_legacy_raw(
        session,
        source,
        external_id="fallback-b",
        payload_fingerprint=fingerprint,
    )

    result = resolve_source_entry_identity(
        session,
        source.id,
        SourceEntryIdentityInput(
            title="Fallback title",
            author="Fallback author",
            published_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
            payload_fingerprint=fingerprint,
        ),
    )

    assert result.outcome is SourceEntryResolutionOutcome.CREATE_NEW
    assert result.source_entry is None
    assert session.scalar(select(func.count()).select_from(SourceEntryKey)) == 2
