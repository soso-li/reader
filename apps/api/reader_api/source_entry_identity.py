from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import json
import re
import unicodedata

from sqlalchemy import select
from sqlalchemy.orm import Session

from .canonicalization import canonical_utc_timestamp
from .digest import canonical_url, normalize_space
from .models import RawEntry, SourceEntryIdentity, SourceEntryKey


MISSING_CANONICAL_URL = "__reader_missing_canonical_url__"
MISSING_PUBLISHED_AT = "__reader_missing_published_at__"
SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


class SourceEntryResolutionOutcome(Enum):
    EXISTING = "existing"
    CLAIMED_LEGACY = "claimed_legacy"
    CREATE_NEW = "create_new"


@dataclass(frozen=True)
class SourceEntryIdentityInput:
    source_guid: str = ""
    url: str = ""
    title: str = ""
    author: str = ""
    published_at: datetime | None = None
    payload_fingerprint: str = ""


@dataclass(frozen=True)
class SourceEntryNaturalKey:
    identity_kind: str
    identity_key: str


@dataclass(frozen=True)
class SourceEntryIdentityResolution:
    outcome: SourceEntryResolutionOutcome
    natural_key: SourceEntryNaturalKey
    source_entry: SourceEntryIdentity | None


@dataclass(frozen=True)
class _NormalizedIdentityInput:
    source_guid: str
    canonical_url: str
    title: str
    author: str
    published_at: str
    payload_fingerprint: str


def natural_source_entry_key(
    identity_input: SourceEntryIdentityInput,
) -> SourceEntryNaturalKey:
    """Build the deterministic natural key without touching persistence."""
    return _natural_key_from_normalized(_normalize_identity_input(identity_input))


def resolve_source_entry_identity(
    session: Session,
    source_id: int,
    identity_input: SourceEntryIdentityInput,
) -> SourceEntryIdentityResolution:
    """Resolve or uniquely claim an identity without creating a revision.

    Production ingestion handles CREATE_NEW and owns the surrounding transaction;
    this resolver only returns or claims the stable identity mapping.
    """
    normalized = _normalize_identity_input(identity_input)
    natural_key = _natural_key_from_normalized(normalized)
    existing = _identity_for_natural_key(
        session,
        source_id,
        natural_key,
    )
    if existing is not None:
        return SourceEntryIdentityResolution(
            outcome=SourceEntryResolutionOutcome.EXISTING,
            natural_key=natural_key,
            source_entry=existing,
        )

    candidate_ids = {
        raw.source_entry_id
        for raw in _legacy_raw_entries(session, source_id)
        if _matches_legacy_candidate(raw, normalized, natural_key.identity_kind)
    }
    candidate_ids -= _identities_with_conflicting_natural_key(
        session,
        source_id,
        candidate_ids,
        natural_key,
    )
    if len(candidate_ids) != 1:
        return SourceEntryIdentityResolution(
            outcome=SourceEntryResolutionOutcome.CREATE_NEW,
            natural_key=natural_key,
            source_entry=None,
        )

    source_entry = session.scalar(
        select(SourceEntryIdentity)
        .where(SourceEntryIdentity.id == candidate_ids.pop())
        .with_for_update()
    )
    if source_entry is None:
        return SourceEntryIdentityResolution(
            outcome=SourceEntryResolutionOutcome.CREATE_NEW,
            natural_key=natural_key,
            source_entry=None,
        )

    existing = _identity_for_natural_key(
        session,
        source_id,
        natural_key,
    )
    if existing is not None:
        return SourceEntryIdentityResolution(
            outcome=SourceEntryResolutionOutcome.EXISTING,
            natural_key=natural_key,
            source_entry=existing,
        )

    assigned_kind_keys = set(
        session.scalars(
            select(SourceEntryKey.identity_key).where(
                SourceEntryKey.source_id == source_id,
                SourceEntryKey.source_entry_id == source_entry.id,
                SourceEntryKey.identity_kind == natural_key.identity_kind,
            )
        ).all()
    )
    if assigned_kind_keys:
        return SourceEntryIdentityResolution(
            outcome=SourceEntryResolutionOutcome.CREATE_NEW,
            natural_key=natural_key,
            source_entry=None,
        )

    session.add(
        SourceEntryKey(
            source_entry_id=source_entry.id,
            source_id=source_id,
            identity_kind=natural_key.identity_kind,
            identity_key=natural_key.identity_key,
        )
    )
    session.flush()
    return SourceEntryIdentityResolution(
        outcome=SourceEntryResolutionOutcome.CLAIMED_LEGACY,
        natural_key=natural_key,
        source_entry=source_entry,
    )


def _identity_for_natural_key(
    session: Session,
    source_id: int,
    natural_key: SourceEntryNaturalKey,
) -> SourceEntryIdentity | None:
    return session.scalar(
        select(SourceEntryIdentity)
        .join(
            SourceEntryKey,
            SourceEntryKey.source_entry_id == SourceEntryIdentity.id,
        )
        .where(
            SourceEntryIdentity.source_id == source_id,
            SourceEntryKey.source_id == source_id,
            SourceEntryKey.identity_kind == natural_key.identity_kind,
            SourceEntryKey.identity_key == natural_key.identity_key,
        )
    )


def _normalize_identity_input(
    identity_input: SourceEntryIdentityInput,
) -> _NormalizedIdentityInput:
    fingerprint = normalize_space(identity_input.payload_fingerprint).lower()
    return _NormalizedIdentityInput(
        source_guid=_normalize_text(identity_input.source_guid),
        canonical_url=canonical_url(_normalize_text(identity_input.url)),
        title=_normalize_text(identity_input.title),
        author=_normalize_text(identity_input.author),
        published_at=canonical_utc_timestamp(identity_input.published_at),
        payload_fingerprint=(
            fingerprint if SHA256_HEX_RE.fullmatch(fingerprint) else ""
        ),
    )


def _natural_key_from_normalized(
    identity_input: _NormalizedIdentityInput,
) -> SourceEntryNaturalKey:
    if identity_input.source_guid:
        identity_kind = "guid"
        components = {
            "guid": identity_input.source_guid,
            "url": identity_input.canonical_url or MISSING_CANONICAL_URL,
        }
    elif identity_input.canonical_url:
        identity_kind = "url"
        components = {"url": identity_input.canonical_url}
    else:
        identity_kind = "fallback"
        components = {
            "author": identity_input.author,
            "published_at": (
                identity_input.published_at or MISSING_PUBLISHED_AT
            ),
            "title": identity_input.title,
        }
    canonical_payload = json.dumps(
        components,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
    return SourceEntryNaturalKey(
        identity_kind=identity_kind,
        identity_key=f"{identity_kind}:{digest}",
    )


def _legacy_raw_entries(session: Session, source_id: int) -> list[RawEntry]:
    legacy_identity_ids = select(SourceEntryKey.source_entry_id).where(
        SourceEntryKey.source_id == source_id,
        SourceEntryKey.identity_kind == "legacy",
    )
    return list(
        session.scalars(
            select(RawEntry)
            .where(
                RawEntry.source_id == source_id,
                RawEntry.source_entry_id.in_(legacy_identity_ids),
            )
            .order_by(RawEntry.id)
        ).all()
    )


def _identities_with_conflicting_natural_key(
    session: Session,
    source_id: int,
    candidate_ids: set[int],
    natural_key: SourceEntryNaturalKey,
) -> set[int]:
    if not candidate_ids:
        return set()
    return set(
        session.scalars(
            select(SourceEntryKey.source_entry_id).where(
                SourceEntryKey.source_id == source_id,
                SourceEntryKey.source_entry_id.in_(candidate_ids),
                SourceEntryKey.identity_kind == natural_key.identity_kind,
                SourceEntryKey.identity_key != natural_key.identity_key,
            )
        ).all()
    )


def _matches_legacy_candidate(
    raw: RawEntry,
    identity_input: _NormalizedIdentityInput,
    identity_kind: str,
) -> bool:
    if identity_kind == "guid":
        if not _legacy_external_id_matches_guid(
            raw.external_id,
            identity_input.source_guid,
        ):
            return False
        legacy_url = canonical_url(_normalize_text(raw.url))
        return not (
            identity_input.canonical_url
            and legacy_url
            and identity_input.canonical_url != legacy_url
        )
    if identity_kind == "url":
        return (
            bool(identity_input.canonical_url)
            and canonical_url(_normalize_text(raw.url))
            == identity_input.canonical_url
        )
    if not identity_input.payload_fingerprint:
        return False
    return raw.payload_fingerprint.lower() == identity_input.payload_fingerprint


def _legacy_external_id_matches_guid(external_id: str, source_guid: str) -> bool:
    normalized_external_id = _normalize_text(external_id)
    if normalized_external_id == source_guid:
        return True
    return (
        re.fullmatch(
            rf"{re.escape(source_guid)}:[0-9a-f]{{12}}",
            normalized_external_id,
        )
        is not None
    )


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFC", normalize_space(value))
