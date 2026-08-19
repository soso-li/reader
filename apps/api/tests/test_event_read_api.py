from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from reader_api.db import Base, engine
from reader_api.main import app
from reader_api.models import (
    Cluster,
    ClusterEventProjection,
    ClusterItem,
    ClusteringRun,
    ContentItem,
    Document,
    Event,
    EventEvidence,
    EventEvidenceVersion,
    EventLineage,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    Source,
)
from tests.factories import make_raw_entry


NOW = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)


def seed_event_lifecycle() -> dict[str, object]:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        cluster = Cluster(cluster_key="read-api-cluster", title="Legacy cluster")
        event = Event(
            uid="11111111-1111-4111-8111-111111111111",
            status="active",
            created_at=NOW,
        )
        run = ClusteringRun(
            id="22222222-2222-4222-8222-222222222222",
            scope_type="read-api-test",
            scope_key="a" * 64,
            rule_version="read-api-test-v1",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        session.add_all([cluster, event, run])
        session.flush()
        revision = EventRevision(
            uid="33333333-3333-4333-8333-333333333333",
            event_id=event.id,
            revision_no=1,
            evidence_fingerprint="b" * 64,
            title_snapshot="Frozen event title",
            event_time_snapshot=NOW,
            created_at=NOW,
        )
        session.add(revision)
        session.flush()
        event.current_revision_id = revision.id
        session.add_all(
            [
                ClusterEventProjection(
                    cluster_id=cluster.id,
                    cluster_id_snapshot=cluster.id,
                    clustering_run_id=run.id,
                    cluster_anchor="c" * 64,
                    cluster_occurrence=1,
                    event_id=event.id,
                    event_revision_id=revision.id,
                    reconciliation_kind="initial",
                    after_evidence_fingerprint=revision.evidence_fingerprint,
                    projected_at=NOW,
                ),
                EventUserState(
                    event_id=event.id,
                    seen_revision_id=revision.id,
                    read_status="summary_seen",
                    read_later=True,
                    starred=False,
                    updated_at=NOW,
                ),
            ]
        )
        session.commit()
        return {
            "cluster_id": cluster.id,
            "event_uid": event.uid,
            "revision_uid": revision.uid,
        }


def test_event_lifecycle_survives_cluster_deletion() -> None:
    fixture = seed_event_lifecycle()
    client = TestClient(app)

    response = client.get(f"/events/{fixture['event_uid']}")

    assert response.status_code == 200
    assert response.json() == {
        "event_uid": fixture["event_uid"],
        "status": "active",
        "created_at": "2026-07-15T08:00:00",
        "superseded_at": None,
        "current_revision": {
            "revision_uid": fixture["revision_uid"],
            "revision_no": 1,
            "title": "Frozen event title",
            "event_time": "2026-07-15T08:00:00",
            "evidence_fingerprint": "b" * 64,
            "created_at": "2026-07-15T08:00:00",
        },
        "seen_revision": {
            "revision_uid": fixture["revision_uid"],
            "revision_no": 1,
            "title": "Frozen event title",
            "event_time": "2026-07-15T08:00:00",
            "evidence_fingerprint": "b" * 64,
            "created_at": "2026-07-15T08:00:00",
        },
        "current_revision_differs_from_seen": False,
        "has_material_update": False,
        "material_update_revision_uid": None,
        "user_state": {
            "read_status": "summary_seen",
            "read_later": True,
            "starred": False,
            "uninterested": False,
            "uninterested_reason": None,
            "uninterested_note": None,
            "uninterested_at": None,
            "updated_at": "2026-07-15T08:00:00",
        },
        "current_projection": {
            "cluster_id": fixture["cluster_id"],
            "cluster_id_snapshot": fixture["cluster_id"],
            "clustering_run_id": "22222222-2222-4222-8222-222222222222",
            "revision_uid": fixture["revision_uid"],
            "reconciliation_kind": "initial",
            "rule_version": None,
            "before_evidence_fingerprint": None,
            "after_evidence_fingerprint": "b" * 64,
            "projected_at": "2026-07-15T08:00:00",
        },
        "revisions": [
            {
                "revision_uid": fixture["revision_uid"],
                "revision_no": 1,
                "title": "Frozen event title",
                "event_time": "2026-07-15T08:00:00",
                "evidence_fingerprint": "b" * 64,
                "created_at": "2026-07-15T08:00:00",
            }
        ],
        "lineage": [],
        "successors": [],
        "synthesis": {
            "status": "missing",
                "current_revision_uid": fixture["revision_uid"],
                "source_view_revision_uid": fixture["revision_uid"],
                "covered_revision_uid": None,
                "reviewed_revision_uid": None,
                "new_source_count": 0,
            "unreviewed_evidence_count": 0,
            "unreviewed_source_count": 0,
            "target_revision_uid": fixture["revision_uid"],
            "source_count": 0,
            "can_generate": False,
            "default_view": "source",
            "task_status": "idle",
            "task": None,
            "current": None,
        },
    }
    Session = sessionmaker(bind=engine)
    with Session() as session:
        projection = session.query(ClusterEventProjection).one()
        projection.cluster_id = None
        session.query(Cluster).delete()
        session.commit()

    deleted = client.get(f"/events/{fixture['event_uid']}")
    assert deleted.status_code == 200
    assert deleted.json()["current_projection"]["cluster_id"] is None


def test_superseded_event_does_not_expose_replaced_cluster_projection() -> None:
    fixture = seed_event_lifecycle()
    replacement_uid = "77777777-7777-4777-8777-777777777777"
    Session = sessionmaker(bind=engine)
    with Session() as session:
        old_event = session.query(Event).filter_by(uid=fixture["event_uid"]).one()
        cluster = session.get(Cluster, fixture["cluster_id"])
        replacement = Event(
            uid=replacement_uid,
            status="active",
            created_at=NOW,
        )
        run = ClusteringRun(
            id="88888888-8888-4888-8888-888888888888",
            scope_type="read-api-test",
            scope_key="d" * 64,
            rule_version="read-api-test-v2",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        session.add_all([replacement, run])
        session.flush()
        replacement_revision = EventRevision(
            uid="99999999-9999-4999-8999-999999999999",
            event_id=replacement.id,
            revision_no=1,
            evidence_fingerprint="e" * 64,
            title_snapshot="Replacement event title",
            event_time_snapshot=NOW,
            created_at=NOW,
        )
        session.add(replacement_revision)
        session.flush()
        replacement.current_revision_id = replacement_revision.id
        old_event.status = "superseded"
        old_event.superseded_at = NOW
        session.add(
            ClusterEventProjection(
                cluster_id=cluster.id,
                cluster_id_snapshot=cluster.id,
                clustering_run_id=run.id,
                cluster_anchor="f" * 64,
                cluster_occurrence=1,
                event_id=replacement.id,
                event_revision_id=replacement_revision.id,
                reconciliation_kind="ambiguous",
                reconciliation_rule_version="read-api-test-v2",
                after_evidence_fingerprint=replacement_revision.evidence_fingerprint,
                projected_at=NOW,
            )
        )
        session.commit()

    client = TestClient(app)
    old_response = client.get(f"/events/{fixture['event_uid']}")
    replacement_response = client.get(f"/events/{replacement_uid}")

    assert old_response.status_code == replacement_response.status_code == 200
    assert old_response.json()["current_projection"] is None
    assert replacement_response.json()["current_projection"]["cluster_id"] == fixture[
        "cluster_id"
    ]


def seed_revision_history(
    *,
    raw_matches: bool = True,
    text_matches: bool = False,
    rich_document: bool = False,
    current_revision_has_evidence: bool = False,
    source_status: str = "archived",
) -> dict[str, object]:
    fixture = seed_event_lifecycle()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.query(Event).filter_by(uid=fixture["event_uid"]).one()
        first_revision = session.query(EventRevision).filter_by(
            uid=fixture["revision_uid"]
        ).one()
        first_projection = session.query(ClusterEventProjection).one()
        source = Source(
            name="Archived source",
            url="https://example.com/feed.xml",
            site_url="https://example.com",
            media_type="article",
            status=source_status,
        )
        session.add(source)
        session.flush()
        historical_raw = make_raw_entry(
            source_id=source.id,
            external_id="read-api-entry",
            title="Raw title",
            url="https://example.com/story",
            author="Source author",
            published_at=NOW,
            raw_content="<script>historical raw html must stay private</script>",
        )
        session.add(historical_raw)
        session.flush()
        current_raw = historical_raw
        if not raw_matches:
            current_raw = make_raw_entry(
                source_id=source.id,
                external_id="read-api-entry-current",
                title="Current raw title",
                url="https://example.com/story-current",
                author="Source author",
                published_at=NOW,
                raw_content="<script>current raw html must stay private</script>",
            )
            session.add(current_raw)
            session.flush()
        current_text = "Current document body"
        document = Document(
            raw_entry_id=current_raw.id,
            document_type="normal_article",
            title=current_raw.title,
            content_text=current_text,
            reading_html=(
                "<article><p>Current safe rich body</p></article>"
                if rich_document
                else None
            ),
            body_source="webpage" if rich_document else None,
            web_fetch_status="succeeded" if rich_document else None,
        )
        session.add(document)
        session.flush()
        item = ContentItem(
            document_id=document.id,
            source_id=source.id,
            title=current_raw.title,
            content_text=current_text,
            url=current_raw.url,
            canonical_url=current_raw.url,
            published_at=current_raw.published_at,
            content_hash=current_raw.content_hash,
        )
        evidence = EventEvidence(
            uid="44444444-4444-4444-8444-444444444444",
            identity_fingerprint="d" * 64,
            source_entry_id=historical_raw.source_entry_id,
            fragment_fingerprint="e" * 64,
            created_at=NOW,
        )
        session.add_all([item, evidence])
        session.flush()
        if current_revision_has_evidence:
            session.add(
                ClusterItem(
                    cluster_id=int(fixture["cluster_id"]),
                    content_item_id=item.id,
                )
            )
        first_version = EventEvidenceVersion(
            uid="55555555-5555-4555-8555-555555555555",
            evidence_id=evidence.id,
            version_fingerprint="f" * 64,
            raw_entry_id=historical_raw.id,
            source_entry_id=historical_raw.source_entry_id,
            source_id=source.id,
            raw_revision_no=historical_raw.revision_no,
            legacy_content_item_id=item.id,
            legacy_content_item_id_snapshot=item.id,
            fragment_fingerprint=evidence.fragment_fingerprint,
            title_snapshot="Historical title",
            url_snapshot="https://example.com/story?v=1",
            author_snapshot="Historical author",
            published_at_snapshot=NOW,
            content_snapshot=(
                current_text if text_matches else "Historical evidence body"
            ),
            created_at=NOW,
        )
        session.add(first_version)
        session.flush()
        session.add(
            EventRevisionEvidence(
                revision_id=first_revision.id,
                evidence_version_id=first_version.id,
                evidence_type="article",
                role="primary_source",
                created_at=NOW,
            )
        )

        second_revision = EventRevision(
            uid="66666666-6666-4666-8666-666666666666",
            event_id=event.id,
            revision_no=2,
            evidence_fingerprint="1" * 64,
            title_snapshot="Current event title",
            event_time_snapshot=NOW,
            created_at=NOW,
        )
        second_run = ClusteringRun(
            id="77777777-7777-4777-8777-777777777777",
            scope_type="read-api-test",
            scope_key="2" * 64,
            rule_version="read-api-test-v2",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        session.add_all([second_revision, second_run])
        session.flush()
        event.current_revision_id = second_revision.id
        if current_revision_has_evidence:
            session.add(
                EventRevisionEvidence(
                    revision_id=second_revision.id,
                    evidence_version_id=first_version.id,
                    evidence_type="article",
                    role="primary_source",
                    created_at=NOW,
                )
            )
        session.add(
            ClusterEventProjection(
                cluster_id=first_projection.cluster_id,
                cluster_id_snapshot=first_projection.cluster_id_snapshot,
                clustering_run_id=second_run.id,
                cluster_anchor=first_projection.cluster_anchor,
                cluster_occurrence=1,
                event_id=event.id,
                event_revision_id=second_revision.id,
                predecessor_projection_id=first_projection.id,
                reconciliation_kind="continued",
                reconciliation_rule_version=second_run.rule_version,
                before_evidence_fingerprint=first_revision.evidence_fingerprint,
                after_evidence_fingerprint=second_revision.evidence_fingerprint,
                projected_at=NOW,
            )
        )
        session.commit()
        return {
            **fixture,
            "current_revision_uid": second_revision.uid,
            "source_id": source.id,
            "source_entry_id": historical_raw.source_entry_id,
            "raw_entry_id": historical_raw.id,
            "content_item_id": item.id,
        }


def test_item_detail_returns_current_reading_body_and_legacy_text_fallback() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(
            name="Reading body source",
            url="https://example.com/reading-body.xml",
            media_type="article",
            status="active",
        )
        session.add(source)
        session.flush()

        def add_item(
            external_id: str,
            content_text: str,
            *,
            reading_html: str | None = None,
            body_source: str | None = None,
            web_fetch_status: str | None = None,
        ) -> int:
            raw = make_raw_entry(
                source_id=source.id,
                external_id=external_id,
                title=external_id,
                raw_content="<script>raw webpage html must stay private</script>",
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                document_type="normal_article",
                title=raw.title,
                content_text=content_text,
                reading_html=reading_html,
                body_source=body_source,
                web_fetch_status=web_fetch_status,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=raw.title,
                content_text=content_text,
                url=raw.url,
                canonical_url=raw.url,
                published_at=raw.published_at,
                content_hash=raw.content_hash,
            )
            session.add(item)
            session.flush()
            return item.id

        current_item_id = add_item(
            "current-reading-body",
            "Current safe text",
            reading_html="<article><p>Current safe body</p></article>",
            body_source="webpage",
            web_fetch_status="succeeded",
        )
        legacy_item_id = add_item("legacy-reading-body", "Legacy text fallback")
        session.commit()

    client = TestClient(app)
    current = client.get(f"/items/{current_item_id}")
    legacy = client.get(f"/items/{legacy_item_id}")

    assert current.status_code == legacy.status_code == 200
    assert {
        key: current.json()[key]
        for key in (
            "content_text",
            "reading_html",
            "body_source",
            "web_fetch_status",
        )
    } == {
        "content_text": "Current safe text",
        "reading_html": "<article><p>Current safe body</p></article>",
        "body_source": "webpage",
        "web_fetch_status": "succeeded",
    }
    assert "raw webpage html must stay private" not in current.text
    assert {
        key: legacy.json()[key]
        for key in (
            "content_text",
            "reading_html",
            "body_source",
            "web_fetch_status",
        )
    } == {
        "content_text": "Legacy text fallback",
        "reading_html": None,
        "body_source": None,
        "web_fetch_status": None,
    }


def test_historical_revision_returns_its_frozen_evidence_and_source_location() -> None:
    fixture = seed_revision_history()
    client = TestClient(app)

    event = client.get(f"/events/{fixture['event_uid']}").json()
    response = client.get(
        f"/events/{fixture['event_uid']}/revisions/{fixture['revision_uid']}"
    )

    assert event["current_revision"]["revision_uid"] == fixture["current_revision_uid"]
    assert event["seen_revision"]["revision_uid"] == fixture["revision_uid"]
    assert event["current_revision_differs_from_seen"] is True
    assert response.status_code == 200
    assert response.json() == {
        "revision_uid": fixture["revision_uid"],
        "revision_no": 1,
        "title": "Frozen event title",
        "event_time": "2026-07-15T08:00:00",
        "evidence_fingerprint": "b" * 64,
        "created_at": "2026-07-15T08:00:00",
        "evidence": [
            {
                "evidence_uid": "44444444-4444-4444-8444-444444444444",
                "identity_fingerprint": "d" * 64,
                "version_uid": "55555555-5555-4555-8555-555555555555",
                "version_fingerprint": "f" * 64,
                "evidence_type": "article",
                "role": "primary_source",
                "source": {
                    "source_id": fixture["source_id"],
                    "name": "Archived source",
                    "feed_url": "https://example.com/feed.xml",
                    "site_url": "https://example.com",
                    "media_type": "article",
                },
                "source_entry_id": fixture["source_entry_id"],
                "raw_entry_id": fixture["raw_entry_id"],
                "raw_revision_no": 1,
                "legacy_content_item_id": fixture["content_item_id"],
                "legacy_content_item_id_snapshot": fixture["content_item_id"],
                "fragment_fingerprint": "e" * 64,
                "title": "Historical title",
                "url": "https://example.com/story?v=1",
                "author": "Historical author",
                "published_at": "2026-07-15T08:00:00",
                "content": "Historical evidence body",
                "reading_html": None,
                "body_source": None,
                "web_fetch_status": None,
            }
        ],
    }


@pytest.mark.parametrize(
    ("raw_matches", "text_matches", "expected_reading_html"),
    [
        (False, True, None),
        (True, False, None),
        (True, True, "<article><p>Current safe rich body</p></article>"),
    ],
)
def test_historical_revision_returns_rich_body_only_for_exact_current_evidence(
    raw_matches: bool,
    text_matches: bool,
    expected_reading_html: str | None,
) -> None:
    fixture = seed_revision_history(
        raw_matches=raw_matches,
        text_matches=text_matches,
        rich_document=True,
        current_revision_has_evidence=True,
        source_status="active",
    )

    client = TestClient(app)
    response = client.get(
        f"/events/{fixture['event_uid']}/revisions/{fixture['revision_uid']}"
    )
    cluster = client.get(f"/clusters/{fixture['cluster_id']}")

    assert response.status_code == cluster.status_code == 200
    evidence = response.json()["evidence"][0]
    expected = {
        "reading_html": expected_reading_html,
        "body_source": "webpage" if expected_reading_html else None,
        "web_fetch_status": "succeeded" if expected_reading_html else None,
    }
    assert {
        key: evidence[key]
        for key in ("reading_html", "body_source", "web_fetch_status")
    } == expected
    assert {
        key: cluster.json()["items"][0][key]
        for key in ("reading_html", "body_source", "web_fetch_status")
    } == expected
    assert "raw html must stay private" not in response.text
    assert "raw html must stay private" not in cluster.text


def seed_lineage_history() -> dict[str, object]:
    fixture = seed_revision_history()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        root = session.query(Event).filter_by(uid=fixture["event_uid"]).one()

        def add_event(uid: str, revision_uid: str, title: str) -> Event:
            event = Event(uid=uid, status="active", created_at=NOW)
            session.add(event)
            session.flush()
            revision = EventRevision(
                uid=revision_uid,
                event_id=event.id,
                revision_no=1,
                evidence_fingerprint=uid.replace("-", "")[:32] * 2,
                title_snapshot=title,
                event_time_snapshot=NOW,
                created_at=NOW,
            )
            session.add(revision)
            session.flush()
            event.current_revision_id = revision.id
            return event

        split_a = add_event(
            "88888888-8888-4888-8888-888888888888",
            "99999999-9999-4999-8999-999999999999",
            "Split child A",
        )
        split_b = add_event(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "Split child B",
        )
        merged = add_event(
            "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            "Merged successor",
        )
        other_parent = add_event(
            "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            "ffffffff-ffff-4fff-8fff-ffffffffffff",
            "Other ambiguous parent",
        )
        ambiguous_a = add_event(
            "12121212-1212-4212-8212-121212121212",
            "13131313-1313-4313-8313-131313131313",
            "Ambiguous successor A",
        )
        ambiguous_b = add_event(
            "14141414-1414-4414-8414-141414141414",
            "15151515-1515-4515-8515-151515151515",
            "Ambiguous successor B",
        )

        split_run = ClusteringRun(
            id="16161616-1616-4616-8616-161616161616",
            scope_type="read-api-lineage",
            scope_key="3" * 64,
            rule_version="split-v1",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        merge_run = ClusteringRun(
            id="17171717-1717-4717-8717-171717171717",
            scope_type="read-api-lineage",
            scope_key="4" * 64,
            rule_version="merge-v1",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        ambiguous_run = ClusteringRun(
            id="18181818-1818-4818-8818-181818181818",
            scope_type="read-api-lineage",
            scope_key="5" * 64,
            rule_version="ambiguous-v1",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        merge_parent_cluster = Cluster(
            cluster_key="read-api-merge-parent",
            title="Merge parent historical cluster",
        )
        merge_parent_run = ClusteringRun(
            id="28282828-2828-4828-8828-282828282828",
            scope_type="read-api-lineage",
            scope_key="8" * 64,
            rule_version="initial-v1",
            status="completed",
            started_at=NOW,
            completed_at=NOW,
            after_snapshot_finalized=True,
        )
        session.add_all(
            [
                split_run,
                merge_run,
                ambiguous_run,
                merge_parent_cluster,
                merge_parent_run,
            ]
        )
        session.flush()
        session.add(
            ClusterEventProjection(
                cluster_id=merge_parent_cluster.id,
                cluster_id_snapshot=merge_parent_cluster.id,
                clustering_run_id=merge_parent_run.id,
                cluster_anchor="9" * 64,
                cluster_occurrence=1,
                event_id=split_b.id,
                event_revision_id=split_b.current_revision_id,
                reconciliation_kind="initial",
                after_evidence_fingerprint="a" * 64,
                projected_at=NOW,
            )
        )

        def add_lineage(
            uid: str,
            run: ClusteringRun,
            relation_type: str,
            source: Event,
            target: Event,
        ) -> None:
            session.add(
                EventLineage(
                    uid=uid,
                    clustering_run_id=run.id,
                    relation_type=relation_type,
                    source_event_id=source.id,
                    target_event_id=target.id,
                    rule_version=run.rule_version,
                    before_evidence_fingerprint="6" * 64,
                    after_evidence_fingerprint="7" * 64,
                    decision_reason=f"{relation_type} test decision",
                    created_at=NOW,
                )
            )

        add_lineage(
            "19191919-1919-4919-8919-191919191919",
            split_run,
            "split_from",
            root,
            split_a,
        )
        add_lineage(
            "20202020-2020-4020-8020-202020202020",
            split_run,
            "split_from",
            root,
            split_b,
        )
        add_lineage(
            "21212121-2121-4121-8121-212121212121",
            merge_run,
            "merged_from",
            split_a,
            merged,
        )
        add_lineage(
            "23232323-2323-4323-8323-232323232323",
            merge_run,
            "merged_from",
            split_b,
            merged,
        )
        for source, first_uid, second_uid in (
            (
                merged,
                "24242424-2424-4424-8424-242424242424",
                "25252525-2525-4525-8525-252525252525",
            ),
            (
                other_parent,
                "26262626-2626-4626-8626-262626262626",
                "27272727-2727-4727-8727-272727272727",
            ),
        ):
            add_lineage(
                first_uid,
                ambiguous_run,
                "ambiguous_from",
                source,
                ambiguous_a,
            )
            add_lineage(
                second_uid,
                ambiguous_run,
                "ambiguous_from",
                source,
                ambiguous_b,
            )

        for superseded in (root, split_a, split_b, merged, other_parent):
            superseded.status = "superseded"
            superseded.superseded_at = NOW
        session.commit()
        return {
            **fixture,
            "split_a": split_a.uid,
            "split_b": split_b.uid,
            "merged": merged.uid,
            "merge_parent_cluster_id": merge_parent_cluster.id,
            "ambiguous_a": ambiguous_a.uid,
            "ambiguous_b": ambiguous_b.uid,
        }


def test_merge_parent_without_surviving_cluster_has_no_current_projection() -> None:
    fixture = seed_lineage_history()
    client = TestClient(app)

    parent = client.get(f"/events/{fixture['split_b']}")

    assert parent.status_code == 200
    assert parent.json()["status"] == "superseded"
    assert parent.json()["current_projection"] is None


def test_event_read_api_returns_continued_and_explicit_lineage_successors() -> None:
    fixture = seed_lineage_history()
    client = TestClient(app)

    root = client.get(f"/events/{fixture['event_uid']}").json()
    split_child = client.get(f"/events/{fixture['split_a']}").json()
    merged = client.get(f"/events/{fixture['merged']}").json()

    continued = next(row for row in root["lineage"] if row["relation_type"] == "continued")
    assert continued == {
        "lineage_uid": None,
        "relation_type": "continued",
        "direction": "self",
        "source_event_uid": fixture["event_uid"],
        "target_event_uid": fixture["event_uid"],
        "source_revision_uid": fixture["revision_uid"],
        "target_revision_uid": fixture["current_revision_uid"],
        "clustering_run_id": "77777777-7777-4777-8777-777777777777",
        "rule_version": "read-api-test-v2",
        "before_evidence_fingerprint": "b" * 64,
        "after_evidence_fingerprint": "1" * 64,
        "decision_reason": None,
        "recorded_at": "2026-07-15T08:00:00",
    }
    assert {
        (row["direction"], row["relation_type"], row["target_event_uid"])
        for row in root["lineage"]
        if row["relation_type"] != "continued"
    } == {
        ("outgoing", "split_from", fixture["split_a"]),
        ("outgoing", "split_from", fixture["split_b"]),
    }
    assert {
        (row["relation_type"], row["event_uid"], row["title"])
        for row in root["successors"]
    } == {
        ("split_from", fixture["split_a"], "Split child A"),
        ("split_from", fixture["split_b"], "Split child B"),
    }
    assert {
        (row["direction"], row["relation_type"], row["source_event_uid"], row["target_event_uid"])
        for row in split_child["lineage"]
    } == {
        ("incoming", "split_from", fixture["event_uid"], fixture["split_a"]),
        ("outgoing", "merged_from", fixture["split_a"], fixture["merged"]),
    }
    assert {
        (row["direction"], row["relation_type"], row["source_event_uid"], row["target_event_uid"])
        for row in merged["lineage"]
    } == {
        ("incoming", "merged_from", fixture["split_a"], fixture["merged"]),
        ("incoming", "merged_from", fixture["split_b"], fixture["merged"]),
        ("outgoing", "ambiguous_from", fixture["merged"], fixture["ambiguous_a"]),
        ("outgoing", "ambiguous_from", fixture["merged"], fixture["ambiguous_b"]),
    }
    assert {row["event_uid"] for row in merged["successors"]} == {
        fixture["ambiguous_a"],
        fixture["ambiguous_b"],
    }

    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.query(ClusterEventProjection).update({"cluster_id": None})
        session.query(Cluster).delete()
        session.commit()

    root_after_cluster_delete = client.get(
        f"/events/{fixture['event_uid']}"
    ).json()
    historical_after_cluster_delete = client.get(
        f"/events/{fixture['event_uid']}/revisions/{fixture['revision_uid']}"
    ).json()
    assert root_after_cluster_delete["lineage"] == root["lineage"]
    assert historical_after_cluster_delete["evidence"][0]["title"] == (
        "Historical title"
    )


def test_interaction_audit_reads_event_or_object_target_without_inventing_history() -> None:
    fixture = seed_revision_history()
    client = TestClient(app)
    event_write = client.post(
        "/event-user-state",
        json={
            "event_uid": fixture["event_uid"],
            "observed_revision_uid": fixture["revision_uid"],
            "operation_id": "issue31-event-audit",
            "action": "starred_set",
            "value": True,
        },
    )
    object_write = client.patch(
        f"/user-state/item/{fixture['content_item_id']}",
        json={
            "operation_id": "issue31-object-audit",
            "starred": True,
        },
    )
    assert event_write.status_code == object_write.status_code == 200

    event_audit = client.get(
        "/interactions", params={"event_uid": fixture["event_uid"]}
    )
    object_audit = client.get(
        "/interactions",
        params={
            "object_type": "item",
            "object_id": fixture["content_item_id"],
        },
    )

    assert event_audit.status_code == object_audit.status_code == 200
    assert [
        {
            key: row[key]
            for key in (
                "operation_id",
                "target_kind",
                "event_uid",
                "observed_revision_uid",
                "object_type",
                "object_id",
                "action",
                "set_value",
            )
        }
        for row in event_audit.json()
    ] == [
        {
            "operation_id": "issue31-event-audit",
            "target_kind": "event",
            "event_uid": fixture["event_uid"],
            "observed_revision_uid": fixture["revision_uid"],
            "object_type": None,
            "object_id": None,
            "action": "starred_set",
            "set_value": True,
        }
    ]
    assert [
        {
            key: row[key]
            for key in (
                "operation_id",
                "target_kind",
                "event_uid",
                "observed_revision_uid",
                "object_type",
                "object_id",
                "action",
                "set_value",
            )
        }
        for row in object_audit.json()
    ] == [
        {
            "operation_id": "issue31-object-audit",
            "target_kind": "object",
            "event_uid": None,
            "observed_revision_uid": None,
            "object_type": "item",
            "object_id": fixture["content_item_id"],
            "action": "starred_set",
            "set_value": True,
        }
    ]
    assert event_audit.json()[0]["occurred_at"]
    assert event_audit.json()[0]["recorded_at"]
    state_before_reads = client.get(f"/events/{fixture['event_uid']}").json()[
        "user_state"
    ]
    assert client.get(
        "/interactions", params={"event_uid": fixture["event_uid"]}
    ).json() == event_audit.json()
    assert client.get(f"/events/{fixture['event_uid']}").json()[
        "user_state"
    ] == state_before_reads
    assert client.get("/interactions").status_code == 400
    assert (
        client.get(
            "/interactions",
            params={
                "event_uid": fixture["event_uid"],
                "object_type": "item",
                "object_id": fixture["content_item_id"],
            },
        ).status_code
        == 400
    )
