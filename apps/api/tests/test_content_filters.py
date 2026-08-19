from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select
from sqlalchemy.orm import sessionmaker

from reader_api.content_filters import refresh_filter_matches_for_items
from reader_api.db import Base, engine
from reader_api.digest import content_hash, normalize_title
from reader_api.generation_producers import (
    GenerationProducerValidationError,
    prepare_cluster_synthesis,
)
from reader_api.main import app
from reader_api.models import (
    ClusterItem,
    ClusteringRun,
    ContentItem,
    Document,
    FilterMatch,
    Source,
    UserState,
)
from reader_api.report_generation import report_clusters
from tests.factories import assign_publishable_cluster, make_raw_entry


def _seed_items() -> tuple[int, list[int]]:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        source = Source(name="Example Feed", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        item_ids: list[int] = []
        now = datetime(2026, 7, 20, 8, tzinfo=timezone.utc)
        rows = [
            ("Ordinary update", "A useful release note"),
            ("Sponsored launch", "Partner announcement"),
            ("Weekly note", "This issue is sponsored by Example"),
        ]
        for index, (title, body) in enumerate(rows):
            raw = make_raw_entry(
                source_id=source.id,
                external_id=str(index),
                title=title,
                content_hash=content_hash(f"raw-{index}"),
            )
            session.add(raw)
            session.flush()
            document = Document(
                raw_entry_id=raw.id,
                title=title,
                summary=body,
                content_text=body,
            )
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=body,
                content_text=body,
                content_hash=content_hash(f"item-{index}"),
                normalized_title=normalize_title(title),
                published_at=now + timedelta(minutes=index),
            )
            session.add(item)
            session.flush()
            assign_publishable_cluster(session, item)
            item_ids.append(item.id)
        session.add(
            UserState(
                object_type="item",
                object_id=item_ids[1],
                read_status="summary_seen",
                read_later=True,
                starred=True,
            )
        )
        source_id = source.id
        session.commit()
    return source_id, item_ids


def test_filter_rule_preview_apply_pause_edit_and_delete_preserve_user_state() -> None:
    source_id, item_ids = _seed_items()
    client = TestClient(app)

    before_state = client.get(f"/items/{item_ids[1]}").json()
    preview = client.post(
        "/filter-rules/preview",
        json={"source_id": None, "match_type": "literal", "pattern": "SPONSORED"},
    )
    assert preview.status_code == 200
    assert preview.json()["count"] == 2
    assert [row["title"] for row in preview.json()["items"]] == [
        "Weekly note",
        "Sponsored launch",
    ]

    created = client.post(
        "/filter-rules",
        json={"source_id": None, "match_type": "literal", "pattern": "sponsored"},
    )
    assert created.status_code == 200
    rule = created.json()
    assert rule["enabled"] is True
    assert rule["match_count"] == 2

    assert [row["title"] for row in client.get("/items").json()] == [
        "Ordinary update"
    ]
    searched = client.get("/items", params={"q": "sponsored"}).json()
    assert {row["title"] for row in searched} == {"Sponsored launch", "Weekly note"}
    assert all(row["filtered"] for row in searched)
    assert all(row["filter_rules"] == ["关键词：sponsored"] for row in searched)
    filtered = client.get("/filtered-items").json()
    assert filtered["count"] == 2
    assert len(filtered["items"]) == 2
    source_summary = next(row for row in client.get("/sources").json() if row["id"] == source_id)
    assert source_summary["unread_count"] == 3
    assert source_summary["all_unread_count"] == 1

    after_state = client.get(f"/items/{item_ids[1]}").json()
    assert {
        key: after_state[key]
        for key in ("read_status", "read_later", "starred")
    } == {
        key: before_state[key]
        for key in ("read_status", "read_later", "starred")
    }

    paused = client.patch(f"/filter-rules/{rule['id']}", json={"enabled": False})
    assert paused.status_code == 200
    assert client.get("/filtered-items").json()["count"] == 0
    assert len(client.get("/items").json()) == 3

    edited = client.patch(
        f"/filter-rules/{rule['id']}",
        json={
            "source_id": source_id,
            "match_type": "regex",
            "pattern": "Ordinary\\s+update",
            "enabled": True,
        },
    )
    assert edited.status_code == 200
    assert edited.json()["match_count"] == 1
    assert client.get("/filtered-items").json()["items"][0]["title"] == "Ordinary update"

    deleted = client.delete(f"/filter-rules/{rule['id']}")
    assert deleted.status_code == 204
    assert client.get("/filter-rules").json() == []
    assert client.get("/filtered-items").json()["count"] == 0
    assert len(client.get("/items").json()) == 3


def test_filter_rule_validation_and_content_refresh() -> None:
    _source_id, item_ids = _seed_items()
    client = TestClient(app)

    invalid = client.post(
        "/filter-rules/preview",
        json={"match_type": "regex", "pattern": "("},
    )
    assert invalid.status_code == 400
    assert "正则表达式无效" in invalid.json()["detail"]

    created = client.post(
        "/filter-rules",
        json={"match_type": "literal", "pattern": "new blocked phrase"},
    ).json()
    assert created["match_count"] == 0

    Session = sessionmaker(bind=engine)
    with Session() as session:
        item = session.get(ContentItem, item_ids[0])
        assert item is not None
        item.content_text = "The body now contains a new blocked phrase."
        refresh_filter_matches_for_items(session, [item.id])
        session.commit()
        assert session.scalar(select(func.count()).select_from(FilterMatch)) == 1

    filtered = client.get("/items", params={"filtered_only": True}).json()
    assert [row["id"] for row in filtered] == [item_ids[0]]


def test_filtered_stream_loads_page_in_three_database_reads() -> None:
    _seed_items()
    client = TestClient(app)
    client.post(
        "/filter-rules",
        json={"match_type": "literal", "pattern": "sponsored"},
    )
    selects: list[str] = []

    def capture_selects(_connection, _cursor, statement, _parameters, _context, _executemany) -> None:
        if statement.lstrip().upper().startswith(("SELECT", "WITH")):
            selects.append(statement)

    event.listen(engine, "before_cursor_execute", capture_selects)
    try:
        response = client.get("/filtered-items")
    finally:
        event.remove(engine, "before_cursor_execute", capture_selects)

    assert response.status_code == 200
    assert response.json()["count"] == 2
    assert len(selects) <= 3


def test_filtered_items_do_not_reenter_report_or_legacy_synthesis_inputs() -> None:
    _source_id, item_ids = _seed_items()
    client = TestClient(app)
    created = client.post(
        "/filter-rules",
        json={"match_type": "literal", "pattern": "sponsored"},
    )
    assert created.status_code == 200

    Session = sessionmaker(bind=engine)
    with Session() as session:
        cluster_ids = dict(
            session.execute(
                select(ClusterItem.content_item_id, ClusterItem.cluster_id).where(
                    ClusterItem.content_item_id.in_(item_ids)
                )
            ).all()
        )
        candidates = report_clusters(
            session,
            datetime(2026, 7, 19, tzinfo=timezone.utc),
            datetime(2026, 7, 21, tzinfo=timezone.utc),
        )
        assert [cluster.id for cluster in candidates] == [cluster_ids[item_ids[0]]]
        with pytest.raises(
            GenerationProducerValidationError,
            match="没有可合成的来源",
        ):
            prepare_cluster_synthesis(session, cluster_ids[item_ids[1]])


def test_pausing_source_preserves_existing_stream_and_derived_inputs() -> None:
    source_id, item_ids = _seed_items()
    client = TestClient(app)
    clusters_before = client.get("/clusters").json()
    items_before = client.get("/items").json()
    source_before = next(
        row for row in client.get("/sources").json() if row["id"] == source_id
    )
    Session = sessionmaker(bind=engine)
    with Session() as session:
        memberships_before = session.execute(
            select(ClusterItem.cluster_id, ClusterItem.content_item_id).order_by(
                ClusterItem.cluster_id, ClusterItem.content_item_id
            )
        ).all()
        embeddings_before = session.execute(
            select(
                ContentItem.id,
                ContentItem.embedding_vector,
                ContentItem.embedding_model,
            ).order_by(ContentItem.id)
        ).all()
        runs_before = list(session.scalars(select(ClusteringRun.id)))

    paused = client.patch(f"/sources/{source_id}", json={"enabled": False})
    assert paused.status_code == 200
    assert paused.json()["enabled"] is False
    clusters_after = client.get("/clusters").json()
    assert [
        (row["id"], row["item_count"], [item["id"] for item in row["items"]])
        for row in clusters_after
    ] == [
        (row["id"], row["item_count"], [item["id"] for item in row["items"]])
        for row in clusters_before
    ]
    assert [row["id"] for row in client.get("/items").json()] == [
        row["id"] for row in items_before
    ]
    source_after = next(
        row for row in client.get("/sources").json() if row["id"] == source_id
    )
    assert (source_after["unread_count"], source_after["all_unread_count"]) == (
        source_before["unread_count"],
        source_before["all_unread_count"],
    )

    with Session() as session:
        assert session.execute(
            select(ClusterItem.cluster_id, ClusterItem.content_item_id).order_by(
                ClusterItem.cluster_id, ClusterItem.content_item_id
            )
        ).all() == memberships_before
        assert session.execute(
            select(
                ContentItem.id,
                ContentItem.embedding_vector,
                ContentItem.embedding_model,
            ).order_by(ContentItem.id)
        ).all() == embeddings_before
        assert list(session.scalars(select(ClusteringRun.id))) == runs_before
        assert {cluster.id for cluster in report_clusters(
            session,
            datetime(2026, 7, 19, tzinfo=timezone.utc),
            datetime(2026, 7, 21, tzinfo=timezone.utc),
        )} == {row["id"] for row in clusters_before}
        prepared = prepare_cluster_synthesis(session, clusters_before[0]["id"])
        assert prepared.source_ids == [source_id]

    restored = client.patch(f"/sources/{source_id}", json={"enabled": True})
    assert restored.status_code == 200
    assert restored.json()["enabled"] is True
    assert [row["id"] for row in client.get("/clusters").json()] == [
        row["id"] for row in clusters_before
    ]


def test_multi_source_event_stays_visible_until_every_evidence_item_is_filtered() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        sources = [
            Source(name="First Feed", url="https://example.com/first.xml"),
            Source(name="Second Feed", url="https://example.com/second.xml"),
        ]
        session.add_all(sources)
        session.flush()
        items: list[ContentItem] = []
        for index, (source, title) in enumerate(
            zip(sources, ("Sponsored evidence", "Useful evidence"), strict=True)
        ):
            raw = make_raw_entry(
                source_id=source.id,
                external_id=str(index),
                title=title,
                content_hash=content_hash(f"raw-{title}"),
            )
            session.add(raw)
            session.flush()
            document = Document(raw_entry_id=raw.id, title=title, content_text=title)
            session.add(document)
            session.flush()
            item = ContentItem(
                document_id=document.id,
                source_id=source.id,
                title=title,
                summary=title,
                content_text=title,
                content_hash=content_hash(title),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            items.append(item)
        cluster = assign_publishable_cluster(session, items[0])
        session.add(ClusterItem(cluster_id=cluster.id, content_item_id=items[1].id))
        session.commit()

    client = TestClient(app)
    first = client.post(
        "/filter-rules",
        json={"match_type": "literal", "pattern": "Sponsored"},
    )
    assert first.status_code == 200
    visible = client.get("/clusters").json()
    assert len(visible) == 1
    assert visible[0]["item_count"] == 1
    assert [item["title"] for item in visible[0]["items"]] == ["Useful evidence"]

    second = client.post(
        "/filter-rules",
        json={"match_type": "literal", "pattern": "Useful"},
    )
    assert second.status_code == 200
    assert client.get("/clusters").json() == []

    diagnostic = client.get("/clusters", params={"q": "evidence"}).json()
    assert len(diagnostic) == 1
    assert diagnostic[0]["item_count"] == 2
    assert {item["title"] for item in diagnostic[0]["items"]} == {
        "Sponsored evidence",
        "Useful evidence",
    }
    assert next(
        item for item in diagnostic[0]["items"]
        if item["title"] == "Sponsored evidence"
    )["filtered"] is True
