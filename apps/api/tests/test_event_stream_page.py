"""分页形状等价回归：新流序分页查询与旧平铺 GROUP BY 形状逐行等价。

旧形状装配（cluster_stream_query + 游标子句 + group_by/order/limit/offset）
在本文件内冻结为语义预言机（快照自 e64634c 的 list_clusters），新形状
cluster_stream_page_query 必须在全部筛选组合、排序方向与游标翻页下给出
完全相同的 (cluster_id, item_count) 序列。
"""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import and_, func, or_, select
from sqlalchemy import update as sa_update
from sqlalchemy.orm import Session, sessionmaker

from reader_api.clustering_run import clustering_run
from reader_api.db import Base, engine
from reader_api.digest import content_hash, normalize_title
from reader_api.event_stream import cluster_stream_page_query, cluster_stream_query
from reader_api.main import STREAM_SEARCH, app
from reader_api.models import (
    Cluster,
    ClusterItem,
    ContentItem,
    Document,
    Event,
    EventRevision,
    EventUserState,
    EvidenceReview,
    EvidenceSnapshot,
    FilterMatch,
    FilterRule,
    Folder,
    Source,
)
from tests.factories import assign_publishable_cluster, make_raw_entry
from tests.test_api import set_cluster_event_state


BASE_TIME = datetime(2026, 8, 1, tzinfo=timezone.utc)


def _seed_matrix() -> dict[str, int]:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        folder = Folder(name="科技")
        other_folder = Folder(name="综合")
        session.add_all([folder, other_folder])
        session.flush()
        source_a = Source(
            name="Source A", url="https://a.example/feed.xml", folder_id=folder.id
        )
        source_b = Source(
            name="Source B", url="https://b.example/feed.xml", folder_id=other_folder.id
        )
        session.add_all([source_a, source_b])
        session.flush()

        def add_item(source: Source, title: str) -> ContentItem:
            raw = make_raw_entry(
                source_id=source.id,
                external_id=title,
                title=title,
                raw_content=title,
                content_hash=content_hash(title),
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
                content_hash=content_hash(f"item-{title}"),
                normalized_title=normalize_title(title),
            )
            session.add(item)
            session.flush()
            return item

        items = {
            "unread": add_item(source_a, "Plain unread"),
            "seen": add_item(source_a, "Seen update"),
            "dismissed": add_item(source_b, "Dismissed update"),
            "uninterested": add_item(source_a, "Uninterested update"),
            "starred": add_item(source_b, "Starred update"),
            "filtered_only": add_item(source_a, "Sponsored only"),
            "mixed_plain": add_item(source_a, "Mixed plain"),
            "mixed_filtered": add_item(source_a, "Sponsored sibling"),
            "multi_1": add_item(source_b, "Multi one"),
            "multi_2": add_item(source_b, "Multi two"),
            "multi_3": add_item(source_b, "Multi three"),
            "material_seen": add_item(source_b, "Material seen update"),
            "null_seen": add_item(source_a, "Null first seen"),
            "tie_a": add_item(source_a, "Tie first"),
            "tie_b": add_item(source_b, "Tie second"),
        }
        with clustering_run(
            session,
            scope_type="stream-page-shape-test",
            item_ids=[item.id for item in items.values()],
            rule_version="stream-page-shape-test-v1",
        ):
            clusters = {
                key: assign_publishable_cluster(session, item)
                for key, item in items.items()
                if key not in {"mixed_filtered", "multi_2", "multi_3"}
            }
            for extra, host in (
                ("mixed_filtered", "mixed_plain"),
                ("multi_2", "multi_1"),
                ("multi_3", "multi_1"),
            ):
                session.add(
                    ClusterItem(
                        cluster_id=clusters[host].id,
                        content_item_id=items[extra].id,
                    )
                )
            session.flush()

        rule = FilterRule(
            source_id=None, match_type="literal", pattern="Sponsored", enabled=True
        )
        session.add(rule)
        session.flush()
        session.add_all(
            [
                FilterMatch(rule_id=rule.id, content_item_id=items["filtered_only"].id),
                FilterMatch(
                    rule_id=rule.id, content_item_id=items["mixed_filtered"].id
                ),
            ]
        )

        # 时间轴：错开 first_seen_at，含同刻并列与 NULL。
        for index, key in enumerate(
            [
                "unread",
                "seen",
                "dismissed",
                "uninterested",
                "starred",
                "filtered_only",
                "mixed_plain",
                "multi_1",
                "material_seen",
            ]
        ):
            clusters[key].first_seen_at = BASE_TIME + timedelta(hours=index)
        tie_time = BASE_TIME + timedelta(days=2)
        clusters["tie_a"].first_seen_at = tie_time
        clusters["tie_b"].first_seen_at = tie_time
        clusters["null_seen"].first_seen_at = None
        session.commit()
        ids = {key: cluster.id for key, cluster in clusters.items()}
        ids["folder"] = folder.id
        ids["source_a"] = source_a.id
        ids["source_b"] = source_b.id
        return ids


def _apply_states(ids: dict[str, int]) -> None:
    client = TestClient(app)
    rows = {
        row["id"]: row
        for row in client.get("/clusters", params={"limit": 200}).json()
    }
    operations = [
        ("seen", "read_status_set", "summary_seen"),
        ("dismissed", "read_status_set", "summary_seen"),
        ("starred", "read_status_set", "summary_seen"),
        ("starred", "starred_set", True),
    ]
    for index, (key, action, value) in enumerate(operations):
        cluster = rows[ids[key]]
        set_cluster_event_state(
            client,
            cluster,
            operation_id=f"70a90000-0000-4000-8000-00000000000{index}",
            action=action,
            value=value,
        )
    uninterested = rows[ids["uninterested"]]
    assert (
        client.post(
            "/uninterested",
            json={
                "operation_id": "70a90000-0000-4000-8000-0000000000ff",
                "target_type": "event",
                "event_uid": uninterested["event_uid"],
                "observed_revision_uid": uninterested["current_revision_uid"],
                "value": True,
            },
        ).status_code
        == 200
    )
    # material_seen：已读于 r1，但 material 评审目标指向更新的 r2 —— ADR-0020
    # 有效状态回到未读。直接落库构造最小证据链（r2 + 双 snapshot + review）。
    material_row = rows[ids["material_seen"]]
    set_cluster_event_state(
        client,
        material_row,
        operation_id="70a90000-0000-4000-8000-0000000000fe",
        action="read_status_set",
        value="summary_seen",
    )
    Session = sessionmaker(bind=engine)
    with Session() as session:
        event = session.execute(
            select(Event).where(Event.uid == material_row["event_uid"])
        ).scalar_one()
        seen_revision = session.execute(
            select(EventRevision).where(
                EventRevision.uid == material_row["current_revision_uid"]
            )
        ).scalar_one()
        newer = EventRevision(
            uid=str(uuid4()),
            event_id=event.id,
            revision_no=seen_revision.revision_no + 1,
            evidence_fingerprint="f" * 64,
            title_snapshot="material update",
            event_time_snapshot=seen_revision.event_time_snapshot,
        )
        session.add(newer)
        session.flush()
        snapshots = {}
        for revision in (seen_revision, newer):
            snapshot = EvidenceSnapshot(
                uid=str(uuid4()),
                event_id=event.id,
                target_revision_id=revision.id,
                source_coverage_fingerprint="a" * 64,
                content_fingerprint="b" * 64,
                policy_version="test-policy",
            )
            session.add(snapshot)
            session.flush()
            snapshots[revision.id] = snapshot.id
        session.add(
            EvidenceReview(
                uid=str(uuid4()),
                event_id=event.id,
                baseline_revision_id=seen_revision.id,
                baseline_snapshot_id=snapshots[seen_revision.id],
                target_revision_id=newer.id,
                target_snapshot_id=snapshots[newer.id],
                comparison_fingerprint="c" * 64,
                result="material",
                reason="stream page shape material fixture",
                provider="test",
                model="test",
                policy_version="test-policy",
            )
        )
        session.commit()
    with Session() as session:
        session.execute(
            sa_update(EventUserState)
            .where(
                EventUserState.event_id.in_(
                    select(Event.id).where(
                        Event.uid == rows[ids["dismissed"]]["event_uid"]
                    )
                )
            )
            .values(read_status="dismissed")
        )
        session.commit()


def _legacy_statement(
    session: Session,
    *,
    folder_id=None,
    source_id=None,
    q=None,
    read_status=None,
    starred=None,
    cursor=None,
    order="desc",
    limit=200,
    offset=0,
):
    """旧形状语义预言机：冻结自 e64634c 的 list_clusters 装配，勿改。"""
    stream = cluster_stream_query(
        session,
        (Cluster, func.count(ClusterItem.id)),
        folder_id=folder_id,
        source_id=source_id,
        q=q,
        read_status=read_status,
        starred=starred,
        search=STREAM_SEARCH,
    )
    stmt = stream.statement
    if cursor is not None:
        id_after_cursor = (
            Cluster.id > cursor.id if order == "asc" else Cluster.id < cursor.id
        )
        if cursor.first_seen_at is None:
            cursor_clause = and_(Cluster.first_seen_at.is_(None), id_after_cursor)
        else:
            time_after_cursor = (
                Cluster.first_seen_at > cursor.first_seen_at
                if order == "asc"
                else Cluster.first_seen_at < cursor.first_seen_at
            )
            cursor_clause = or_(
                time_after_cursor,
                and_(Cluster.first_seen_at == cursor.first_seen_at, id_after_cursor),
                Cluster.first_seen_at.is_(None),
            )
        stmt = stmt.where(cursor_clause)
    order_by = (
        (Cluster.first_seen_at.asc().nullslast(), Cluster.id.asc())
        if order == "asc"
        else (Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
    )
    return (
        stmt.group_by(Cluster.id)
        .order_by(*order_by)
        .limit(min(max(limit, 1), 200))
        .offset(max(offset, 0))
    )


def _page_rows(session: Session, statement) -> list[tuple[int, int]]:
    return [(cluster.id, int(count)) for cluster, count in session.execute(statement)]


COMBOS = [
    {},
    {"read_status": "unread"},
    {"read_status": "summary_seen"},
    {"read_status": "dismissed"},
    {"starred": True},
    {"starred": False},
    {"read_status": "unread", "starred": False},
]


def test_page_shape_matches_legacy_across_filters_and_scopes() -> None:
    ids = _seed_matrix()
    _apply_states(ids)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        scopes = [
            {},
            {"folder_id": ids["folder"]},
            {"source_id": ids["source_a"]},
            {"q": "update"},
        ]
        for scope in scopes:
            for combo in COMBOS:
                for order in ("desc", "asc"):
                    params = {**scope, **combo}
                    legacy = _page_rows(
                        session, _legacy_statement(session, order=order, **params)
                    )
                    paged = cluster_stream_page_query(
                        session,
                        search=STREAM_SEARCH,
                        folder_id=params.get("folder_id"),
                        source_id=params.get("source_id"),
                        q=params.get("q"),
                        read_status=params.get("read_status"),
                        starred=params.get("starred"),
                        order=order,
                        limit=200,
                    )
                    assert _page_rows(session, paged.statement) == legacy, (
                        f"scope={scope} combo={combo} order={order}"
                    )


def test_page_shape_matches_legacy_cursor_walk_and_offset() -> None:
    ids = _seed_matrix()
    _apply_states(ids)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        for order in ("desc", "asc"):
            cursor = None
            walked_pages = 0
            while True:
                legacy = _page_rows(
                    session,
                    _legacy_statement(session, cursor=cursor, order=order, limit=2),
                )
                paged = cluster_stream_page_query(
                    session,
                    search=STREAM_SEARCH,
                    folder_id=None,
                    source_id=None,
                    q=None,
                    read_status=None,
                    starred=None,
                    cursor=cursor,
                    order=order,
                    limit=2,
                )
                assert _page_rows(session, paged.statement) == legacy, (
                    f"order={order} page={walked_pages}"
                )
                if not legacy:
                    break
                cursor = session.get(Cluster, legacy[-1][0])
                walked_pages += 1
            assert walked_pages >= 4, "游标翻页应覆盖多页（含 NULL first_seen_at 尾页）"

        for offset in (0, 1, 3, 50):
            legacy = _page_rows(
                session, _legacy_statement(session, limit=3, offset=offset)
            )
            paged = cluster_stream_page_query(
                session,
                search=STREAM_SEARCH,
                folder_id=None,
                source_id=None,
                q=None,
                read_status=None,
                starred=None,
                limit=3,
                offset=offset,
            )
            assert _page_rows(session, paged.statement) == legacy, f"offset={offset}"


def test_page_shape_counts_only_visible_items_per_scope() -> None:
    """混合簇在自动流里只计未命中条目，主动查找显露命中条目。"""
    ids = _seed_matrix()
    _apply_states(ids)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        auto = dict(
            _page_rows(
                session,
                cluster_stream_page_query(
                    session,
                    search=STREAM_SEARCH,
                    folder_id=None,
                    source_id=None,
                    q=None,
                    read_status=None,
                    starred=None,
                    limit=200,
                ).statement,
            )
        )
        assert auto[ids["mixed_plain"]] == 1
        assert auto[ids["multi_1"]] == 3
        assert ids["filtered_only"] not in auto
        assert ids["uninterested"] not in auto
        assert ids["dismissed"] not in auto

        revealed = dict(
            _page_rows(
                session,
                cluster_stream_page_query(
                    session,
                    search=STREAM_SEARCH,
                    folder_id=None,
                    source_id=ids["source_a"],
                    q=None,
                    read_status=None,
                    starred=None,
                    limit=200,
                ).statement,
            )
        )
        assert revealed[ids["mixed_plain"]] == 2
        assert revealed[ids["filtered_only"]] == 1


def test_unread_windowed_two_phase_matches_legacy() -> None:
    """未读窗口化两段查询与旧形状单查逐行等价（小窗口强制续查腿）。"""
    from reader_api.event_stream import cluster_stream_unread_page_rows

    ids = _seed_matrix()
    _apply_states(ids)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        legacy_all = _page_rows(
            session, _legacy_statement(session, read_status="unread")
        )
        assert len(legacy_all) >= 4
        for window in (1, 2, 3, 200):
            for limit in (1, 2, 50):
                rows = [
                    (cluster.id, int(count))
                    for cluster, count in cluster_stream_unread_page_rows(
                        session, search=STREAM_SEARCH, limit=limit, window=window
                    )
                ]
                assert rows == legacy_all[:limit], f"window={window} limit={limit}"
        # 游标行走：每页 2 条走完全部未读，与旧形状分页一致
        for window in (2, 200):
            cursor = None
            walked = []
            while True:
                rows = cluster_stream_unread_page_rows(
                    session,
                    search=STREAM_SEARCH,
                    cursor=cursor,
                    limit=2,
                    window=window,
                )
                page = [(cluster.id, int(count)) for cluster, count in rows]
                legacy_page = _page_rows(
                    session,
                    _legacy_statement(
                        session, read_status="unread", cursor=cursor, limit=2
                    ),
                )
                assert page == legacy_page, f"window={window} cursor={cursor}"
                if not page:
                    break
                walked.extend(page)
                cursor = session.get(Cluster, page[-1][0])
            assert walked == legacy_all


def test_material_update_returns_cluster_to_unread_in_page_shape() -> None:
    """已读但有实质更新覆盖的簇必须出现在未读筛选（ADR-0020）。"""
    ids = _seed_matrix()
    _apply_states(ids)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        unread_rows = dict(
            _page_rows(
                session,
                cluster_stream_page_query(
                    session,
                    search=STREAM_SEARCH,
                    folder_id=None,
                    source_id=None,
                    q=None,
                    read_status="unread",
                    starred=None,
                    limit=200,
                ).statement,
            )
        )
        assert ids["material_seen"] in unread_rows
        assert ids["seen"] not in unread_rows
        assert ids["dismissed"] not in unread_rows
        legacy = dict(
            _page_rows(
                session, _legacy_statement(session, read_status="unread")
            )
        )
        assert set(unread_rows) == set(legacy)


def test_list_router_picks_page_shape_only_for_pure_default_stream() -> None:
    """窄混合路由：纯默认流走早退形状（无 GROUP BY），任何筛选走旧形状。"""
    from reader_api.event_stream import cluster_stream_list_query

    _seed_matrix()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        def sql_of(**params):
            merged = {
                "folder_id": None,
                "source_id": None,
                "q": None,
                "read_status": None,
                "starred": None,
                **params,
            }
            stream = cluster_stream_list_query(
                session, search=STREAM_SEARCH, limit=50, **merged
            )
            return str(
                stream.statement.compile(compile_kwargs={"literal_binds": False})
            )

        assert "GROUP BY" not in sql_of()
        assert "GROUP BY" in sql_of(read_status="unread")
        assert "GROUP BY" in sql_of(read_status="summary_seen")
        assert "GROUP BY" in sql_of(starred=True)
        assert "GROUP BY" in sql_of(starred=False)
        assert "GROUP BY" in sql_of(folder_id=1)
        assert "GROUP BY" in sql_of(source_id=1)
        assert "GROUP BY" in sql_of(q="update")
