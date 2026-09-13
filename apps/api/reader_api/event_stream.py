"""事件流选择（Event Stream Selection）。

文章事件进入自动列表、计数、批量未读候选与报告选材前的统一选择规则：

- 可见性关卡：排除硬过滤命中内容与"不感兴趣"目标。主动查找（指定来源、
  搜索、收藏视图）保留硬过滤命中内容；"不感兴趣"排除始终生效。
- 有效状态：ADR-0020 的版本感知阅读状态与收藏投影。

`docs/ARCHITECTURE.md` §2 要求所有自动列表、计数、批量未读候选和新报告
通过同一可见性 seam 排除命中项；本模块是该 seam 的唯一定义处。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from fastapi import HTTPException
from sqlalchemy import and_, case, false, func, or_, select, true
from sqlalchemy.orm import Session

from .content_filters import unfiltered_content_clause
from .event_projection import (
    cluster_current_event_state_lateral,
    cluster_current_event_state_projection,
)
from .models import Cluster, ClusterItem, ContentItem, Source
from .uninterested import ordinary_content_clause

READ_STATUSES = {"unread", "summary_seen", "original_opened", "dismissed"}


def include_filtered_for_scope(
    *,
    source_id: int | None,
    q: str | None,
    starred_view: bool,
) -> bool:
    """主动查找（指定来源、搜索、收藏视图）保留硬过滤命中内容。

    自动流、计数与批量未读候选一律通过可见性关卡；``starred_view`` 只在
    请求明确是收藏视图时为真——``starred=false`` 这类排除式筛选不是
    主动查找，不得显露命中内容。
    """
    return source_id is not None or bool(q) or starred_view


def effective_state_expressions_from(state_selectable):
    """从任一状态投影可选体导出 ADR-0020 有效阅读/收藏表达式（唯一定义处）。"""
    effective_read_status = case(
        (
            state_selectable.c.material_update_revision_uid.is_not(None),
            "unread",
        ),
        else_=func.coalesce(state_selectable.c.read_status, "unread"),
    )
    effective_starred = func.coalesce(state_selectable.c.starred, false())
    return effective_read_status, effective_starred


def cluster_effective_state_expressions(session: Session):
    """事件级有效状态：实质证据变化使事件回到未读（ADR-0020）。"""
    current_event_state = cluster_current_event_state_projection(session)
    effective_read_status, effective_starred = effective_state_expressions_from(
        current_event_state
    )
    return (
        current_event_state,
        effective_read_status,
        effective_starred,
    )


def visible_item_predicates(session: Session, *, active_lookup: bool) -> list[Any]:
    """条目级可见性关卡（唯一定义处）：active article 来源 + 普通内容，
    非主动查找时叠加硬过滤排除。"""
    predicates: list[Any] = [
        Source.status == "active",
        Source.media_type == "article",
        ordinary_content_clause(session, ContentItem.id),
    ]
    if not active_lookup:
        predicates.append(unfiltered_content_clause(ContentItem.id))
    return predicates


@dataclass(frozen=True)
class StreamSearch:
    """搜索适配器：把索引/模糊搜索实现注入事件流选择，避免反向依赖路由层。"""

    can_use_indexed: Callable[[Session, str], bool]
    indexed_membership: Callable[[Session, str, int | None, int | None], Any]
    clause: Callable[..., Any]


@dataclass(frozen=True)
class ClusterStreamQuery:
    statement: Any
    active_lookup: bool


def cluster_stream_query(
    session: Session,
    columns: tuple[Any, ...],
    *,
    folder_id: int | None,
    source_id: int | None,
    q: str | None,
    read_status: str | None,
    starred: bool | None,
    search: StreamSearch,
) -> ClusterStreamQuery:
    """列表与计数共用的文章事件流选择；``columns`` 决定取行还是取数。"""
    (
        current_event_state,
        effective_read_status,
        effective_starred,
    ) = cluster_effective_state_expressions(session)
    active_lookup = include_filtered_for_scope(
        source_id=source_id, q=q, starred_view=starred is True
    )
    stmt = (
        select(*columns)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(current_event_state, current_event_state.c.cluster_id == Cluster.id, isouter=True)
        .where(
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            *visible_item_predicates(session, active_lookup=active_lookup),
        )
    )
    if read_status:
        if read_status not in READ_STATUSES:
            raise HTTPException(status_code=400, detail="未知阅读状态")
        stmt = stmt.where(effective_read_status == read_status)
    elif starred is None:
        stmt = stmt.where(effective_read_status != "dismissed")
    if starred is not None:
        stmt = stmt.where(effective_starred.is_(starred))
    if folder_id is not None or source_id is not None or q:
        stmt = stmt.where(
            Cluster.id.in_(
                _matching_cluster_membership(
                    session,
                    folder_id=folder_id,
                    source_id=source_id,
                    q=q,
                    search=search,
                    active_lookup=active_lookup,
                )
            )
        )
    return ClusterStreamQuery(statement=stmt, active_lookup=active_lookup)


def _matching_cluster_membership(
    session: Session,
    *,
    folder_id: int | None,
    source_id: int | None,
    q: str | None,
    search: StreamSearch,
    active_lookup: bool,
):
    """folder/source/搜索范围的簇成员资格（列表与分页形状共用，唯一定义处）。"""
    matching_clusters = (
        select(ClusterItem.cluster_id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(Cluster, Cluster.id == ClusterItem.cluster_id)
        .where(Source.status == "active", Source.media_type == "article")
    )
    if folder_id is not None:
        matching_clusters = matching_clusters.where(Source.folder_id == folder_id)
    if source_id is not None:
        matching_clusters = matching_clusters.where(Source.id == source_id)
    if q and search.can_use_indexed(session, q):
        matching_clusters = search.indexed_membership(session, q, folder_id, source_id)
    elif q:
        matching_clusters = matching_clusters.where(
            ordinary_content_clause(session, ContentItem.id),
            search.clause(session, q, include_cluster=True),
        )
    if not active_lookup:
        matching_clusters = matching_clusters.where(
            unfiltered_content_clause(ContentItem.id)
        )
    return matching_clusters


def stream_cursor_clause(cursor: Cluster, order: str):
    """游标分页边界（含 first_seen_at NULL 分支）；列表路由与分页形状共用。"""
    id_after_cursor = Cluster.id > cursor.id if order == "asc" else Cluster.id < cursor.id
    if cursor.first_seen_at is None:
        return and_(Cluster.first_seen_at.is_(None), id_after_cursor)
    time_after_cursor = (
        Cluster.first_seen_at > cursor.first_seen_at
        if order == "asc"
        else Cluster.first_seen_at < cursor.first_seen_at
    )
    return or_(
        time_after_cursor,
        and_(Cluster.first_seen_at == cursor.first_seen_at, id_after_cursor),
        Cluster.first_seen_at.is_(None),
    )


def cluster_stream_list_query(
    session: Session,
    *,
    folder_id: int | None,
    source_id: int | None,
    q: str | None,
    read_status: str | None,
    starred: bool | None,
    search: StreamSearch,
    cursor: Cluster | None = None,
    order: str = "desc",
    limit: int,
    offset: int = 0,
) -> ClusterStreamQuery:
    """列表形状路由（#108 窄混合）：纯默认流走流序早退形状（实测首页/
    游标页 ~2.3ms、buffers ~3.8k）；任何 read_status/starred/folder/source/
    搜索筛选维持旧平铺 GROUP BY 形状——有效状态 CASE 表达式无统计，
    规划器在筛选下总是估行 < LIMIT 而回退全量物化，且新形状回退路径
    多耗 ~3× buffers。未读路径的可估计谓词重构另立 issue。"""
    if (
        read_status is None
        and starred is None
        and folder_id is None
        and source_id is None
        and not q
    ):
        return cluster_stream_page_query(
            session,
            folder_id=folder_id,
            source_id=source_id,
            q=q,
            read_status=read_status,
            starred=starred,
            search=search,
            cursor=cursor,
            order=order,
            limit=limit,
            offset=offset,
        )
    stream = cluster_stream_query(
        session,
        (Cluster, func.count(ClusterItem.id)),
        folder_id=folder_id,
        source_id=source_id,
        q=q,
        read_status=read_status,
        starred=starred,
        search=search,
    )
    stmt = stream.statement
    if cursor is not None:
        stmt = stmt.where(stream_cursor_clause(cursor, order))
    order_by = (
        (Cluster.first_seen_at.asc().nullslast(), Cluster.id.asc())
        if order == "asc"
        else (Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
    )
    stmt = (
        stmt.group_by(Cluster.id).order_by(*order_by).limit(limit).offset(offset)
    )
    return ClusterStreamQuery(statement=stmt, active_lookup=stream.active_lookup)


UNREAD_STREAM_WINDOW = 1000


def cluster_stream_unread_page_rows(
    session: Session,
    *,
    search: StreamSearch,
    cursor: Cluster | None = None,
    limit: int,
    window: int = UNREAD_STREAM_WINDOW,
) -> list[Any]:
    """未读筛选的窗口化两段查询（#109，仅 desc、无范围、无 offset 路由至此）。

    有效阅读状态是跨投影的 CASE 表达式，规划器无统计可用：估行恒为
    个位数时旧形状全量物化（~196k buffers），反连接改写实测又会在未读
    稀缺时误选全流扫描（~1.7M buffers）。改为确定性窗口：
    1. 索引只读取流序（游标后）前 ``window`` 个簇 id（毫秒级）；
    2. 在该 id 窗口内跑旧平铺形状（物化范围有界，~窗口/全库 比例的
       buffers，任何计划都便宜）；
    3. 不足 ``limit`` 且窗口取满时，以窗口边缘为游标对余下整个流跑一次
       旧形状续查（仅未读稀缺时触发，成本即原全量基线）。
    结果与旧形状单查逐行等价，由分页预言机回归（含小窗口强制续查）钉住。
    """
    collected: list[Any] = []
    window_cursor = cursor
    order = "desc"
    window_stmt = select(Cluster.id, Cluster.first_seen_at).order_by(
        Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc()
    ).limit(window)
    if window_cursor is not None:
        window_stmt = window_stmt.where(stream_cursor_clause(window_cursor, order))
    window_rows = session.execute(window_stmt).all()
    if window_rows:
        stream = cluster_stream_query(
            session,
            (Cluster, func.count(ClusterItem.id)),
            folder_id=None,
            source_id=None,
            q=None,
            read_status="unread",
            starred=None,
            search=search,
        )
        stmt = (
            stream.statement.where(
                Cluster.id.in_([row.id for row in window_rows])
            )
            .group_by(Cluster.id)
            .order_by(Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
            .limit(limit)
        )
        collected.extend(session.execute(stmt).all())
    if len(collected) < limit and len(window_rows) == window:
        edge = session.get(Cluster, window_rows[-1].id)
        stream = cluster_stream_query(
            session,
            (Cluster, func.count(ClusterItem.id)),
            folder_id=None,
            source_id=None,
            q=None,
            read_status="unread",
            starred=None,
            search=search,
        )
        stmt = (
            stream.statement.where(stream_cursor_clause(edge, order))
            .group_by(Cluster.id)
            .order_by(Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
            .limit(limit - len(collected))
        )
        collected.extend(session.execute(stmt).all())
    return collected


def cluster_stream_page_query(
    session: Session,
    *,
    folder_id: int | None,
    source_id: int | None,
    q: str | None,
    read_status: str | None,
    starred: bool | None,
    search: StreamSearch,
    cursor: Cluster | None = None,
    order: str = "desc",
    limit: int,
    offset: int = 0,
) -> ClusterStreamQuery:
    """分页专用流序形状（#108）：clusters 为驱动表沿 (first_seen_at DESC
    NULLS LAST, id DESC) 走 ``ix_clusters_stream_order``，逐簇评估条目级
    关卡（EXISTS 成员资格 + 相关标量计数）与簇级状态（PostgreSQL 相关
    LATERAL 投影探针，SQLite 保持 subquery join），LIMIT 早退。

    语义与 ``cluster_stream_query`` + GROUP BY 的旧列表装配逐行等价，由
    ``tests/test_event_stream_page.py`` 的预言机回归钉住；count 与批量
    未读候选继续走旧形状。
    """
    active_lookup = include_filtered_for_scope(
        source_id=source_id, q=q, starred_view=starred is True
    )
    item_scope = [
        ClusterItem.cluster_id == Cluster.id,
        *visible_item_predicates(session, active_lookup=active_lookup),
    ]
    visible_exists = (
        select(ClusterItem.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(*item_scope)
        .exists()
    )
    item_count = (
        select(func.count(ClusterItem.id))
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(*item_scope)
        .scalar_subquery()
        .label("item_count")
    )

    if session.get_bind().dialect.name == "postgresql":
        state = cluster_current_event_state_lateral(session, Cluster.id)
        state_onclause = true()
    else:
        state = cluster_current_event_state_projection(session)
        state_onclause = state.c.cluster_id == Cluster.id
    effective_read_status, effective_starred = effective_state_expressions_from(state)

    stmt = (
        select(Cluster, item_count)
        .join(state, state_onclause, isouter=True)
        .where(
            visible_exists,
            func.coalesce(state.c.uninterested, false()).is_(False),
        )
    )
    if read_status:
        if read_status not in READ_STATUSES:
            raise HTTPException(status_code=400, detail="未知阅读状态")
        stmt = stmt.where(effective_read_status == read_status)
    elif starred is None:
        stmt = stmt.where(effective_read_status != "dismissed")
    if starred is not None:
        stmt = stmt.where(effective_starred.is_(starred))
    if folder_id is not None or source_id is not None or q:
        stmt = stmt.where(
            Cluster.id.in_(
                _matching_cluster_membership(
                    session,
                    folder_id=folder_id,
                    source_id=source_id,
                    q=q,
                    search=search,
                    active_lookup=active_lookup,
                )
            )
        )
    if cursor is not None:
        stmt = stmt.where(stream_cursor_clause(cursor, order))
    order_by = (
        (Cluster.first_seen_at.asc().nullslast(), Cluster.id.asc())
        if order == "asc"
        else (Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
    )
    stmt = stmt.order_by(*order_by).limit(limit).offset(offset)
    return ClusterStreamQuery(statement=stmt, active_lookup=active_lookup)


def unread_event_stream_membership(session: Session):
    """未读文章事件的统一成员资格（cluster × source × folder × media_type）。

    来源、文件夹与全局未读计数都必须从这一份成员资格导出，保证三级徽标
    口径一致地通过可见性关卡。
    """
    (
        current_event_state,
        effective_read_status,
        _effective_starred,
    ) = cluster_effective_state_expressions(session)
    return (
        select(
            ClusterItem.cluster_id.label("cluster_id"),
            ContentItem.source_id.label("source_id"),
            Source.folder_id.label("folder_id"),
            Source.media_type.label("media_type"),
        )
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(
            current_event_state,
            current_event_state.c.cluster_id == ClusterItem.cluster_id,
            isouter=True,
        )
        .where(
            Source.status == "active",
            effective_read_status == "unread",
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .distinct()
        .cte("unread_membership")
    )
