from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import exists, func, literal, select, union_all
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from .event_interactions import lock_operation_id
from .event_projection import latest_live_cluster_projection
from .models import (
    Cluster,
    ClusterEventProjection,
    ClusterItem,
    ContentItem,
    DELETED_SOURCE_STATUS,
    Event,
    EventRevision,
    EventUserState,
    FilterMatch,
    FilterRule,
    InteractionEvent,
    Source,
    UserState,
    now_utc,
)
from .schemas import (
    UninterestedMutationIn,
    UninterestedMutationOut,
    UninterestedTargetOut,
    UninterestedTargetsOut,
)


@dataclass(frozen=True)
class UninterestedFeedback:
    reason: str | None
    note: str | None
    marked_at: datetime | None


@dataclass(frozen=True)
class CurrentEventTarget:
    event_id: int
    event_uid: str
    revision_id: int
    revision_uid: str
    cluster_id: int


def ordinary_content_clause(
    session: Session,
    content_item_id: ColumnElement[int] = ContentItem.id,
) -> ColumnElement[bool]:
    direct = exists(
        select(literal(1))
        .select_from(UserState)
        .where(
            UserState.object_type == "item",
            UserState.object_id == content_item_id,
            UserState.uninterested.is_(True),
        )
        .correlate(ContentItem)
    )
    latest = latest_live_cluster_projection(session)
    through_event = exists(
        select(literal(1))
        .select_from(ClusterItem)
        .join(latest, latest.c.cluster_id == ClusterItem.cluster_id)
        .join(
            ClusterEventProjection,
            ClusterEventProjection.id == latest.c.projection_id,
        )
        .join(Event, Event.id == ClusterEventProjection.event_id)
        .join(EventUserState, EventUserState.event_id == Event.id)
        .where(
            ClusterItem.content_item_id == content_item_id,
            Event.status == "active",
            EventUserState.uninterested.is_(True),
        )
        .correlate(ContentItem)
    )
    return ~direct & ~through_event


def apply_uninterested_mutation(
    session: Session,
    mutation: UninterestedMutationIn,
) -> UninterestedMutationOut:
    lock_operation_id(session, mutation.operation_id)
    existing = session.scalar(
        select(InteractionEvent).where(
            InteractionEvent.operation_id == mutation.operation_id
        )
    )
    if existing is not None:
        return _original_result(existing, mutation)

    target = _resolve_target(session, mutation)
    occurred_at = now_utc()
    if isinstance(target, CurrentEventTarget):
        state = session.scalar(
            select(EventUserState)
            .where(EventUserState.event_id == target.event_id)
            .with_for_update()
        )
        if state is None:
            state = EventUserState(
                event_id=target.event_id,
                seen_revision_id=None,
                read_status="unread",
                read_later=False,
                starred=False,
                uninterested=False,
            )
            session.add(state)
        _project_feedback(state, mutation, occurred_at)
        affected_item_ids = _cluster_item_ids(session, target.cluster_id)
        result = UninterestedMutationOut(
            operation_id=mutation.operation_id,
            target_kind="event",
            event_uid=target.event_uid,
            observed_revision_uid=target.revision_uid,
            cluster_id=target.cluster_id,
            affected_item_ids=affected_item_ids,
            uninterested=state.uninterested,
            reason=state.uninterested_reason,
            note=state.uninterested_note,
            marked_at=state.uninterested_at,
            updated_at=occurred_at,
        )
        interaction = InteractionEvent(
            operation_id=mutation.operation_id,
            target_kind="event",
            event_id=target.event_id,
            observed_revision_id=target.revision_id,
            action="uninterested_set",
            set_value=mutation.value,
            payload=_interaction_payload(mutation, result),
            occurred_at=occurred_at,
            recorded_at=occurred_at,
        )
    else:
        item = target
        state = session.scalar(
            select(UserState)
            .where(
                UserState.object_type == "item",
                UserState.object_id == item.id,
            )
            .with_for_update()
        )
        if state is None:
            state = UserState(
                object_type="item",
                object_id=item.id,
                read_status="unread",
                read_later=False,
                starred=False,
                uninterested=False,
            )
            session.add(state)
        _project_feedback(state, mutation, occurred_at)
        result = UninterestedMutationOut(
            operation_id=mutation.operation_id,
            target_kind="item",
            item_id=item.id,
            affected_item_ids=[item.id],
            uninterested=state.uninterested,
            reason=state.uninterested_reason,
            note=state.uninterested_note,
            marked_at=state.uninterested_at,
            updated_at=occurred_at,
        )
        interaction = InteractionEvent(
            operation_id=mutation.operation_id,
            target_kind="legacy",
            object_type="item",
            object_id=item.id,
            action="uninterested_set",
            set_value=mutation.value,
            payload=_interaction_payload(mutation, result),
            occurred_at=occurred_at,
            recorded_at=occurred_at,
        )
    session.add(interaction)
    session.flush()
    return result


def list_uninterested_targets(
    session: Session,
    *,
    q: str | None = None,
    reason: str | None = None,
    source_id: int | None = None,
    folder_id: int | None = None,
    limit: int = 80,
    offset: int = 0,
) -> UninterestedTargetsOut:
    # ponytail: a personal bucket is scanned in memory so rule coverage and
    # cross-source filters stay simple; paginate in SQL if this reaches thousands.
    latest = latest_live_cluster_projection(session)
    event_rows = session.execute(
        select(Cluster, Event, EventRevision, EventUserState)
        .select_from(latest)
        .join(Cluster, Cluster.id == latest.c.cluster_id)
        .join(
            ClusterEventProjection,
            ClusterEventProjection.id == latest.c.projection_id,
        )
        .join(Event, Event.id == ClusterEventProjection.event_id)
        .join(EventRevision, EventRevision.id == Event.current_revision_id)
        .join(EventUserState, EventUserState.event_id == Event.id)
        .where(
            Event.status == "active",
            EventUserState.uninterested.is_(True),
        )
    ).all()
    cluster_ids = [cluster.id for cluster, _event, _revision, _state in event_rows]
    member_rows = (
        session.execute(
            select(ClusterItem.cluster_id, ContentItem, Source)
            .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
            .join(Source, Source.id == ContentItem.source_id)
            .where(
                ClusterItem.cluster_id.in_(cluster_ids),
                Source.status != "deleted",
            )
            .order_by(
                ClusterItem.cluster_id,
                ContentItem.published_at.asc().nullslast(),
                ContentItem.id,
            )
        ).all()
        if cluster_ids
        else []
    )
    members: dict[int, list[tuple[ContentItem, Source]]] = {}
    for cluster_id, item, source in member_rows:
        members.setdefault(cluster_id, []).append((item, source))
    event_item_ids = {
        item.id for rows in members.values() for item, _source in rows
    }

    item_states = list(
        session.scalars(
            select(UserState).where(
                UserState.object_type == "item",
                UserState.uninterested.is_(True),
            )
        )
    )
    standalone_ids = [state.object_id for state in item_states]
    standalone_rows = (
        session.execute(
            select(ContentItem, Source)
            .join(Source, Source.id == ContentItem.source_id)
            .where(
                ContentItem.id.in_(standalone_ids),
                Source.status != "deleted",
            )
        ).all()
        if standalone_ids
        else []
    )
    standalone_by_id = {item.id: (item, source) for item, source in standalone_rows}

    all_item_ids = [
        item.id
        for rows in members.values()
        for item, _source in rows
    ] + standalone_ids
    filtered_ids = _active_filtered_item_ids(session, all_item_ids)
    targets: list[UninterestedTargetOut] = []

    for cluster, event, revision, state in event_rows:
        rows = members.get(cluster.id, [])
        item_ids = [item.id for item, _source in rows]
        if item_ids and all(item_id in filtered_ids for item_id in item_ids):
            continue
        sources = _unique_sources(rows)
        summary = cluster.generated_summary or (
            rows[0][0].summary or rows[0][0].content_text[:500] if rows else ""
        )
        target = UninterestedTargetOut(
            target_kind="event",
            event_uid=event.uid,
            current_revision_uid=revision.uid,
            cluster_id=cluster.id,
            item_ids=item_ids,
            title=revision.title_snapshot or cluster.generated_title or cluster.title,
            summary=_preview(summary),
            source_ids=[source.id for source in sources],
            source_names=[source.name for source in sources],
            media_type="article",
            item_count=len(item_ids),
            read_status=state.read_status,
            read_later=state.read_later,
            starred=state.starred,
            reason=state.uninterested_reason,
            note=state.uninterested_note,
            marked_at=state.uninterested_at,
        )
        if _target_matches(
            target,
            rows,
            q=q,
            reason=reason,
            source_id=source_id,
            folder_id=folder_id,
        ):
            targets.append(target)

    for state in item_states:
        row = standalone_by_id.get(state.object_id)
        if (
            row is None
            or state.object_id in filtered_ids
            or state.object_id in event_item_ids
        ):
            continue
        item, source = row
        target = UninterestedTargetOut(
            target_kind="item",
            item_id=item.id,
            item_ids=[item.id],
            title=item.title,
            summary=_preview(item.summary or item.content_text),
            source_ids=[source.id],
            source_names=[source.name],
            media_type=source.media_type,
            item_count=1,
            read_status=state.read_status,
            read_later=state.read_later,
            starred=state.starred,
            reason=state.uninterested_reason,
            note=state.uninterested_note,
            marked_at=state.uninterested_at,
        )
        if _target_matches(
            target,
            [(item, source)],
            q=q,
            reason=reason,
            source_id=source_id,
            folder_id=folder_id,
        ):
            targets.append(target)

    targets.sort(key=lambda target: target.marked_at, reverse=True)
    start = max(offset, 0)
    page_size = min(max(limit, 1), 200)
    return UninterestedTargetsOut(
        count=len(targets),
        items=targets[start : start + page_size],
    )


def uninterested_feedback_for_items(
    session: Session,
    item_ids: list[int],
) -> dict[int, UninterestedFeedback]:
    ids = sorted({int(item_id) for item_id in item_ids if item_id})
    if not ids:
        return {}
    latest = latest_live_cluster_projection(session)
    direct = select(
        UserState.object_id.label("item_id"),
        UserState.uninterested_reason.label("reason"),
        UserState.uninterested_note.label("note"),
        UserState.uninterested_at.label("marked_at"),
        literal(0).label("priority"),
    ).where(
        UserState.object_type == "item",
        UserState.object_id.in_(ids),
        UserState.uninterested.is_(True),
    )
    through_event = (
        select(
            ClusterItem.content_item_id.label("item_id"),
            EventUserState.uninterested_reason.label("reason"),
            EventUserState.uninterested_note.label("note"),
            EventUserState.uninterested_at.label("marked_at"),
            literal(1).label("priority"),
        )
        .select_from(ClusterItem)
        .join(latest, latest.c.cluster_id == ClusterItem.cluster_id)
        .join(
            ClusterEventProjection,
            ClusterEventProjection.id == latest.c.projection_id,
        )
        .join(Event, Event.id == ClusterEventProjection.event_id)
        .join(EventUserState, EventUserState.event_id == Event.id)
        .where(
            ClusterItem.content_item_id.in_(ids),
            Event.status == "active",
            EventUserState.uninterested.is_(True),
        )
    )
    combined = union_all(direct, through_event).subquery()
    feedback: dict[int, UninterestedFeedback] = {}
    for item_id, reason, note, marked_at, _priority in session.execute(
        select(combined).order_by(combined.c.priority)
    ):
        feedback[item_id] = UninterestedFeedback(
            reason=reason,
            note=note,
            marked_at=marked_at,
        )
    return feedback


def _resolve_target(
    session: Session,
    mutation: UninterestedMutationIn,
) -> CurrentEventTarget | ContentItem:
    if mutation.target_type == "event":
        assert mutation.event_uid is not None
        assert mutation.observed_revision_uid is not None
        event = session.scalar(
            select(Event)
            .where(Event.uid == mutation.event_uid)
            .with_for_update()
        )
        if event is None:
            raise HTTPException(status_code=404, detail="Event 不存在")
        if event.status != "active":
            raise HTTPException(status_code=409, detail="Event 已被后继事件取代，请刷新")
        revision = session.scalar(
            select(EventRevision).where(
                EventRevision.uid == mutation.observed_revision_uid
            )
        )
        if revision is None or revision.event_id != event.id:
            raise HTTPException(
                status_code=409,
                detail="observed revision 不属于目标 Event",
            )
        cluster_id = _current_cluster_id(session, event.id)
        if cluster_id is None:
            raise HTTPException(status_code=409, detail="Event 当前投影缺失，请刷新")
        return CurrentEventTarget(
            event_id=event.id,
            event_uid=event.uid,
            revision_id=revision.id,
            revision_uid=revision.uid,
            cluster_id=cluster_id,
        )

    assert mutation.item_id is not None
    item = session.scalar(
        select(ContentItem)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            ContentItem.id == mutation.item_id,
            Source.status != DELETED_SOURCE_STATUS,
        )
        .with_for_update()
    )
    if item is None:
        raise HTTPException(status_code=404, detail="条目不存在")
    if mutation.target_type == "item":
        return item
    target = _current_event_for_item(session, item.id)
    if target is None:
        return item
    event = session.scalar(
        select(Event)
        .where(Event.id == target.event_id)
        .with_for_update()
    )
    if event is None or event.status != "active":
        return item
    return target


def _project_feedback(
    state: EventUserState | UserState,
    mutation: UninterestedMutationIn,
    occurred_at: datetime,
) -> None:
    was_uninterested = state.uninterested
    state.uninterested = mutation.value
    if mutation.value:
        state.uninterested_reason = mutation.reason
        state.uninterested_note = mutation.note
        if not was_uninterested or state.uninterested_at is None:
            state.uninterested_at = occurred_at
    else:
        state.uninterested_reason = None
        state.uninterested_note = None
        state.uninterested_at = None
    state.updated_at = occurred_at


def _current_event_for_item(
    session: Session,
    item_id: int,
) -> CurrentEventTarget | None:
    latest = latest_live_cluster_projection(session)
    row = session.execute(
        select(
            Event.id,
            Event.uid,
            EventRevision.id,
            EventRevision.uid,
            ClusterItem.cluster_id,
        )
        .select_from(ClusterItem)
        .join(latest, latest.c.cluster_id == ClusterItem.cluster_id)
        .join(
            ClusterEventProjection,
            ClusterEventProjection.id == latest.c.projection_id,
        )
        .join(Event, Event.id == ClusterEventProjection.event_id)
        .join(EventRevision, EventRevision.id == Event.current_revision_id)
        .where(
            ClusterItem.content_item_id == item_id,
            Event.status == "active",
        )
        .limit(1)
    ).first()
    return (
        CurrentEventTarget(
            event_id=row[0],
            event_uid=row[1],
            revision_id=row[2],
            revision_uid=row[3],
            cluster_id=row[4],
        )
        if row is not None
        else None
    )


def _current_cluster_id(session: Session, event_id: int) -> int | None:
    latest = latest_live_cluster_projection(session)
    return session.scalar(
        select(latest.c.cluster_id)
        .join(
            ClusterEventProjection,
            ClusterEventProjection.id == latest.c.projection_id,
        )
        .where(ClusterEventProjection.event_id == event_id)
        .limit(1)
    )


def _cluster_item_ids(session: Session, cluster_id: int) -> list[int]:
    return list(
        session.scalars(
            select(ClusterItem.content_item_id)
            .where(ClusterItem.cluster_id == cluster_id)
            .order_by(ClusterItem.content_item_id)
        )
    )


def _interaction_payload(
    mutation: UninterestedMutationIn,
    result: UninterestedMutationOut,
) -> dict[str, object]:
    return {
        "request": mutation.model_dump(mode="json"),
        "result": result.model_dump(mode="json"),
    }


def _original_result(
    existing: InteractionEvent,
    mutation: UninterestedMutationIn,
) -> UninterestedMutationOut:
    payload = existing.payload if isinstance(existing.payload, dict) else {}
    if (
        existing.action != "uninterested_set"
        or payload.get("request") != mutation.model_dump(mode="json")
    ):
        raise HTTPException(status_code=409, detail="operation_id 已用于另一项操作")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Interaction Event 缺少原操作结果")
    return UninterestedMutationOut.model_validate(result)


def _active_filtered_item_ids(session: Session, item_ids: list[int]) -> set[int]:
    ids = sorted({int(item_id) for item_id in item_ids if item_id})
    if not ids:
        return set()
    return set(
        session.scalars(
            select(FilterMatch.content_item_id)
            .join(FilterRule, FilterRule.id == FilterMatch.rule_id)
            .where(
                FilterMatch.content_item_id.in_(ids),
                FilterRule.enabled.is_(True),
            )
            .distinct()
        )
    )


def _unique_sources(
    rows: list[tuple[ContentItem, Source]],
) -> list[Source]:
    by_id = {source.id: source for _item, source in rows}
    return [by_id[source_id] for source_id in sorted(by_id)]


def _target_matches(
    target: UninterestedTargetOut,
    rows: list[tuple[ContentItem, Source]],
    *,
    q: str | None,
    reason: str | None,
    source_id: int | None,
    folder_id: int | None,
) -> bool:
    if reason and target.reason != reason:
        return False
    if source_id is not None and source_id not in target.source_ids:
        return False
    if folder_id is not None and not any(
        source.folder_id == folder_id for _item, source in rows
    ):
        return False
    term = (q or "").strip().casefold()
    if not term:
        return True
    searchable = "\n".join(
        [
            target.title,
            target.summary,
            *target.source_names,
            *[
                "\n".join((item.title, item.summary, item.content_text))
                for item, _source in rows
            ],
        ]
    ).casefold()
    return term in searchable


def _preview(value: str, limit: int = 500) -> str:
    text = " ".join(value.split())
    return text if len(text) <= limit else f"{text[:limit]}…"
