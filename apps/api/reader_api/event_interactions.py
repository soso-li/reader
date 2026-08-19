from __future__ import annotations

import hashlib

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .event_projection import event_material_updates_for
from .feed_metrics import locked_feed_metric, refresh_source_trust_score
from .models import (
    DELETED_SOURCE_STATUS,
    Event,
    EventEvidenceVersion,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    FeedMetric,
    InteractionEvent,
    MigrationBaseline,
    Source,
    now_utc,
)
from .reading_state import next_read_status
from .schemas import EventUserStateMutationIn, EventUserStateMutationOut


EVENT_STATE_ACTION_FIELDS = {
    "starred_set": "starred",
    "read_later_set": "read_later",
}
EVENT_READ_STATUS_ACTION = "read_status_set"
EVENT_READ_STATUSES = {"unread", "summary_seen", "original_opened"}
SEEN_READ_STATUSES = {"summary_seen", "original_opened"}


def apply_event_user_state_mutation(
    session: Session,
    mutation: EventUserStateMutationIn,
) -> EventUserStateMutationOut:
    field = EVENT_STATE_ACTION_FIELDS.get(mutation.action)
    is_read_status = mutation.action == EVENT_READ_STATUS_ACTION
    if field is None and not is_read_status:
        raise HTTPException(status_code=400, detail="不支持的 Event 状态操作")
    if field is not None and (
        not isinstance(mutation.value, bool)
        or mutation.source_id is not None
        or mutation.evidence_version_uid is not None
    ):
        raise HTTPException(status_code=400, detail="Event 保存状态必须提交布尔 set 值")
    if is_read_status and (
        not isinstance(mutation.value, str)
        or mutation.value not in EVENT_READ_STATUSES
    ):
        raise HTTPException(status_code=400, detail="不支持的 Event 阅读状态")
    if is_read_status and mutation.value == "original_opened":
        if mutation.source_id is None:
            raise HTTPException(status_code=400, detail="打开原文必须提交来源")
    elif is_read_status and (
        mutation.source_id is not None or mutation.evidence_version_uid is not None
    ):
        raise HTTPException(status_code=400, detail="只有打开原文可以提交来源")

    lock_operation_id(session, mutation.operation_id)
    existing = session.scalar(
        select(InteractionEvent).where(
            InteractionEvent.operation_id == mutation.operation_id
        )
    )
    if existing is not None:
        return original_operation_result(session, existing, mutation)

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
    if revision is None:
        raise HTTPException(status_code=404, detail="Event Revision 不存在")
    if revision.event_id != event.id:
        raise HTTPException(
            status_code=409,
            detail="observed revision 不属于目标 Event",
        )

    observed_sources = revision_source_ids(session, revision.id)
    opened_evidence = None
    if mutation.evidence_version_uid is not None:
        opened_evidence = session.scalar(
            select(EventEvidenceVersion)
            .join(
                EventRevisionEvidence,
                EventRevisionEvidence.evidence_version_id
                == EventEvidenceVersion.id,
            )
            .where(
                EventRevisionEvidence.revision_id == revision.id,
                EventEvidenceVersion.uid == mutation.evidence_version_uid,
            )
        )
        if (
            opened_evidence is None
            or opened_evidence.source_id != mutation.source_id
        ):
            raise HTTPException(
                status_code=409,
                detail="打开的 Evidence 不属于 observed revision 或来源",
            )
    previous_sources: list[int] = []
    read_sources: list[int] = []
    opened_sources: list[int] = []
    if field is not None:
        previous_sources = previous_metric_sources(session, event.id, mutation.action)
        metric_source_ids = sorted(set(observed_sources).union(previous_sources))
    else:
        (
            read_sources,
            opened_sources,
            baseline_read_sources,
        ) = previous_read_metric_sources(session, event.id)
        if (
            mutation.value == "original_opened"
            and mutation.source_id not in observed_sources
        ):
            raise HTTPException(
                status_code=409,
                detail="打开的来源不属于 observed revision",
            )
        metric_source_ids = sorted(
            set(observed_sources).union(read_sources, opened_sources)
        )
    locked_metrics = lock_feed_metrics(session, metric_source_ids)
    require_live_event_sources(locked_metrics, observed_sources)

    state = session.scalar(
        select(EventUserState)
        .where(EventUserState.event_id == event.id)
        .with_for_update()
    )
    if state is None:
        state = EventUserState(
            event_id=event.id,
            seen_revision_id=None,
            read_status="unread",
            read_later=False,
            starred=False,
        )
        session.add(state)

    occurred_at = now_utc()
    interaction_payload: dict[str, object]
    if field is not None:
        previous_value = bool(getattr(state, field))
        next_sources, metric_delta = metric_projection_change(
            previous_value=previous_value,
            next_value=bool(mutation.value),
            previous_sources=previous_sources,
            observed_sources=observed_sources,
        )
        setattr(state, field, mutation.value)
        metric_field = (
            "starred_count" if field == "starred" else "read_later_count"
        )
        apply_metric_deltas(
            session,
            {
                source_id: {metric_field: delta}
                for source_id, delta in metric_delta.items()
            },
            locked_metrics,
        )
        interaction_payload = {
            "metric_source_ids": next_sources,
            "metric_delta": {
                str(key): value for key, value in metric_delta.items()
            },
        }
    else:
        interaction_payload = apply_event_read_status(
            session,
            event=event,
            revision=revision,
            state=state,
            requested_status=str(mutation.value),
            opened_source_id=mutation.source_id,
            opened_evidence_version_uid=(
                opened_evidence.uid if opened_evidence is not None else None
            ),
            observed_sources=observed_sources,
            read_sources=read_sources,
            opened_sources=opened_sources,
            baseline_read_sources=baseline_read_sources,
            locked_metrics=locked_metrics,
        )

    state.updated_at = occurred_at
    result_data: dict[str, object] = {
        "operation_id": mutation.operation_id,
        "event_uid": event.uid,
        "observed_revision_uid": revision.uid,
        "action": mutation.action,
        "value": mutation.value,
        "read_later": state.read_later,
        "starred": state.starred,
        "updated_at": occurred_at,
    }
    if is_read_status:
        session.flush()
        seen_revision = (
            session.get(EventRevision, state.seen_revision_id)
            if state.seen_revision_id is not None
            else None
        )
        result_data.update(
            source_id=mutation.source_id,
            read_status=state.read_status,
            seen_revision_uid=seen_revision.uid if seen_revision else None,
            current_revision_differs_from_seen=(
                event.current_revision_id != state.seen_revision_id
            ),
            material_update_revision_uid=event_material_updates_for(
                session, [event.id]
            ).get(event.id),
        )
        result_data["has_material_update"] = (
            result_data["material_update_revision_uid"] is not None
        )
        if opened_evidence is not None:
            result_data["evidence_version_uid"] = opened_evidence.uid
    result = EventUserStateMutationOut(**result_data)
    result_payload = result.model_dump(mode="json", exclude_unset=True)
    interaction_payload["result"] = result_payload
    interaction = InteractionEvent(
        operation_id=mutation.operation_id,
        target_kind="event",
        event_id=event.id,
        observed_revision_id=revision.id,
        action=mutation.action,
        set_value=mutation.value,
        payload=interaction_payload,
        occurred_at=occurred_at,
        recorded_at=occurred_at,
    )
    session.add(interaction)
    session.flush()
    return result


def apply_event_read_status(
    session: Session,
    *,
    event: Event,
    revision: EventRevision,
    state: EventUserState,
    requested_status: str,
    opened_source_id: int | None,
    opened_evidence_version_uid: str | None,
    observed_sources: list[int],
    read_sources: list[int],
    opened_sources: list[int],
    baseline_read_sources: list[int],
    locked_metrics: dict[int, tuple[Source, FeedMetric]],
) -> dict[str, object]:
    metric_deltas: dict[int, dict[str, int]] = {}
    if requested_status == "unread":
        next_read_sources: list[int] = []
        next_opened_sources: list[int] = []
        for source_id in read_sources:
            metric_deltas.setdefault(source_id, {})["read_count"] = -1
        for source_id in opened_sources:
            metric_deltas.setdefault(source_id, {})["opened_count"] = -1
    else:
        known_read_sources = set(read_sources).union(baseline_read_sources)
        newly_read_sources = sorted(
            set(observed_sources).difference(known_read_sources)
        )
        next_read_sources = sorted(set(read_sources).union(newly_read_sources))
        next_opened_sources = list(opened_sources)
        for source_id in newly_read_sources:
            metric_deltas.setdefault(source_id, {})["read_count"] = 1
        if requested_status == "original_opened":
            assert opened_source_id is not None
            next_opened_sources = sorted(set(opened_sources).union({opened_source_id}))
            if opened_source_id not in opened_sources:
                metric_deltas.setdefault(opened_source_id, {})["opened_count"] = 1

    state.read_status = next_read_status(state.read_status, requested_status)
    if requested_status in SEEN_READ_STATUSES:
        seen_revision = (
            session.get(EventRevision, state.seen_revision_id)
            if state.seen_revision_id is not None
            else None
        )
        if seen_revision is None or revision.revision_no > seen_revision.revision_no:
            state.seen_revision_id = revision.id

    apply_metric_deltas(session, metric_deltas, locked_metrics)
    payload: dict[str, object] = {
        "read_metric_source_ids": next_read_sources,
        "opened_metric_source_ids": next_opened_sources,
        "metric_delta": {
            str(source_id): deltas
            for source_id, deltas in sorted(metric_deltas.items())
        },
    }
    if opened_source_id is not None:
        payload["opened_source_id"] = opened_source_id
    if opened_evidence_version_uid is not None:
        payload["opened_evidence_version_uid"] = opened_evidence_version_uid
    return payload


def lock_feed_metrics(
    session: Session,
    source_ids: list[int],
) -> dict[int, tuple[Source, FeedMetric]]:
    """Lock durable Sources before publishing Event metrics."""

    return {
        source_id: locked_feed_metric(session, source_id)
        for source_id in sorted(set(source_ids))
    }


def require_live_event_sources(
    locked_metrics: dict[int, tuple[Source, FeedMetric]],
    observed_source_ids: list[int] | set[int],
) -> None:
    if any(
        locked_metrics[source_id][0].status == DELETED_SOURCE_STATUS
        for source_id in observed_source_ids
    ):
        raise HTTPException(status_code=409, detail="Event 来源已失效，请刷新")


def apply_metric_deltas(
    session: Session,
    metric_deltas: dict[int, dict[str, int]],
    locked_metrics: dict[int, tuple[Source, FeedMetric]],
) -> None:
    for source_id, deltas in sorted(metric_deltas.items()):
        source, metric = locked_metrics[source_id]
        for metric_field, delta in sorted(deltas.items()):
            current = int(getattr(metric, metric_field) or 0)
            setattr(metric, metric_field, max(current + delta, 0))
        refresh_source_trust_score(session, source, metric)


def lock_operation_id(session: Session, operation_id: str) -> None:
    if session.bind is None or session.bind.dialect.name != "postgresql":
        return
    lock_key = int.from_bytes(
        hashlib.sha256(operation_id.encode("utf-8")).digest()[:8],
        byteorder="big",
        signed=True,
    )
    session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": lock_key},
    )


def original_operation_result(
    session: Session,
    existing: InteractionEvent,
    mutation: EventUserStateMutationIn,
) -> EventUserStateMutationOut:
    event = session.get(Event, existing.event_id) if existing.event_id else None
    revision = (
        session.get(EventRevision, existing.observed_revision_id)
        if existing.observed_revision_id
        else None
    )
    payload = existing.payload if isinstance(existing.payload, dict) else {}
    result = payload.get("result")
    stored_source_id = result.get("source_id") if isinstance(result, dict) else None
    stored_evidence_version_uid = (
        result.get("evidence_version_uid") if isinstance(result, dict) else None
    )
    matches = (
        existing.target_kind == "event"
        and event is not None
        and revision is not None
        and event.uid == mutation.event_uid
        and revision.uid == mutation.observed_revision_uid
        and existing.action == mutation.action
        and existing.set_value == mutation.value
        and stored_source_id == mutation.source_id
        and stored_evidence_version_uid == mutation.evidence_version_uid
    )
    if not matches:
        raise HTTPException(
            status_code=409,
            detail="operation_id 已用于另一项操作",
        )
    if not isinstance(result, dict):
        raise RuntimeError("Interaction Event 缺少原操作结果")
    return EventUserStateMutationOut.model_validate(result)


def revision_source_ids(session: Session, revision_id: int) -> list[int]:
    return sorted(
        set(
            session.scalars(
                select(EventEvidenceVersion.source_id)
                .join(
                    EventRevisionEvidence,
                    EventRevisionEvidence.evidence_version_id
                    == EventEvidenceVersion.id,
                )
                .where(EventRevisionEvidence.revision_id == revision_id)
            ).all()
        )
    )


def previous_metric_sources(
    session: Session,
    event_id: int,
    action: str,
) -> list[int]:
    previous = session.scalar(
        select(InteractionEvent)
        .where(
            InteractionEvent.event_id == event_id,
            InteractionEvent.action == action,
        )
        .order_by(
            InteractionEvent.recorded_at.desc(),
            InteractionEvent.id.desc(),
        )
        .limit(1)
    )
    if previous is None or not isinstance(previous.payload, dict):
        return []
    source_ids = previous.payload.get("metric_source_ids")
    if not isinstance(source_ids, list):
        return []
    return sorted({source_id for source_id in source_ids if isinstance(source_id, int)})


def previous_read_metric_sources(
    session: Session,
    event_id: int,
) -> tuple[list[int], list[int], list[int]]:
    previous = session.scalar(
        select(InteractionEvent)
        .where(
            InteractionEvent.event_id == event_id,
            InteractionEvent.action == EVENT_READ_STATUS_ACTION,
        )
        .order_by(
            InteractionEvent.recorded_at.desc(),
            InteractionEvent.id.desc(),
        )
        .limit(1)
    )
    read_sources: list[int] = []
    opened_sources: list[int] = []
    if previous is not None:
        payload = previous.payload if isinstance(previous.payload, dict) else {}
        read_sources = integer_source_ids(payload.get("read_metric_source_ids"))
        opened_sources = integer_source_ids(payload.get("opened_metric_source_ids"))
    baseline_revision_id = session.scalar(
        select(MigrationBaseline.resolved_revision_id)
        .where(
            MigrationBaseline.resolved_event_id == event_id,
            MigrationBaseline.read_status.in_(SEEN_READ_STATUSES),
        )
        .limit(1)
    )
    baseline_sources = (
        revision_source_ids(session, baseline_revision_id)
        if baseline_revision_id is not None
        else []
    )
    return read_sources, opened_sources, baseline_sources


def integer_source_ids(value: object) -> list[int]:
    if not isinstance(value, list):
        return []
    return sorted({source_id for source_id in value if isinstance(source_id, int)})


def metric_projection_change(
    *,
    previous_value: bool,
    next_value: bool,
    previous_sources: list[int],
    observed_sources: list[int],
) -> tuple[list[int], dict[int, int]]:
    if previous_value == next_value:
        return previous_sources, {}
    if next_value:
        return observed_sources, {source_id: 1 for source_id in observed_sources}
    return [], {source_id: -1 for source_id in previous_sources}
