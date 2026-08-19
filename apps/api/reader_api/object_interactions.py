from __future__ import annotations

import hashlib

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .event_interactions import lock_operation_id
from .feed_metrics import locked_feed_metric, project_item_user_state_metrics
from .models import (
    ContentItem,
    DELETED_SOURCE_STATUS,
    FeedMetric,
    InteractionEvent,
    Source,
    TopicGroup,
    UserState,
    now_utc,
)
from .reading_state import next_read_status
from .schemas import UserStateOut, UserStatePatch


OBJECT_ACTION_FIELDS = {
    "read_status": "read_status_set",
    "read_later": "read_later_set",
    "starred": "starred_set",
}
OBJECT_STATE_TYPES = {"item", "report", "topic"}
STORED_OBJECT_TARGET_KIND = "legacy"


def apply_object_user_state_mutation(
    session: Session,
    object_type: str,
    object_id: int,
    mutation: UserStatePatch,
) -> UserStateOut:
    if object_type not in OBJECT_STATE_TYPES:
        raise HTTPException(status_code=400, detail="不支持的对象状态类型")
    if mutation.operation_id is None:
        raise HTTPException(status_code=400, detail="明确状态操作必须提交 operation_id")

    requested = [
        (field, getattr(mutation, field))
        for field in OBJECT_ACTION_FIELDS
        if getattr(mutation, field) is not None
    ]
    if len(requested) != 1:
        raise HTTPException(status_code=400, detail="每次状态操作必须只提交一个明确 set 值")
    field, value = requested[0]
    action = OBJECT_ACTION_FIELDS[field]

    lock_operation_id(session, mutation.operation_id)
    existing = session.scalar(
        select(InteractionEvent).where(
            InteractionEvent.operation_id == mutation.operation_id
        )
    )
    if existing is not None:
        return original_object_operation_result(
            existing,
            object_type=object_type,
            object_id=object_id,
            action=action,
            value=value,
        )

    source_id = lock_object_target(session, object_type, object_id)
    locked_metric = lock_object_metric(session, source_id)
    item = lock_item_after_source(session, object_id, source_id)
    state = session.scalar(
        select(UserState)
        .where(
            UserState.object_type == object_type,
            UserState.object_id == object_id,
        )
        .with_for_update()
    )
    if state is None:
        state = UserState(object_type=object_type, object_id=object_id)
        session.add(state)
        session.flush()

    previous = (state.read_status, state.read_later, state.starred)
    if field == "read_status":
        state.read_status = next_read_status(state.read_status, str(value))
    else:
        setattr(state, field, bool(value))
    occurred_at = now_utc()
    state.updated_at = occurred_at

    metric_delta: dict[str, int] = {}
    if object_type == "item":
        if item is None or locked_metric is None:
            raise HTTPException(status_code=404, detail="条目不存在")
        metric_delta = project_item_user_state_metrics(
            session,
            item.source_id,
            state,
            previous,
            locked=locked_metric,
        )

    result = UserStateOut.model_validate(state)
    result_payload = result.model_dump(mode="json")
    interaction = InteractionEvent(
        operation_id=mutation.operation_id,
        target_kind=STORED_OBJECT_TARGET_KIND,
        object_type=object_type,
        object_id=object_id,
        action=action,
        set_value=value,
        payload={
            "previous": {
                "read_status": previous[0],
                "read_later": previous[1],
                "starred": previous[2],
            },
            "metric_source_id": item.source_id if item is not None else None,
            "metric_delta": metric_delta,
            "result": result_payload,
        },
        occurred_at=occurred_at,
        recorded_at=now_utc(),
    )
    session.add(interaction)
    session.flush()
    return result


def lock_object_target(
    session: Session,
    object_type: str,
    object_id: int,
) -> int | None:
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        digest = hashlib.sha256(
            f"object-target:{object_type}:{object_id}".encode("utf-8")
        ).digest()
        lock_key = int.from_bytes(digest[:8], byteorder="big", signed=True)
        session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": lock_key},
        )

    if object_type == "report":
        return None
    if object_type == "item":
        source_id = session.scalar(
            select(ContentItem.source_id).where(ContentItem.id == object_id)
        )
        if source_id is None:
            raise HTTPException(status_code=404, detail="条目不存在")
        return source_id
    model = TopicGroup
    target = session.scalar(
        select(model).where(model.id == object_id).with_for_update()
    )
    if target is None:
        raise HTTPException(status_code=404, detail="议题组不存在")
    return None


def lock_object_metric(
    session: Session,
    source_id: int | None,
) -> tuple[Source, FeedMetric] | None:
    if source_id is None:
        return None
    try:
        locked = locked_feed_metric(session, source_id)
        if locked[0].status == DELETED_SOURCE_STATUS:
            raise HTTPException(status_code=404, detail="条目不存在")
        return locked
    except RuntimeError as exc:
        raise HTTPException(status_code=404, detail="条目来源不存在") from exc


def lock_item_after_source(
    session: Session,
    object_id: int,
    source_id: int | None,
) -> ContentItem | None:
    if source_id is None:
        return None
    item = session.scalar(
        select(ContentItem)
        .where(ContentItem.id == object_id)
        .with_for_update()
    )
    if item is None or item.source_id != source_id:
        raise HTTPException(status_code=404, detail="条目不存在")
    return item


def original_object_operation_result(
    existing: InteractionEvent,
    *,
    object_type: str,
    object_id: int,
    action: str,
    value: object,
) -> UserStateOut:
    matches = (
        existing.target_kind == STORED_OBJECT_TARGET_KIND
        and existing.object_type == object_type
        and existing.object_id == object_id
        and existing.action == action
        and existing.set_value == value
    )
    if not matches:
        raise HTTPException(status_code=409, detail="operation_id 已用于另一项操作")
    payload = existing.payload if isinstance(existing.payload, dict) else {}
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Interaction Event 缺少原操作结果")
    return UserStateOut.model_validate(result)
