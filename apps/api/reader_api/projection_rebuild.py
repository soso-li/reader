from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session

from .feed_metrics import refresh_source_trust_score
from .models import (
    ContentItem,
    EventRevision,
    EventUserState,
    FeedMetric,
    InteractionEvent,
    MigrationBaseline,
    Source,
    UserState,
)
from .reading_state import next_read_status


BEHAVIOR_METRIC_FIELDS = (
    "read_count",
    "opened_count",
    "starred_count",
    "read_later_count",
)
SEEN_READ_STATUSES = {"summary_seen", "original_opened"}
UNINTERESTED_REASONS = {
    "promotion",
    "repetitive",
    "topic",
    "low_quality",
    "other",
}


def inspect_projection_rebuild(
    session: Session,
    *,
    mode: str = "verify",
) -> dict[str, object]:
    if mode not in {"verify", "dry-run"}:
        raise ValueError(f"不支持的投影检查模式：{mode}")
    return _projection_report(session, _expected_projections(session), mode=mode)


def rebuild_projections(session: Session) -> dict[str, object]:
    _lock_projection_publication(session)
    expected = _expected_projections(session)
    before = _projection_report(session, expected, mode="rebuild")
    unsupported = before["unsupported_user_states"]
    if unsupported:
        targets = ", ".join(
            f"{row['object_type']}:{row['object_id']}" for row in unsupported
        )
        raise RuntimeError(
            "投影重建拒绝：UserState 存在无事实支撑的目标"
            "（缺少 MigrationBaseline、InteractionEvent 或可重放 Event 投影）："
            f"{targets}"
        )
    if bool(before["matches"]):
        return {**before, "applied": False, "differences_before": before["differences"]}

    session.execute(delete(EventUserState))
    session.execute(delete(UserState))
    session.flush()

    event_states, object_states, metric_counts = expected
    session.add_all(
        EventUserState(
            baseline_id=state["baseline_id"],
            event_id=event_id,
            seen_revision_id=state["seen_revision_id"],
            read_status=str(state["read_status"]),
            read_later=bool(state["read_later"]),
            starred=bool(state["starred"]),
            uninterested=bool(state["uninterested"]),
            uninterested_reason=state["uninterested_reason"],
            uninterested_note=state["uninterested_note"],
            uninterested_at=state["uninterested_at"],
            updated_at=state["updated_at"],
        )
        for event_id, state in sorted(event_states.items())
    )
    session.add_all(
        UserState(
            object_type=object_type,
            object_id=object_id,
            read_status=str(state["read_status"]),
            read_later=bool(state["read_later"]),
            starred=bool(state["starred"]),
            uninterested=bool(state["uninterested"]),
            uninterested_reason=state["uninterested_reason"],
            uninterested_note=state["uninterested_note"],
            uninterested_at=state["uninterested_at"],
            updated_at=state["updated_at"],
        )
        for (object_type, object_id), state in sorted(object_states.items())
    )

    metrics = {
        metric.source_id: metric
        for metric in session.scalars(select(FeedMetric).with_for_update()).all()
    }
    for source_id in sorted(set(metrics).union(metric_counts)):
        metric = metrics.get(source_id)
        if metric is None:
            metric = FeedMetric(source_id=source_id)
            session.add(metric)
            metrics[source_id] = metric
        counts = metric_counts.get(source_id, {})
        for field in BEHAVIOR_METRIC_FIELDS:
            setattr(metric, field, int(counts.get(field, 0)))
    session.flush()
    for source_id, metric in sorted(metrics.items()):
        source = session.get(Source, source_id)
        if source is None:
            raise RuntimeError(f"FeedMetric 来源不存在：{source_id}")
        refresh_source_trust_score(session, source, metric)
    session.flush()

    after = _projection_report(session, expected, mode="rebuild")
    if not bool(after["matches"]):
        raise RuntimeError(f"投影重建后仍不一致：{after['differences']}")
    return {
        **after,
        "applied": True,
        "differences_before": before["differences"],
    }


def _lock_projection_publication(session: Session) -> None:
    if session.bind is None or session.bind.dialect.name != "postgresql":
        return
    session.execute(text("LOCK TABLE sources IN SHARE MODE"))
    session.execute(
        text(
            "LOCK TABLE feed_metrics, event_user_states, user_states "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    session.execute(
        text(
            "LOCK TABLE migration_baselines, interaction_events "
            "IN SHARE MODE"
        )
    )


def _expected_projections(
    session: Session,
) -> tuple[
    dict[int, dict[str, object]],
    dict[tuple[str, int], dict[str, object]],
    dict[int, dict[str, int]],
]:
    source_ids = set(session.scalars(select(Source.id)).all())
    item_sources = {
        item_id: source_id
        for item_id, source_id in session.execute(
            select(ContentItem.id, ContentItem.source_id)
        ).tuples()
    }
    revisions = {
        revision.id: (revision.event_id, revision.revision_no)
        for revision in session.scalars(select(EventRevision)).all()
    }
    event_states: dict[int, dict[str, object]] = {}
    object_states: dict[tuple[str, int], dict[str, object]] = {}

    for baseline in session.scalars(
        select(MigrationBaseline).order_by(
            MigrationBaseline.recorded_at,
            MigrationBaseline.id,
        )
    ):
        if baseline.legacy_object_type == "cluster":
            if baseline.resolved_event_id is None or baseline.resolved_revision_id is None:
                raise RuntimeError(f"Cluster baseline 缺少 Event：{baseline.id}")
            if baseline.resolved_event_id in event_states:
                raise RuntimeError(f"Event 存在多个 baseline：{baseline.resolved_event_id}")
            resolved_revision = revisions.get(baseline.resolved_revision_id)
            if (
                resolved_revision is None
                or resolved_revision[0] != baseline.resolved_event_id
            ):
                raise RuntimeError(f"Cluster baseline revision 不匹配：{baseline.id}")
            state = _event_state(
                baseline_id=baseline.id,
                seen_revision_id=(
                    baseline.resolved_revision_id
                    if baseline.read_status in SEEN_READ_STATUSES
                    else None
                ),
                read_status=baseline.read_status,
                read_later=baseline.read_later,
                starred=baseline.starred,
                updated_at=baseline.source_updated_at,
            )
            if state["seen_revision_id"] is not None:
                state["seen_revision_no"] = resolved_revision[1]
            event_states[baseline.resolved_event_id] = state
            continue
        key = (baseline.legacy_object_type, baseline.legacy_object_id)
        if key in object_states:
            raise RuntimeError(f"对象目标存在多个 baseline：{key}")
        object_states[key] = _object_state(
            read_status=baseline.read_status,
            read_later=baseline.read_later,
            starred=baseline.starred,
            updated_at=baseline.source_updated_at,
            metric_source_id=(
                item_sources.get(baseline.legacy_object_id)
                if baseline.legacy_object_type == "item"
                else None
            ),
        )

    seen_operations: set[str] = set()
    for interaction in session.scalars(
        select(InteractionEvent).order_by(
            InteractionEvent.recorded_at,
            InteractionEvent.id,
        )
    ):
        if interaction.operation_id in seen_operations:
            continue
        seen_operations.add(interaction.operation_id)
        payload = interaction.payload if isinstance(interaction.payload, dict) else {}
        if interaction.target_kind == "event":
            if interaction.event_id is None or interaction.observed_revision_id is None:
                raise RuntimeError(f"Event interaction target 不完整：{interaction.id}")
            revision = revisions.get(interaction.observed_revision_id)
            if revision is None or revision[0] != interaction.event_id:
                raise RuntimeError(f"Event interaction revision 不匹配：{interaction.id}")
            state = event_states.setdefault(
                interaction.event_id,
                _event_state(updated_at=interaction.occurred_at),
            )
            _apply_event_interaction(
                state,
                interaction,
                revision_no=revision[1],
                payload=payload,
                source_ids=source_ids,
            )
        elif interaction.target_kind == "legacy":
            if (
                interaction.object_type not in {"item", "report", "topic"}
                or interaction.object_id is None
            ):
                raise RuntimeError(f"对象 interaction target 不完整：{interaction.id}")
            key = (interaction.object_type, interaction.object_id)
            state = object_states.setdefault(
                key,
                _object_state(updated_at=interaction.occurred_at),
            )
            _apply_object_interaction(state, interaction, payload, source_ids)
        else:
            raise RuntimeError(f"未知 interaction target_kind：{interaction.target_kind}")

    metric_counts = _metric_counts(event_states, object_states, source_ids)
    return event_states, object_states, metric_counts


def _event_state(
    *,
    baseline_id: int | None = None,
    seen_revision_id: int | None = None,
    read_status: str = "unread",
    read_later: bool = False,
    starred: bool = False,
    uninterested: bool = False,
    uninterested_reason: str | None = None,
    uninterested_note: str | None = None,
    uninterested_at: datetime | None = None,
    updated_at: datetime,
) -> dict[str, object]:
    return {
        "baseline_id": baseline_id,
        "seen_revision_id": seen_revision_id,
        "read_status": read_status,
        "read_later": read_later,
        "starred": starred,
        "uninterested": uninterested,
        "uninterested_reason": uninterested_reason,
        "uninterested_note": uninterested_note,
        "uninterested_at": uninterested_at,
        "updated_at": updated_at,
        "seen_revision_no": None,
        "starred_sources": [],
        "read_later_sources": [],
        "read_sources": [],
        "opened_sources": [],
    }


def _object_state(
    *,
    read_status: str = "unread",
    read_later: bool = False,
    starred: bool = False,
    uninterested: bool = False,
    uninterested_reason: str | None = None,
    uninterested_note: str | None = None,
    uninterested_at: datetime | None = None,
    updated_at: datetime,
    metric_source_id: int | None = None,
) -> dict[str, object]:
    return {
        "read_status": read_status,
        "read_later": read_later,
        "starred": starred,
        "uninterested": uninterested,
        "uninterested_reason": uninterested_reason,
        "uninterested_note": uninterested_note,
        "uninterested_at": uninterested_at,
        "updated_at": updated_at,
        "metric_source_id": metric_source_id,
    }


def _apply_event_interaction(
    state: dict[str, object],
    interaction: InteractionEvent,
    *,
    revision_no: int,
    payload: dict[str, object],
    source_ids: set[int],
) -> None:
    if interaction.action in {"starred_set", "read_later_set"}:
        if not isinstance(interaction.set_value, bool):
            raise RuntimeError(f"Event set value 非布尔值：{interaction.id}")
        field = "starred" if interaction.action == "starred_set" else "read_later"
        sources = _payload_source_ids(payload, "metric_source_ids", interaction.id)
        _validate_sources(sources, source_ids, interaction.id)
        if not interaction.set_value and sources:
            raise RuntimeError(f"Event false set 仍含指标来源：{interaction.id}")
        state[field] = interaction.set_value
        state[f"{field}_sources"] = sources
    elif interaction.action == "read_status_set":
        if interaction.set_value not in {"unread", "summary_seen", "original_opened"}:
            raise RuntimeError(f"Event read status 非法：{interaction.id}")
        read_sources = _payload_source_ids(
            payload, "read_metric_source_ids", interaction.id
        )
        opened_sources = _payload_source_ids(
            payload, "opened_metric_source_ids", interaction.id
        )
        _validate_sources(read_sources + opened_sources, source_ids, interaction.id)
        if interaction.set_value == "unread" and (read_sources or opened_sources):
            raise RuntimeError(f"Event unread 仍含指标来源：{interaction.id}")
        state["read_status"] = next_read_status(
            str(state["read_status"]), str(interaction.set_value)
        )
        state["read_sources"] = read_sources
        state["opened_sources"] = opened_sources
        if interaction.set_value in SEEN_READ_STATUSES:
            previous_no = state["seen_revision_no"]
            if previous_no is None or revision_no > int(previous_no):
                state["seen_revision_id"] = interaction.observed_revision_id
                state["seen_revision_no"] = revision_no
    elif interaction.action == "uninterested_set":
        _apply_uninterested_interaction(state, interaction, payload)
    else:
        raise RuntimeError(f"未知 Event interaction action：{interaction.action}")
    state["updated_at"] = interaction.occurred_at


def _apply_object_interaction(
    state: dict[str, object],
    interaction: InteractionEvent,
    payload: dict[str, object],
    source_ids: set[int],
) -> None:
    if interaction.action == "read_status_set":
        if not isinstance(interaction.set_value, str):
            raise RuntimeError(f"对象 read status 非字符串：{interaction.id}")
        state["read_status"] = next_read_status(
            str(state["read_status"]), interaction.set_value
        )
    elif interaction.action in {"starred_set", "read_later_set"}:
        if not isinstance(interaction.set_value, bool):
            raise RuntimeError(f"对象 set value 非布尔值：{interaction.id}")
        field = "starred" if interaction.action == "starred_set" else "read_later"
        state[field] = interaction.set_value
    elif interaction.action == "uninterested_set":
        _apply_uninterested_interaction(state, interaction, payload)
    else:
        raise RuntimeError(f"未知对象 interaction action：{interaction.action}")
    metric_source_id = payload.get("metric_source_id")
    if metric_source_id is not None:
        if not isinstance(metric_source_id, int) or metric_source_id not in source_ids:
            raise RuntimeError(f"对象 interaction 指标来源非法：{interaction.id}")
        state["metric_source_id"] = metric_source_id
    state["updated_at"] = interaction.occurred_at


def _apply_uninterested_interaction(
    state: dict[str, object],
    interaction: InteractionEvent,
    payload: dict[str, object],
) -> None:
    if not isinstance(interaction.set_value, bool):
        raise RuntimeError(f"不感兴趣 set value 非布尔值：{interaction.id}")
    request = payload.get("request")
    if not isinstance(request, dict):
        raise RuntimeError(f"不感兴趣 interaction 缺少请求：{interaction.id}")
    reason = request.get("reason")
    note = request.get("note")
    if reason is not None and reason not in UNINTERESTED_REASONS:
        raise RuntimeError(f"不感兴趣原因非法：{interaction.id}")
    if reason == "other" and (not isinstance(note, str) or not note.strip()):
        raise RuntimeError(f"不感兴趣其他原因缺少说明：{interaction.id}")
    if reason != "other" and note is not None:
        raise RuntimeError(f"不感兴趣原因包含多余说明：{interaction.id}")
    was_uninterested = bool(state["uninterested"])
    state["uninterested"] = interaction.set_value
    if interaction.set_value:
        state["uninterested_reason"] = reason
        state["uninterested_note"] = note
        if not was_uninterested or state["uninterested_at"] is None:
            state["uninterested_at"] = interaction.occurred_at
    else:
        state["uninterested_reason"] = None
        state["uninterested_note"] = None
        state["uninterested_at"] = None


def _payload_source_ids(
    payload: dict[str, object],
    field: str,
    interaction_id: str,
) -> list[int]:
    value = payload.get(field)
    if not isinstance(value, list) or any(not isinstance(item, int) for item in value):
        raise RuntimeError(f"Interaction payload 缺少 {field}：{interaction_id}")
    return sorted(set(value))


def _validate_sources(
    values: list[int],
    source_ids: set[int],
    interaction_id: str,
) -> None:
    missing = sorted(set(values).difference(source_ids))
    if missing:
        raise RuntimeError(f"Interaction 指标来源不存在：{interaction_id}, {missing}")


def _metric_counts(
    event_states: dict[int, dict[str, object]],
    object_states: dict[tuple[str, int], dict[str, object]],
    source_ids: set[int],
) -> dict[int, dict[str, int]]:
    counts: dict[int, dict[str, int]] = {}

    def increment(source_id: int, field: str) -> None:
        if source_id not in source_ids:
            raise RuntimeError(f"投影指标来源不存在：{source_id}")
        row = counts.setdefault(source_id, {})
        row[field] = row.get(field, 0) + 1

    for state in event_states.values():
        for source_id in state["read_sources"]:
            increment(int(source_id), "read_count")
        for source_id in state["opened_sources"]:
            increment(int(source_id), "opened_count")
        for source_id in state["starred_sources"]:
            increment(int(source_id), "starred_count")
        for source_id in state["read_later_sources"]:
            increment(int(source_id), "read_later_count")

    for (object_type, _object_id), state in object_states.items():
        if object_type != "item":
            continue
        source_id = state["metric_source_id"]
        contributes = (
            state["read_status"] in SEEN_READ_STATUSES
            or bool(state["starred"])
            or bool(state["read_later"])
        )
        if source_id is None:
            if contributes:
                raise RuntimeError("item UserState 无法解析指标来源")
            continue
        if state["read_status"] in SEEN_READ_STATUSES:
            increment(int(source_id), "read_count")
        if state["read_status"] == "original_opened":
            increment(int(source_id), "opened_count")
        if bool(state["starred"]):
            increment(int(source_id), "starred_count")
        if bool(state["read_later"]):
            increment(int(source_id), "read_later_count")
    return counts


def _projection_report(
    session: Session,
    expected: tuple[
        dict[int, dict[str, object]],
        dict[tuple[str, int], dict[str, object]],
        dict[int, dict[str, int]],
    ],
    *,
    mode: str,
) -> dict[str, object]:
    event_expected, object_expected, metric_expected = expected
    event_actual = {
        state.event_id: (
            state.baseline_id,
            state.seen_revision_id,
            state.read_status,
            state.read_later,
            state.starred,
            state.uninterested,
            state.uninterested_reason,
            state.uninterested_note,
            state.uninterested_at,
            state.updated_at,
        )
        for state in session.scalars(select(EventUserState)).all()
    }
    event_rows = {
        event_id: (
            state["baseline_id"],
            state["seen_revision_id"],
            state["read_status"],
            state["read_later"],
            state["starred"],
            state["uninterested"],
            state["uninterested_reason"],
            state["uninterested_note"],
            state["uninterested_at"],
            state["updated_at"],
        )
        for event_id, state in event_expected.items()
    }
    object_actual = {
        (state.object_type, state.object_id): (
            state.read_status,
            state.read_later,
            state.starred,
            state.uninterested,
            state.uninterested_reason,
            state.uninterested_note,
            state.uninterested_at,
            state.updated_at,
        )
        for state in session.scalars(select(UserState)).all()
    }
    object_rows = {
        key: (
            state["read_status"],
            state["read_later"],
            state["starred"],
            state["uninterested"],
            state["uninterested_reason"],
            state["uninterested_note"],
            state["uninterested_at"],
            state["updated_at"],
        )
        for key, state in object_expected.items()
    }
    supported_user_state_keys = set(object_rows)
    supported_user_state_keys.update(
        (object_type, object_id)
        for object_type, object_id in session.execute(
            select(
                MigrationBaseline.legacy_object_type,
                MigrationBaseline.legacy_object_id,
            )
        )
    )
    unsupported_user_state_keys = sorted(
        set(object_actual) - supported_user_state_keys
    )
    metric_actual = {
        metric.source_id: tuple(
            int(getattr(metric, field) or 0) for field in BEHAVIOR_METRIC_FIELDS
        )
        for metric in session.scalars(select(FeedMetric)).all()
    }
    metric_rows = {
        source_id: tuple(counts.get(field, 0) for field in BEHAVIOR_METRIC_FIELDS)
        for source_id, counts in metric_expected.items()
    }
    metric_rows.update(
        {
            source_id: (0, 0, 0, 0)
            for source_id in metric_actual
            if source_id not in metric_rows
        }
    )
    differences = {
        "event_user_states": _difference(event_rows, event_actual),
        "user_states": _difference(object_rows, object_actual),
        "feed_metrics": _difference(metric_rows, metric_actual),
    }
    matches = all(
        not any(int(value) for value in difference.values())
        for difference in differences.values()
    )
    return {
        "mode": mode,
        "matches": matches,
        "safe_to_apply": not unsupported_user_state_keys,
        "unsupported_user_states": [
            {"object_type": object_type, "object_id": object_id}
            for object_type, object_id in unsupported_user_state_keys
        ],
        "differences": differences,
        "expected_counts": {
            "event_user_states": len(event_rows),
            "user_states": len(object_rows),
            "feed_metrics": len(metric_rows),
        },
        "actual_counts": {
            "event_user_states": len(event_actual),
            "user_states": len(object_actual),
            "feed_metrics": len(metric_actual),
        },
        "published_count": len(event_rows) + len(object_rows) + len(metric_rows),
    }


def _difference(
    expected: dict[object, tuple[object, ...]],
    actual: dict[object, tuple[object, ...]],
) -> dict[str, int]:
    expected_keys = set(expected)
    actual_keys = set(actual)
    return {
        "missing": len(expected_keys - actual_keys),
        "unexpected": len(actual_keys - expected_keys),
        "changed": sum(
            expected[key] != actual[key] for key in expected_keys & actual_keys
        ),
    }
