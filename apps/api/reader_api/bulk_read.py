from __future__ import annotations

from collections import defaultdict
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError
from redis import Redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .event_interactions import (
    EVENT_READ_STATUS_ACTION,
    SEEN_READ_STATUSES,
    apply_metric_deltas,
    integer_source_ids,
    lock_feed_metrics,
    lock_operation_id,
    require_live_event_sources,
)
from .event_projection import event_material_updates_for
from .models import (
    ContentItem,
    DELETED_SOURCE_STATUS,
    Event,
    EventEvidenceVersion,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    InteractionEvent,
    MigrationBaseline,
    UserState,
    now_utc,
)
from .reading_state import next_read_status
from .schemas import (
    BulkReadManifest,
    BulkReadPrepared,
    BulkReadTarget,
    EventUserStateMutationOut,
    UserStateOut,
)


BULK_READ_BATCH_TTL_SECONDS = 30 * 60
BULK_READ_BATCH_KEY_PREFIX = "reader:bulk-read:"


def prepare_bulk_read_manifest(
    session: Session,
    *,
    object_type: str,
    object_ids: list[int],
) -> BulkReadManifest:
    frozen_ids = sorted(set(object_ids))
    if object_type == "item":
        return BulkReadManifest(
            targets=[
                BulkReadTarget(
                    target_kind="object",
                    object_type="item",
                    object_id=object_id,
                    operation_id=str(uuid4()),
                )
                for object_id in frozen_ids
            ]
        )
    if object_type != "event":
        raise HTTPException(status_code=400, detail="批量标记只支持 item 或 Event")

    event_rows = session.execute(
        select(Event.id, Event.uid, EventRevision.uid)
        .join(EventRevision, EventRevision.id == Event.current_revision_id)
        .where(Event.id.in_(frozen_ids), Event.status == "active")
    ).all()
    identities = {
        event_id: (event_uid, revision_uid)
        for event_id, event_uid, revision_uid in event_rows
    }
    if any(event_id not in identities for event_id in frozen_ids):
        raise HTTPException(
            status_code=409,
            detail="批量目标包含失效 Event，请刷新后重试",
        )
    return BulkReadManifest(
        targets=[
            BulkReadTarget(
                target_kind="event",
                event_uid=identities[event_id][0],
                observed_revision_uid=identities[event_id][1],
                operation_id=str(uuid4()),
            )
            for event_id in frozen_ids
        ]
    )


def store_bulk_read_manifest(
    connection: Redis,
    manifest: BulkReadManifest,
) -> BulkReadPrepared:
    if not manifest.targets:
        return BulkReadPrepared(batch_id=None, target_count=0)
    batch_id = str(uuid4())
    stored = connection.set(
        bulk_read_batch_key(batch_id),
        manifest.model_dump_json(exclude_none=True),
        ex=BULK_READ_BATCH_TTL_SECONDS,
        nx=True,
    )
    if not stored:
        raise RuntimeError("无法保存批量已读批次")
    return BulkReadPrepared(batch_id=batch_id, target_count=len(manifest.targets))


def confirm_bulk_read_batch(
    session: Session,
    connection: Redis,
    batch_id: str,
) -> int:
    lock_operation_id(session, f"bulk-read-batch:{batch_id}")
    manifest = load_bulk_read_manifest(connection, batch_id)
    connection.expire(
        bulk_read_batch_key(batch_id),
        BULK_READ_BATCH_TTL_SECONDS,
    )
    return confirm_bulk_read_manifest(session, manifest)


def load_bulk_read_manifest(
    connection: Redis,
    batch_id: str,
) -> BulkReadManifest:
    raw = connection.get(bulk_read_batch_key(batch_id))
    if raw is None:
        raise HTTPException(
            status_code=409,
            detail="批量已读确认已过期，请重新准备",
        )
    try:
        return BulkReadManifest.model_validate_json(raw)
    except (TypeError, ValueError, ValidationError) as exc:
        raise RuntimeError("批量已读批次内容损坏") from exc


def bulk_read_batch_key(batch_id: str) -> str:
    return f"{BULK_READ_BATCH_KEY_PREFIX}{batch_id}"


def confirm_bulk_read_manifest(
    session: Session,
    manifest: BulkReadManifest,
) -> int:
    if not manifest.targets:
        return 0
    existing_result = existing_bulk_read_result(session, manifest)
    if existing_result is not None:
        return existing_result
    target_kinds = {target.target_kind for target in manifest.targets}
    if target_kinds == {"event"}:
        return apply_bulk_event_read(session, manifest.targets)
    if target_kinds == {"object"}:
        return apply_bulk_item_read(session, manifest.targets)
    raise HTTPException(status_code=409, detail="批量已读批次目标类型不一致")


def existing_bulk_read_result(
    session: Session,
    manifest: BulkReadManifest,
) -> int | None:
    targets_by_operation = {
        target.operation_id: target for target in manifest.targets
    }
    existing = list(
        session.scalars(
            select(InteractionEvent).where(
                InteractionEvent.operation_id.in_(targets_by_operation)
            )
        ).all()
    )
    if not existing:
        return None
    if len(existing) != len(manifest.targets):
        raise HTTPException(
            status_code=409,
            detail="批量已读批次只有部分操作已存在",
        )

    event_ids = {
        interaction.event_id
        for interaction in existing
        if interaction.event_id is not None
    }
    revision_ids = {
        interaction.observed_revision_id
        for interaction in existing
        if interaction.observed_revision_id is not None
    }
    event_uids = dict(
        session.execute(
            select(Event.id, Event.uid).where(Event.id.in_(event_ids))
        ).all()
    )
    revision_uids = dict(
        session.execute(
            select(EventRevision.id, EventRevision.uid).where(
                EventRevision.id.in_(revision_ids)
            )
        ).all()
    )
    for interaction in existing:
        target = targets_by_operation[interaction.operation_id]
        payload = interaction.payload if isinstance(interaction.payload, dict) else {}
        if not isinstance(payload.get("result"), dict):
            raise RuntimeError("Interaction Event 缺少原操作结果")
        if target.target_kind == "event":
            matches = (
                interaction.target_kind == "event"
                and event_uids.get(interaction.event_id) == target.event_uid
                and revision_uids.get(interaction.observed_revision_id)
                == target.observed_revision_uid
                and interaction.action == EVENT_READ_STATUS_ACTION
                and interaction.set_value == "summary_seen"
            )
        else:
            matches = (
                interaction.target_kind == "legacy"
                and interaction.object_type == "item"
                and interaction.object_id == target.object_id
                and interaction.action == EVENT_READ_STATUS_ACTION
                and interaction.set_value == "summary_seen"
            )
        if not matches:
            raise HTTPException(
                status_code=409,
                detail="operation_id 已用于另一项操作",
            )
    return len(manifest.targets)


def apply_bulk_event_read(
    session: Session,
    targets: list[BulkReadTarget],
) -> int:
    event_uids = [target.event_uid for target in targets]
    revision_uids = [target.observed_revision_uid for target in targets]
    events = {
        event.uid: event
        for event in session.scalars(
            select(Event)
            .where(Event.uid.in_(event_uids))
            .order_by(Event.id)
            .with_for_update()
        ).all()
    }
    revisions = {
        revision.uid: revision
        for revision in session.scalars(
            select(EventRevision).where(EventRevision.uid.in_(revision_uids))
        ).all()
    }

    resolved: list[tuple[BulkReadTarget, Event, EventRevision]] = []
    for target in targets:
        assert target.event_uid is not None
        assert target.observed_revision_uid is not None
        event = events.get(target.event_uid)
        if event is None:
            raise HTTPException(status_code=404, detail="Event 不存在")
        if event.status != "active":
            raise HTTPException(status_code=409, detail="Event 已被后继事件取代，请刷新")
        revision = revisions.get(target.observed_revision_uid)
        if revision is None:
            raise HTTPException(status_code=404, detail="Event Revision 不存在")
        if revision.event_id != event.id:
            raise HTTPException(
                status_code=409,
                detail="observed revision 不属于目标 Event",
            )
        resolved.append((target, event, revision))

    event_ids = [event.id for _, event, _ in resolved]
    revision_ids = [revision.id for _, _, revision in resolved]
    observed_sources: dict[int, set[int]] = defaultdict(set)
    for revision_id, source_id in session.execute(
        select(
            EventRevisionEvidence.revision_id,
            EventEvidenceVersion.source_id,
        )
        .select_from(EventRevisionEvidence)
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id
            == EventRevisionEvidence.evidence_version_id,
        )
        .where(EventRevisionEvidence.revision_id.in_(revision_ids))
    ):
        observed_sources[revision_id].add(source_id)

    ranked_interactions = (
        select(
            InteractionEvent.event_id.label("event_id"),
            InteractionEvent.payload.label("payload"),
            func.row_number()
            .over(
                partition_by=InteractionEvent.event_id,
                order_by=(
                    InteractionEvent.recorded_at.desc(),
                    InteractionEvent.id.desc(),
                ),
            )
            .label("row_no"),
        )
        .where(
            InteractionEvent.event_id.in_(event_ids),
            InteractionEvent.action == EVENT_READ_STATUS_ACTION,
        )
        .subquery()
    )
    previous_payloads = dict(
        session.execute(
            select(
                ranked_interactions.c.event_id,
                ranked_interactions.c.payload,
            ).where(ranked_interactions.c.row_no == 1)
        ).all()
    )

    baseline_sources: dict[int, set[int]] = defaultdict(set)
    for event_id, source_id in session.execute(
        select(
            MigrationBaseline.resolved_event_id,
            EventEvidenceVersion.source_id,
        )
        .select_from(MigrationBaseline)
        .join(
            EventRevisionEvidence,
            EventRevisionEvidence.revision_id
            == MigrationBaseline.resolved_revision_id,
        )
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id
            == EventRevisionEvidence.evidence_version_id,
        )
        .where(
            MigrationBaseline.resolved_event_id.in_(event_ids),
            MigrationBaseline.read_status.in_(SEEN_READ_STATUSES),
        )
    ):
        assert event_id is not None
        baseline_sources[event_id].add(source_id)

    source_ids: set[int] = set()
    previous_sources: dict[int, tuple[list[int], list[int]]] = {}
    for _, event, revision in resolved:
        payload = previous_payloads.get(event.id)
        payload = payload if isinstance(payload, dict) else {}
        read_sources = integer_source_ids(payload.get("read_metric_source_ids"))
        opened_sources = integer_source_ids(
            payload.get("opened_metric_source_ids")
        )
        previous_sources[event.id] = (read_sources, opened_sources)
        source_ids.update(observed_sources[revision.id])
        source_ids.update(read_sources)
        source_ids.update(opened_sources)
    locked_metrics = lock_feed_metrics(session, list(source_ids))
    require_live_event_sources(
        locked_metrics,
        {
            source_id
            for revision_source_ids in observed_sources.values()
            for source_id in revision_source_ids
        },
    )

    states = {
        state.event_id: state
        for state in session.scalars(
            select(EventUserState)
            .where(EventUserState.event_id.in_(event_ids))
            .order_by(EventUserState.event_id)
            .with_for_update()
        ).all()
    }
    seen_revision_ids = {
        state.seen_revision_id
        for state in states.values()
        if state.seen_revision_id is not None
    }
    revisions_by_id = {
        revision.id: revision
        for revision in revisions.values()
    }
    revisions_by_id.update(
        {
            revision.id: revision
            for revision in session.scalars(
                select(EventRevision).where(
                    EventRevision.id.in_(seen_revision_ids)
                )
            ).all()
        }
    )

    occurred_at = now_utc()
    metric_deltas: dict[int, dict[str, int]] = {}
    prepared: list[
        tuple[
            BulkReadTarget,
            Event,
            EventRevision,
            EventUserState,
            list[int],
            list[int],
            list[int],
        ]
    ] = []
    for target, event, revision in resolved:
        state = states.get(event.id)
        if state is None:
            state = EventUserState(
                event_id=event.id,
                seen_revision_id=None,
                read_status="unread",
                read_later=False,
                starred=False,
            )
            session.add(state)
            states[event.id] = state
        read_sources, opened_sources = previous_sources[event.id]
        known_sources = set(read_sources).union(baseline_sources[event.id])
        newly_read_sources = sorted(
            observed_sources[revision.id].difference(known_sources)
        )
        next_read_sources = sorted(set(read_sources).union(newly_read_sources))
        for source_id in newly_read_sources:
            source_deltas = metric_deltas.setdefault(source_id, {})
            source_deltas["read_count"] = source_deltas.get("read_count", 0) + 1

        state.read_status = next_read_status(state.read_status, "summary_seen")
        seen_revision = revisions_by_id.get(state.seen_revision_id)
        if seen_revision is None or revision.revision_no > seen_revision.revision_no:
            state.seen_revision_id = revision.id
        state.updated_at = occurred_at
        prepared.append(
            (
                target,
                event,
                revision,
                state,
                next_read_sources,
                opened_sources,
                newly_read_sources,
            )
        )

    apply_metric_deltas(session, metric_deltas, locked_metrics)
    session.flush()
    material_updates = event_material_updates_for(session, event_ids)
    interactions: list[InteractionEvent] = []
    for (
        target,
        event,
        revision,
        state,
        next_read_sources,
        opened_sources,
        newly_read_sources,
    ) in prepared:
        seen_revision = revisions_by_id.get(state.seen_revision_id)
        material_update_uid = material_updates.get(event.id)
        result = EventUserStateMutationOut(
            operation_id=target.operation_id,
            event_uid=event.uid,
            observed_revision_uid=revision.uid,
            action=EVENT_READ_STATUS_ACTION,
            value="summary_seen",
            read_later=state.read_later,
            starred=state.starred,
            updated_at=occurred_at,
            read_status=state.read_status,
            seen_revision_uid=seen_revision.uid if seen_revision else None,
            current_revision_differs_from_seen=(
                event.current_revision_id != state.seen_revision_id
            ),
            has_material_update=material_update_uid is not None,
            material_update_revision_uid=material_update_uid,
        )
        interactions.append(
            InteractionEvent(
                operation_id=target.operation_id,
                target_kind="event",
                event_id=event.id,
                observed_revision_id=revision.id,
                action=EVENT_READ_STATUS_ACTION,
                set_value="summary_seen",
                payload={
                    "read_metric_source_ids": next_read_sources,
                    "opened_metric_source_ids": opened_sources,
                    "metric_delta": {
                        str(source_id): {"read_count": 1}
                        for source_id in newly_read_sources
                    },
                    "result": result.model_dump(
                        mode="json",
                        exclude_unset=True,
                    ),
                },
                occurred_at=occurred_at,
                recorded_at=occurred_at,
            )
        )
    session.add_all(interactions)
    session.flush()
    return len(targets)


def apply_bulk_item_read(
    session: Session,
    targets: list[BulkReadTarget],
) -> int:
    object_ids = [target.object_id for target in targets]
    item_sources = dict(
        session.execute(
            select(ContentItem.id, ContentItem.source_id).where(
                ContentItem.id.in_(object_ids)
            )
        ).all()
    )
    if len(item_sources) != len(targets):
        raise HTTPException(status_code=404, detail="条目不存在")

    locked_metrics = lock_feed_metrics(session, list(item_sources.values()))
    if any(source.status == DELETED_SOURCE_STATUS for source, _ in locked_metrics.values()):
        raise HTTPException(status_code=404, detail="条目不存在")
    locked_items = {
        item.id: item
        for item in session.scalars(
            select(ContentItem)
            .where(ContentItem.id.in_(object_ids))
            .order_by(ContentItem.id)
            .with_for_update()
        ).all()
    }
    if len(locked_items) != len(targets):
        raise HTTPException(status_code=404, detail="条目不存在")

    states = {
        state.object_id: state
        for state in session.scalars(
            select(UserState)
            .where(
                UserState.object_type == "item",
                UserState.object_id.in_(object_ids),
            )
            .order_by(UserState.object_id)
            .with_for_update()
        ).all()
    }
    occurred_at = now_utc()
    metric_deltas: dict[int, dict[str, int]] = {}
    interactions: list[InteractionEvent] = []
    for target in targets:
        assert target.object_id is not None
        item = locked_items[target.object_id]
        state = states.get(item.id)
        if state is None:
            state = UserState(
                object_type="item",
                object_id=item.id,
                read_status="unread",
                read_later=False,
                starred=False,
            )
            session.add(state)
            states[item.id] = state
        previous = (state.read_status, state.read_later, state.starred)
        state.read_status = next_read_status(state.read_status, "summary_seen")
        state.updated_at = occurred_at
        changed_to_read = (
            previous[0] not in SEEN_READ_STATUSES
            and state.read_status in SEEN_READ_STATUSES
        )
        metric_delta = {"read_count": 1} if changed_to_read else {}
        if changed_to_read:
            source_deltas = metric_deltas.setdefault(item.source_id, {})
            source_deltas["read_count"] = source_deltas.get("read_count", 0) + 1
        result = UserStateOut.model_validate(state)
        interactions.append(
            InteractionEvent(
                operation_id=target.operation_id,
                target_kind="legacy",
                object_type="item",
                object_id=item.id,
                action=EVENT_READ_STATUS_ACTION,
                set_value="summary_seen",
                payload={
                    "previous": {
                        "read_status": previous[0],
                        "read_later": previous[1],
                        "starred": previous[2],
                    },
                    "metric_source_id": item.source_id,
                    "metric_delta": metric_delta,
                    "result": result.model_dump(mode="json"),
                },
                occurred_at=occurred_at,
                recorded_at=occurred_at,
            )
        )

    apply_metric_deltas(session, metric_deltas, locked_metrics)
    session.add_all(interactions)
    session.flush()
    return len(targets)
