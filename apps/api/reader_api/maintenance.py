from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.orm import Session, sessionmaker

from .db import SessionLocal, prepare_runtime_database
from .cluster import repair_exact_content_duplicates
from .models import (
    GenerationAttemptRunnerAudit,
    GenerationRequestPayload,
    MaintenanceRun,
    SourceEntryRelation,
    now_utc,
)
from .projection_rebuild import inspect_projection_rebuild, rebuild_projections
from .source_entry_relations import (
    list_duplicate_feed_relations,
    record_duplicate_feed_relations,
    revoke_duplicate_feed_relation,
)


logger = logging.getLogger(__name__)

OVER_SPLIT_DOCUMENTS = "over-split-documents"
TITLE_ONLY_CLUSTERS = "title-only-clusters"
WINDOWED_CLUSTERS = "windowed-clusters"
EMBEDDING_CLUSTERS = "embedding-clusters"
DUPLICATE_FEED_ENTRIES = "duplicate-feed-entries"
EXACT_CONTENT_DUPLICATES = "exact-content-duplicates"
LIST_DUPLICATE_FEED_RELATIONS = "list-duplicate-feed-relations"
REVOKE_DUPLICATE_FEED_RELATION = "revoke-duplicate-feed-relation"
REBUILD_PROJECTIONS = "rebuild-projections"
GENERATION_RETENTION = "generation-retention"
GENERATION_RETENTION_DAYS = 30
GENERATION_RETENTION_BATCH_SIZE = 100
GENERATION_RETENTION_INTERVAL = timedelta(days=1)

USER_STATE_REPAIR_DISABLED_MESSAGE = (
    "历史派生修复已禁用；当前实现会在拆分、合并或重建派生数据时删除、合并或扩散 User State，"
    "请等待状态无损版本。"
)
DISABLED_OPERATION_MESSAGES = {
    OVER_SPLIT_DOCUMENTS: USER_STATE_REPAIR_DISABLED_MESSAGE,
    TITLE_ONLY_CLUSTERS: USER_STATE_REPAIR_DISABLED_MESSAGE,
    WINDOWED_CLUSTERS: USER_STATE_REPAIR_DISABLED_MESSAGE,
    EMBEDDING_CLUSTERS: USER_STATE_REPAIR_DISABLED_MESSAGE,
}
DISABLED_OPERATIONS = tuple(DISABLED_OPERATION_MESSAGES)
EXPLICIT_OPERATIONS = (
    *DISABLED_OPERATIONS,
    DUPLICATE_FEED_ENTRIES,
    EXACT_CONTENT_DUPLICATES,
    REBUILD_PROJECTIONS,
    GENERATION_RETENTION,
)
SessionFactory = sessionmaker[Session]


@dataclass(frozen=True)
class MaintenanceResult:
    run_id: int
    operation_type: str
    start_status: str
    end_status: str
    processed_count: int
    failure_info: str
    started_at: datetime
    finished_at: datetime | None
    scanned_count: int = 0

    def as_json(self) -> str:
        payload = asdict(self)
        payload["started_at"] = self.started_at.isoformat()
        payload["finished_at"] = (
            self.finished_at.isoformat() if self.finished_at is not None else None
        )
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


class MaintenanceFinalizationError(RuntimeError):
    def __init__(
        self,
        *,
        run_id: int,
        operation_type: str,
        started_at: datetime,
        cause: Exception,
    ) -> None:
        super().__init__(str(cause) or cause.__class__.__name__)
        self.run_id = run_id
        self.operation_type = operation_type
        self.started_at = started_at


def result_from_record(record: MaintenanceRun) -> MaintenanceResult:
    return MaintenanceResult(
        run_id=record.id,
        operation_type=record.operation_type,
        start_status=record.start_status,
        end_status=record.end_status,
        scanned_count=record.scanned_count,
        processed_count=record.processed_count,
        failure_info=record.failure_info,
        started_at=record.started_at,
        finished_at=record.finished_at,
    )


def source_entry_relation_payload(
    relation: SourceEntryRelation,
) -> dict[str, object]:
    return {
        "id": relation.id,
        "source_entry_id": relation.source_entry_id,
        "canonical_source_entry_id": relation.canonical_source_entry_id,
        "relation_type": relation.relation_type,
        "reason": relation.reason,
        "detected_at": relation.detected_at.isoformat(),
        "rule_version": relation.rule_version,
        "active": relation.active,
        "revoked_at": (
            relation.revoked_at.isoformat()
            if relation.revoked_at is not None
            else None
        ),
    }


def query_duplicate_feed_relations(
    *,
    include_revoked: bool = False,
    session_factory: SessionFactory = SessionLocal,
    prepare_database: Callable[[], None] = prepare_runtime_database,
) -> list[dict[str, object]]:
    prepare_database()
    with session_factory() as session:
        return [
            source_entry_relation_payload(relation)
            for relation in list_duplicate_feed_relations(
                session,
                include_revoked=include_revoked,
            )
        ]


def revoke_duplicate_feed_relation_by_id(
    relation_id: int,
    *,
    session_factory: SessionFactory = SessionLocal,
    prepare_database: Callable[[], None] = prepare_runtime_database,
) -> dict[str, object]:
    prepare_database()
    with session_factory() as session:
        relation = revoke_duplicate_feed_relation(session, relation_id)
        session.commit()
        return source_entry_relation_payload(relation)


def inspect_projection_rebuild_database(
    *,
    mode: str,
    session_factory: SessionFactory = SessionLocal,
    prepare_database: Callable[[], None] = prepare_runtime_database,
) -> dict[str, object]:
    prepare_database()
    with session_factory() as session:
        result = inspect_projection_rebuild(session, mode=mode)
        session.rollback()
        return result


def run_explicit_maintenance(
    operation: str,
    *,
    session_factory: SessionFactory = SessionLocal,
    prepare_database: Callable[[], None] = prepare_runtime_database,
    clock: Callable[[], datetime] = now_utc,
    retention_batch_size: int = GENERATION_RETENTION_BATCH_SIZE,
) -> MaintenanceResult:
    if operation not in EXPLICIT_OPERATIONS:
        raise ValueError(f"不支持的维护操作：{operation}")

    prepare_database()
    with session_factory() as session:
        record = MaintenanceRun(
            operation_type=operation,
            start_status="started",
            end_status="",
            scanned_count=0,
            processed_count=0,
            failure_info="",
            started_at=clock(),
        )
        session.add(record)
        session.commit()
        run_id = record.id
        started_at = record.started_at
        logger.info("maintenance_audit %s", result_from_record(record).as_json())

        if operation in {
            DUPLICATE_FEED_ENTRIES,
            EXACT_CONTENT_DUPLICATES,
            REBUILD_PROJECTIONS,
            GENERATION_RETENTION,
        }:
            try:
                if operation == DUPLICATE_FEED_ENTRIES:
                    processed_count = record_duplicate_feed_relations(session)
                    scanned_count = 0
                elif operation == EXACT_CONTENT_DUPLICATES:
                    processed_count = repair_exact_content_duplicates(session)
                    scanned_count = 0
                elif operation == REBUILD_PROJECTIONS:
                    report = rebuild_projections(session)
                    processed_count = int(report["published_count"])
                    scanned_count = 0
                else:
                    scanned_count, processed_count = purge_expired_generation_data(
                        session,
                        now=clock(),
                        batch_size=retention_batch_size,
                    )
            except Exception as exc:
                session.rollback()
                result = _finalize_maintenance_run(
                    session,
                    run_id=run_id,
                    operation_type=operation,
                    started_at=started_at,
                    end_status="failed",
                    scanned_count=0,
                    processed_count=0,
                    failure_info=str(exc) or exc.__class__.__name__,
                    clock=clock,
                )
            else:
                result = _finalize_maintenance_run(
                    session,
                    run_id=run_id,
                    operation_type=operation,
                    started_at=started_at,
                    end_status="succeeded",
                    scanned_count=scanned_count,
                    processed_count=processed_count,
                    failure_info="",
                    clock=clock,
                )
        else:
            result = _finalize_maintenance_run(
                session,
                run_id=run_id,
                operation_type=operation,
                started_at=started_at,
                end_status="failed",
                scanned_count=0,
                processed_count=0,
                failure_info=DISABLED_OPERATION_MESSAGES[operation],
                clock=clock,
            )
        log_result = logger.info if result.end_status == "succeeded" else logger.error
        log_result("maintenance_audit %s", result.as_json())
        return result


def _finalize_maintenance_run(
    session: Session,
    *,
    run_id: int,
    operation_type: str,
    started_at: datetime,
    end_status: str,
    scanned_count: int,
    processed_count: int,
    failure_info: str,
    clock: Callable[[], datetime] = now_utc,
) -> MaintenanceResult:
    try:
        record = session.get(MaintenanceRun, run_id)
        if record is None:
            raise RuntimeError(f"维护审计记录丢失：{run_id}")
        record.end_status = end_status
        record.scanned_count = scanned_count
        record.processed_count = processed_count
        record.failure_info = failure_info
        record.finished_at = clock()
        session.commit()
    except Exception as exc:
        session.rollback()
        raise MaintenanceFinalizationError(
            run_id=run_id,
            operation_type=operation_type,
            started_at=started_at,
            cause=exc,
        ) from exc
    return result_from_record(record)


def purge_expired_generation_data(
    session: Session,
    *,
    now: datetime,
    batch_size: int = GENERATION_RETENTION_BATCH_SIZE,
) -> tuple[int, int]:
    if batch_size < 1:
        raise ValueError("生成保留清理批次必须大于 0")
    cutoff = now - timedelta(days=GENERATION_RETENTION_DAYS)
    payload_ids = list(
        session.scalars(
            select(GenerationRequestPayload.request_id)
            .where(
                GenerationRequestPayload.payload_json.is_not(None),
                GenerationRequestPayload.created_at < cutoff,
            )
            .order_by(
                GenerationRequestPayload.created_at,
                GenerationRequestPayload.request_id,
            )
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        ).all()
    )
    remaining = batch_size - len(payload_ids)
    audit_ids = (
        list(
            session.scalars(
                select(GenerationAttemptRunnerAudit.attempt_id)
                .where(
                    GenerationAttemptRunnerAudit.events_json.is_not(None),
                    GenerationAttemptRunnerAudit.created_at < cutoff,
                )
                .order_by(
                    GenerationAttemptRunnerAudit.created_at,
                    GenerationAttemptRunnerAudit.attempt_id,
                )
                .limit(remaining)
                .with_for_update(skip_locked=True)
            ).all()
        )
        if remaining
        else []
    )
    if payload_ids:
        session.execute(
            update(GenerationRequestPayload)
            .where(GenerationRequestPayload.request_id.in_(payload_ids))
            .values(payload_json=None, purged_at=now)
        )
    if audit_ids:
        session.execute(
            update(GenerationAttemptRunnerAudit)
            .where(GenerationAttemptRunnerAudit.attempt_id.in_(audit_ids))
            .values(events_json=None, purged_at=now)
        )
    deleted_count = len(payload_ids) + len(audit_ids)
    session.flush()
    return deleted_count, deleted_count


def generation_retention_status(session: Session) -> dict[str, object]:
    record = session.scalar(
        select(MaintenanceRun)
        .where(MaintenanceRun.operation_type == GENERATION_RETENTION)
        .order_by(MaintenanceRun.started_at.desc(), MaintenanceRun.id.desc())
        .limit(1)
    )
    if record is None:
        return {
            "status": "never",
            "last_run_at": None,
            "finished_at": None,
            "scanned_count": 0,
            "deleted_count": 0,
            "failure_reason": "",
        }
    return {
        "status": record.end_status or "running",
        "last_run_at": _utc(record.started_at),
        "finished_at": _utc(record.finished_at),
        "scanned_count": record.scanned_count,
        "deleted_count": record.processed_count,
        "failure_reason": record.failure_info,
    }


def _utc(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def run_scheduled_generation_retention(
    *,
    session_factory: SessionFactory = SessionLocal,
    prepare_database: Callable[[], None] = prepare_runtime_database,
    clock: Callable[[], datetime] = now_utc,
) -> MaintenanceResult | None:
    prepare_database()
    now = clock()
    with session_factory() as session:
        latest_success = session.scalar(
            select(MaintenanceRun)
            .where(
                MaintenanceRun.operation_type == GENERATION_RETENTION,
                MaintenanceRun.end_status == "succeeded",
            )
            .order_by(MaintenanceRun.finished_at.desc(), MaintenanceRun.id.desc())
            .limit(1)
        )
        if (
            latest_success is not None
            and _utc(latest_success.finished_at) is not None
            and _utc(latest_success.finished_at) >= now - GENERATION_RETENTION_INTERVAL
        ):
            return None
    return run_explicit_maintenance(
        GENERATION_RETENTION,
        session_factory=session_factory,
        prepare_database=lambda: None,
        clock=lambda: now,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m reader_api.maintenance",
        description="显式运行一次历史派生数据维护，并持久记录审计结果。",
    )
    parser.add_argument(
        "operation",
        choices=(
            *EXPLICIT_OPERATIONS,
            LIST_DUPLICATE_FEED_RELATIONS,
            REVOKE_DUPLICATE_FEED_RELATION,
        ),
    )
    parser.add_argument("relation_id", nargs="?", type=int)
    parser.add_argument("--include-revoked", action="store_true")
    report_mode = parser.add_mutually_exclusive_group()
    report_mode.add_argument("--verify", action="store_true")
    report_mode.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if (args.verify or args.dry_run) and args.operation != REBUILD_PROJECTIONS:
        parser.error("只有 rebuild-projections 接受 --verify/--dry-run")
    if args.operation == REBUILD_PROJECTIONS and (args.verify or args.dry_run):
        if args.relation_id is not None or args.include_revoked:
            parser.error("投影检查不接受 relation 参数")
        mode = "verify" if args.verify else "dry-run"
        try:
            payload = inspect_projection_rebuild_database(mode=mode)
        except Exception as exc:
            logger.exception("投影检查失败")
            print(
                json.dumps(
                    {
                        "mode": mode,
                        "matches": False,
                        "failure_info": str(exc) or exc.__class__.__name__,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if mode == "dry-run" or payload.get("matches") is True else 1
    if args.operation == LIST_DUPLICATE_FEED_RELATIONS:
        if args.relation_id is not None:
            parser.error("查询重复关系不接受 relation id")
        payload = query_duplicate_feed_relations(
            include_revoked=args.include_revoked
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    if args.operation == REVOKE_DUPLICATE_FEED_RELATION:
        if args.relation_id is None:
            parser.error("撤销重复关系必须提供 relation id")
        if args.include_revoked:
            parser.error("撤销重复关系不接受 --include-revoked")
        try:
            payload = revoke_duplicate_feed_relation_by_id(args.relation_id)
        except Exception as exc:
            logger.exception("撤销重复关系失败")
            print(
                json.dumps(
                    {
                        "operation_type": args.operation,
                        "end_status": "failed",
                        "failure_info": str(exc) or exc.__class__.__name__,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    if args.relation_id is not None or args.include_revoked:
        parser.error("该维护操作不接受 relation 参数")
    try:
        result = run_explicit_maintenance(args.operation)
    except MaintenanceFinalizationError as exc:
        logger.exception("显式维护结束审计未确认")
        print(
            json.dumps(
                {
                    "run_id": exc.run_id,
                    "operation_type": exc.operation_type,
                    "start_status": "started",
                    "end_status": "unconfirmed",
                    "scanned_count": 0,
                    "processed_count": 0,
                    "failure_info": f"结束审计未确认：{exc}",
                    "started_at": exc.started_at.isoformat(),
                    "finished_at": None,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    except Exception as exc:
        logger.exception("显式维护入口执行失败")
        print(
            json.dumps(
                {
                    "operation_type": args.operation,
                    "start_status": "not_recorded",
                    "end_status": "failed",
                    "processed_count": 0,
                    "failure_info": str(exc) or exc.__class__.__name__,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1

    print(result.as_json())
    if result.end_status == "succeeded":
        return 0
    return (
        1
        if args.operation in {REBUILD_PROJECTIONS, GENERATION_RETENTION}
        else 2
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())
