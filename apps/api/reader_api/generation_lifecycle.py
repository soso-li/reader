from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from .generation_types import GenerationFailureClass, GenerationRetryKind
from .models import (
    EvidenceReview,
    GenerationApplication,
    GenerationAdmission,
    GenerationAttempt,
    GenerationAttemptRunnerAudit,
    GenerationControl,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationRequestSource,
    GenerationResult,
    Source,
    SynthesisVersion,
    now_utc,
)
from .schemas import GenerationControlOut, GenerationTaskOut


class GenerationControlError(ValueError):
    pass


class GenerationApprovalConsumedError(ValueError):
    pass


class GenerationRequestHasResultError(ValueError):
    pass


class GenerationRequestNotRetryableError(ValueError):
    pass


POSTGRES_INTEGER_MAX = 2_147_483_647
GENERATION_LIFECYCLE_LOCK_ID = 0x52454144
SYNTHESIS_APPLICATION_CONTEXT_KEYS = (
    "event_uid",
    "target_revision_uid",
    "snapshot_uid",
    "policy_version",
    "prompt_version",
    "schema_version",
    "source_coverage_fingerprint",
    "content_fingerprint",
    "generation_fingerprint",
)
EVIDENCE_REVIEW_APPLICATION_CONTEXT_KEYS = (
    "event_uid",
    "baseline_snapshot_uid",
    "baseline_revision_uid",
    "target_snapshot_uid",
    "target_revision_uid",
    "policy_version",
    "prompt_version",
    "schema_version",
    "comparison_fingerprint",
)


@dataclass
class GenerationTaskContext:
    control: GenerationControl
    admissions: dict[int, GenerationAdmission]
    payloads: dict[int, GenerationRequestPayload]
    attempts: dict[int, list[GenerationAttempt]]
    result_applications: dict[
        int, tuple[GenerationResult, GenerationApplication]
    ]
    sources: dict[int, list[GenerationRequestSource]]
    current_sources: dict[int, Source]
    runner_audits: dict[int, GenerationAttemptRunnerAudit]
    artifact_uids: dict[tuple[str, int], str]


def lock_generation_lifecycle(session: Session) -> None:
    if session.get_bind().dialect.name == "postgresql":
        session.execute(
            select(func.pg_advisory_xact_lock(GENERATION_LIFECYCLE_LOCK_ID))
        )


def generation_control_out(session: Session) -> GenerationControlOut:
    control = current_generation_control(session)
    used_tokens, reserved_tokens = daily_generation_usage(session, control)
    remaining_tokens = (
        None
        if control.daily_budget_tokens is None
        else max(control.daily_budget_tokens - used_tokens - reserved_tokens, 0)
    )
    return GenerationControlOut(
        global_pause=control.global_pause,
        auto_run=control.auto_run,
        daily_budget_tokens=control.daily_budget_tokens,
        input_estimator=control.input_estimator,
        output_reserve_tokens=control.output_reserve_tokens,
        day_timezone=control.day_timezone,
        used_tokens=used_tokens,
        reserved_tokens=reserved_tokens,
        remaining_tokens=remaining_tokens,
        requires_usage_review=(
            not control.auto_run
            and session.scalar(
                select(GenerationResult.id)
                .where(
                    (GenerationResult.input_tokens.is_(None))
                    | (GenerationResult.output_tokens.is_(None))
                )
                .limit(1)
            )
            is not None
        ),
    )


def daily_generation_usage(
    session: Session, control: GenerationControl
) -> tuple[int, int]:
    zone = ZoneInfo(control.day_timezone)
    local_now = now_utc().astimezone(zone)
    day_start = datetime.combine(local_now.date(), time.min, zone).astimezone(
        timezone.utc
    )
    day_end = datetime.combine(
        local_now.date() + timedelta(days=1),
        time.min,
        zone,
    ).astimezone(timezone.utc)
    attempts = session.scalars(
        select(GenerationAttempt).where(
            GenerationAttempt.created_at >= day_start,
            GenerationAttempt.created_at < day_end,
        )
    ).all()
    used_tokens = 0
    reserved_tokens = 0
    for attempt in attempts:
        estimate = attempt.input_tokens_estimated or 0
        reserve = attempt.output_tokens_reserved or 0
        if attempt.status in {"pending", "running"}:
            reserved_tokens += estimate + reserve
        elif attempt.status in {"complete", "failed", "canceled", "expired"}:
            used_tokens += (attempt.input_tokens_actual or 0) + (
                attempt.output_tokens_actual or 0
            )
    return used_tokens, reserved_tokens


def current_generation_control(session: Session) -> GenerationControl:
    return session.get(GenerationControl, 1) or GenerationControl(
        id=1,
        global_pause=True,
        auto_run=False,
        daily_budget_tokens=None,
        input_estimator="unicode-codepoints-v1",
        output_reserve_tokens=0,
        day_timezone="Asia/Shanghai",
    )


def locked_generation_control(session: Session) -> GenerationControl:
    control = session.scalar(
        select(GenerationControl)
        .where(GenerationControl.id == 1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if control is None:
        control = current_generation_control(session)
        session.add(control)
        session.flush()
    return control


def generation_admission(
    session: Session, request: GenerationRequest, *, lock: bool = False
) -> GenerationAdmission:
    statement = select(GenerationAdmission).where(
        GenerationAdmission.request_id == request.id
    )
    if lock:
        statement = statement.with_for_update().execution_options(
            populate_existing=True
        )
    admission = session.scalar(statement)
    if admission is None:
        admission = GenerationAdmission(
            request_id=request.id,
            approval_status="awaiting",
            admission_status="awaiting",
            admission_reason="",
        )
        session.add(admission)
        session.flush()
    return admission


def locked_generation_request(
    session: Session, request: GenerationRequest
) -> GenerationRequest:
    locked = session.scalar(
        select(GenerationRequest)
        .where(GenerationRequest.id == request.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if locked is None:
        raise RuntimeError("Generation Request 不存在")
    return locked


def approve_generation_request(
    session: Session,
    request: GenerationRequest,
    *,
    allow_consumed_reissue: bool = False,
    allow_canceled_reissue: bool = False,
) -> GenerationAdmission:
    request = locked_generation_request(session, request)
    existing_result_id = session.scalar(
        select(GenerationResult.id)
        .where(GenerationResult.request_id == request.id)
        .limit(1)
    )
    if existing_result_id is not None:
        raise GenerationRequestHasResultError("已有生成结果，无需再次批准")
    frozen = session.get(GenerationRequestPayload, request.id)
    if request.privacy_status != "blocked" and (
        frozen is None or frozen.payload_json is None
    ):
        raise GenerationRequestNotRetryableError(
            "请求 Payload 已按保留策略清理，请从原业务重新发起"
        )
    assert_generation_request_eligible(session, request, lock_sources=True)
    admission = generation_admission(session, request, lock=True)
    if admission.canceled_at is not None and not allow_canceled_reissue:
        raise GenerationRequestNotRetryableError("任务已取消，请使用明确重试")
    if admission.approval_status == "consumed" and not allow_consumed_reissue:
        raise GenerationApprovalConsumedError("这次执行许可已被消费")
    if allow_consumed_reissue and admission.next_attempt_kind is None:
        admission.next_attempt_kind = "manual"
    now = now_utc()
    admission.approval_status = "approved"
    admission.approved_at = now
    admission.consumed_at = None
    admission.updated_at = now
    session.flush()
    return admission


def estimate_generation_input(
    session: Session,
    request: GenerationRequest,
    estimator_version: str,
) -> int:
    payload = session.get(GenerationRequestPayload, request.id)
    if payload is None:
        raise RuntimeError("Generation Request 缺少冻结 payload")
    if payload.payload_json is None:
        raise RuntimeError("Generation Request payload 已按保留策略清理")
    return estimate_generation_payload(payload.payload_json, estimator_version)


def estimate_generation_payload(
    payload: dict[str, object], estimator_version: str
) -> int:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    if estimator_version == "unicode-codepoints-v1":
        return len(canonical)
    if estimator_version == "utf8-bytes-v1":
        return len(canonical.encode("utf-8"))
    raise GenerationControlError("输入估算规则无效")


def start_admitted_generation_attempt(
    session: Session,
    request: GenerationRequest,
    *,
    runner_id: str | None = None,
    runner_environment_id: str | None = None,
    runner_cli_version: str | None = None,
    lease_token_hash: str | None = None,
    lease_expires_at: datetime | None = None,
    last_heartbeat_at: datetime | None = None,
) -> GenerationAttempt | None:
    request = locked_generation_request(session, request)
    control = locked_generation_control(session)
    admission = generation_admission(session, request, lock=True)
    retry_kind = admission.next_attempt_kind
    if retry_kind is None:
        return None
    admission.input_tokens_estimated = estimate_generation_input(
        session, request, control.input_estimator
    )
    admission.output_tokens_reserved = control.output_reserve_tokens
    admission.updated_at = now_utc()
    if control.global_pause:
        admission.admission_status = "blocked_paused"
        admission.admission_reason = "生成任务已全局暂停"
        session.flush()
        return None
    if control.daily_budget_tokens is None:
        admission.admission_status = "blocked_budget_unconfigured"
        admission.admission_reason = "每日 Token 预算尚未配置"
        session.flush()
        return None
    if admission.approval_status != "approved" and not control.auto_run:
        admission.admission_status = "awaiting"
        admission.admission_reason = "等待用户批准"
        session.flush()
        return None
    if session.scalar(
        select(GenerationAttempt.id)
        .where(GenerationAttempt.status == "running")
        .limit(1)
    ) is not None:
        admission.admission_status = "blocked_concurrency"
        admission.admission_reason = "已有生成任务正在执行"
        session.flush()
        return None
    used_tokens, reserved_tokens = daily_generation_usage(session, control)
    requested_tokens = (
        admission.input_tokens_estimated + admission.output_tokens_reserved
    )
    if used_tokens + reserved_tokens + requested_tokens > control.daily_budget_tokens:
        admission.admission_status = "blocked_budget"
        admission.admission_reason = "今日 Token 预算不足"
        session.flush()
        return None
    # Claim must recheck the current Source policy while the Request is locked
    # and before consuming its one-shot approval. start_generation_attempt()
    # repeats this check as a defense-in-depth write boundary.
    assert_generation_request_eligible(session, request, lock_sources=True)
    now = now_utc()
    admission.approval_status = "consumed"
    admission.admission_status = "admitted"
    admission.admission_reason = ""
    admission.consumed_at = now
    if admission.approved_at is None:
        admission.approved_at = now
    admission.updated_at = now
    attempt = start_generation_attempt(
        session,
        request,
        retry_kind=retry_kind,
        estimator_version=control.input_estimator,
        input_tokens_estimated=admission.input_tokens_estimated,
        output_tokens_reserved=admission.output_tokens_reserved,
        runner_id=runner_id,
        runner_environment_id=runner_environment_id,
        runner_cli_version=runner_cli_version,
        lease_token_hash=lease_token_hash,
        lease_expires_at=lease_expires_at,
        last_heartbeat_at=last_heartbeat_at,
    )
    admission.next_attempt_kind = None
    session.flush()
    return attempt


def save_generation_control(
    session: Session, changes: dict[str, object]
) -> GenerationControlOut:
    for field in (
        "global_pause",
        "auto_run",
        "input_estimator",
        "output_reserve_tokens",
        "day_timezone",
    ):
        if field in changes and changes[field] is None:
            raise GenerationControlError(f"{field} 不能留空")
    budget = changes.get("daily_budget_tokens")
    if budget is not None and int(budget) < 0:
        raise GenerationControlError("每日预算不能小于 0，或留空为未配置")
    if budget is not None and int(budget) > POSTGRES_INTEGER_MAX:
        raise GenerationControlError("每日预算不能超过 2,147,483,647")
    reserve = changes.get("output_reserve_tokens")
    if reserve is not None and int(reserve) < 0:
        raise GenerationControlError("每次输出预留不能小于 0")
    if reserve is not None and int(reserve) > POSTGRES_INTEGER_MAX:
        raise GenerationControlError("每次输出预留不能超过 2,147,483,647")
    if "day_timezone" in changes:
        day_timezone = str(changes["day_timezone"]).strip()
        try:
            ZoneInfo(day_timezone)
        except (ValueError, ZoneInfoNotFoundError):
            raise GenerationControlError("日界时区无效") from None
        changes["day_timezone"] = day_timezone
    control = current_generation_control(session)
    for field, value in changes.items():
        setattr(control, field, value)
    control.updated_at = now_utc()
    session.add(control)
    session.commit()
    return generation_control_out(session)


class GenerationPrivacyError(RuntimeError):
    pass


def utc_time(value: datetime | None) -> datetime | None:
    return value.replace(tzinfo=timezone.utc) if value is not None and value.tzinfo is None else value


def stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    ).hexdigest()


def generation_application_context(
    task_type: str,
    payload: dict[str, object] | None,
) -> dict[str, object] | None:
    if payload is None:
        return None
    if task_type.startswith("report:"):
        from .report_generation import report_application_context

        return report_application_context(task_type, payload)
    input_data = payload.get("input_data")
    if not isinstance(input_data, dict):
        return None
    if task_type == "item-summary":
        return {}
    if task_type == "cluster-synthesis":
        citations = input_data.get("citations")
        if not isinstance(citations, list) or not citations:
            return None
        retained: list[dict[str, object]] = []
        for citation in citations:
            if not isinstance(citation, dict):
                return None
            retained.append(
                {
                    key: citation.get(key)
                    for key in (
                        "source_id",
                        "source_name",
                        "url",
                        "published_at",
                    )
                }
            )
        return {"citations": retained}
    keys = (
        SYNTHESIS_APPLICATION_CONTEXT_KEYS
        if task_type == "event-synthesis"
        else EVIDENCE_REVIEW_APPLICATION_CONTEXT_KEYS
        if task_type == "evidence-review"
        else ()
    )
    if not keys:
        return None
    context = {key: input_data.get(key) for key in keys}
    if not all(
        isinstance(value, str) and value.strip() for value in context.values()
    ) or not context:
        return None
    return context


def generation_request_fingerprint(
    *,
    task_type: str,
    reason: str,
    target_type: str,
    target_uid: str,
    provider: str,
    model: str,
    prompt_version: str,
    schema_version: str,
    input_fingerprint: str,
    payload_fingerprint: str,
    source_policy_fingerprint: str | None = None,
) -> str:
    fields = {
        "task_type": task_type,
        "reason": reason,
        "target_type": target_type,
        "target_uid": target_uid,
        "provider": provider,
        "model": model,
        "prompt_version": prompt_version,
        "schema_version": schema_version,
        "input_fingerprint": input_fingerprint,
        "payload_fingerprint": payload_fingerprint,
    }
    if source_policy_fingerprint is not None:
        fields["source_policy_fingerprint"] = source_policy_fingerprint
    return stable_hash(fields)


def external_generation_policy(
    session: Session,
    source_ids: list[int],
    *,
    lock_sources: bool = False,
    provenance_complete: bool = True,
) -> tuple[list[Source], str, str]:
    requested_ids = sorted({source_id for source_id in source_ids if source_id > 0})
    source_query = select(Source).where(Source.id.in_(requested_ids)).order_by(Source.id)
    source_query = source_query.execution_options(populate_existing=True)
    if lock_sources:
        source_query = source_query.with_for_update()
    sources = list(
        session.scalars(source_query).all()
    )
    found_ids = {source.id for source in sources}
    missing_ids = [source_id for source_id in requested_ids if source_id not in found_ids]
    policy_fingerprint = stable_hash(
        {
            "sources": [
                {
                    "source_id": source.id,
                    "privacy_class": source.privacy_class,
                    "external_generation_allowed": source.external_generation_allowed,
                    "source_policy_version": source.generation_policy_version,
                }
                for source in sources
            ],
            "missing_source_ids": missing_ids,
            "provenance_complete": provenance_complete,
        }
    )
    if not requested_ids or missing_ids or not provenance_complete:
        return sources, policy_fingerprint, "无法证明全部来源，不能发送给外部生成服务"
    if any(source.privacy_class == "private" for source in sources):
        return sources, policy_fingerprint, "包含私密来源，只能使用本地 LM Studio"
    if any(source.privacy_class == "unclassified" for source in sources):
        return sources, policy_fingerprint, "包含未分类来源，不能发送给外部生成服务"
    if any(not source.external_generation_allowed for source in sources):
        return sources, policy_fingerprint, "来源尚未允许发送给外部生成服务"
    return sources, policy_fingerprint, ""


def get_or_create_generation_request(
    session: Session,
    *,
    task_type: str,
    reason: str,
    target_type: str,
    target_id: int,
    target_uid: str,
    provider: str,
    model: str,
    prompt_version: str,
    schema_version: str,
    input_fingerprint: str,
    payload: dict[str, object] | None,
    privacy_status: str = "local",
    privacy_reason: str = "",
    source_policy_fingerprint: str | None = None,
    source_policies: list[Source] | None = None,
) -> tuple[GenerationRequest, bool]:
    if provider not in {"local", "openai_compatible"}:
        raise ValueError("不支持的生成服务")
    if privacy_status not in {"local", "eligible", "blocked"}:
        raise ValueError("Generation Request 隐私状态无效")
    if provider == "openai_compatible" and privacy_status == "local":
        raise ValueError("外部 Generation Request 不得使用本地隐私状态")
    if privacy_status == "local":
        if payload is None or privacy_reason or source_policy_fingerprint is not None:
            raise ValueError("本地 Generation Request 隐私字段无效")
    elif source_policy_fingerprint is None:
        raise ValueError("外部 Generation Request 缺少来源策略指纹")
    elif privacy_status == "blocked" and (payload is not None or not privacy_reason):
        raise ValueError("被阻断的 Generation Request 不得保存 payload")
    elif privacy_status == "eligible" and (payload is None or privacy_reason):
        raise ValueError("可外发 Generation Request 隐私字段无效")
    elif privacy_status == "eligible" and not source_policies:
        raise ValueError("可外发 Generation Request 缺少来源策略快照")
    payload_fingerprint = stable_hash(
        payload
        if payload is not None
        else {
            "privacy_status": privacy_status,
            "privacy_reason": privacy_reason,
            "source_policy_fingerprint": source_policy_fingerprint,
        }
    )
    application_context = generation_application_context(task_type, payload)
    request_fingerprint = generation_request_fingerprint(
        task_type=task_type,
        reason=reason,
        target_type=target_type,
        target_uid=target_uid,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        schema_version=schema_version,
        input_fingerprint=input_fingerprint,
        payload_fingerprint=payload_fingerprint,
        source_policy_fingerprint=source_policy_fingerprint,
    )
    lock_generation_lifecycle(session)
    request = session.scalar(
        select(GenerationRequest).where(
            GenerationRequest.request_fingerprint == request_fingerprint
        )
        .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
        .limit(1)
    )
    if request is not None:
        frozen = session.get(GenerationRequestPayload, request.id)
        if payload is None and frozen is not None:
            raise RuntimeError("被阻断的 Generation Request 不得包含 payload")
        if payload is not None:
            if frozen is None or frozen.payload_fingerprint != payload_fingerprint:
                raise RuntimeError("Generation Request 的冻结 payload 不匹配")
            if frozen.payload_json is not None:
                if frozen.payload_json != payload:
                    raise RuntimeError("Generation Request 的冻结 payload 不匹配")
                if frozen.application_context_json != application_context:
                    raise RuntimeError(
                        "Generation Request 的应用校验上下文不匹配"
                    )
                return request, False
            result = session.scalar(
                select(GenerationResult)
                .where(GenerationResult.request_id == request.id)
                .order_by(GenerationResult.id.desc())
                .limit(1)
            )
            if result is not None:
                application = session.scalar(
                    select(GenerationApplication)
                    .where(GenerationApplication.result_id == result.id)
                    .limit(1)
                )
                if generation_result_currency(
                    session,
                    request=request,
                    attempt=latest_attempt(session, request.id),
                    result=result,
                    application=application,
                ) == "current":
                    return request, False
        else:
            return request, False
    request = GenerationRequest(
        request_fingerprint=request_fingerprint,
        input_fingerprint=input_fingerprint,
        task_type=task_type,
        reason=reason,
        target_type=target_type,
        target_id=target_id,
        target_uid=target_uid,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        schema_version=schema_version,
        privacy_status=privacy_status,
        privacy_reason=privacy_reason,
        source_policy_fingerprint=source_policy_fingerprint,
    )
    session.add(request)
    session.flush()
    for source in source_policies or []:
        session.add(
            GenerationRequestSource(
                request_id=request.id,
                source_id=source.id,
                source_name=source.name,
                privacy_class=source.privacy_class,
                external_generation_allowed=source.external_generation_allowed,
                source_policy_version=source.generation_policy_version,
            )
        )
    if payload is not None:
        session.add(
            GenerationRequestPayload(
                request_id=request.id,
                payload_json=payload,
                application_context_json=application_context,
                payload_fingerprint=payload_fingerprint,
            )
        )
    session.flush()
    control = current_generation_control(session)
    session.add(
        GenerationAdmission(
            request_id=request.id,
            approval_status="awaiting",
            admission_status="awaiting",
            admission_reason="",
            next_attempt_kind="initial",
            input_tokens_estimated=(
                estimate_generation_payload(payload, control.input_estimator)
                if payload is not None
                else None
            ),
            output_tokens_reserved=(
                control.output_reserve_tokens if payload is not None else None
            ),
        )
    )
    session.flush()
    return request, True


def generation_request_sources(
    session: Session, request_id: int
) -> list[GenerationRequestSource]:
    return list(
        session.scalars(
            select(GenerationRequestSource)
            .where(GenerationRequestSource.request_id == request_id)
            .order_by(GenerationRequestSource.source_id)
        ).all()
    )


def generation_privacy_status(
    session: Session,
    request: GenerationRequest,
    *,
    lock_sources: bool = False,
    snapshots: list[GenerationRequestSource] | None = None,
    current_sources: dict[int, Source] | None = None,
) -> tuple[str, str]:
    if request.privacy_status == "local":
        return "local", ""
    if request.privacy_status == "blocked":
        return "blocked", request.privacy_reason
    snapshots = (
        generation_request_sources(session, request.id)
        if snapshots is None
        else snapshots
    )
    if not snapshots:
        return "blocked", "无法证明全部来源，不能发送给外部生成服务"
    if current_sources is None:
        source_query = (
            select(Source)
            .where(Source.id.in_([row.source_id for row in snapshots]))
            .execution_options(populate_existing=True)
        )
        if lock_sources:
            source_query = source_query.with_for_update()
        current = {
            source.id: source
            for source in session.scalars(source_query).all()
        }
    else:
        current = current_sources
    for snapshot in snapshots:
        source = current.get(snapshot.source_id)
        if (
            source is None
            or source.generation_policy_version != snapshot.source_policy_version
            or source.privacy_class != snapshot.privacy_class
            or source.external_generation_allowed
            != snapshot.external_generation_allowed
            or source.privacy_class != "public"
            or not source.external_generation_allowed
        ):
            return "blocked", "来源外部生成策略已变更，请重新批准此任务"
    return "eligible", ""


def assert_generation_request_eligible(
    session: Session,
    request: GenerationRequest,
    *,
    lock_sources: bool = False,
) -> None:
    privacy_status, reason = generation_privacy_status(
        session, request, lock_sources=lock_sources
    )
    if privacy_status == "blocked":
        raise GenerationPrivacyError(reason)


def latest_attempt(
    session: Session, request_id: int
) -> GenerationAttempt | None:
    return session.scalar(
        select(GenerationAttempt)
        .where(GenerationAttempt.request_id == request_id)
        .order_by(GenerationAttempt.attempt_no.desc())
        .limit(1)
    )


def start_generation_attempt(
    session: Session,
    request: GenerationRequest,
    *,
    retry_kind: GenerationRetryKind = "initial",
    estimator_version: str | None = None,
    input_tokens_estimated: int | None = None,
    output_tokens_reserved: int | None = None,
    runner_id: str | None = None,
    runner_environment_id: str | None = None,
    runner_cli_version: str | None = None,
    lease_token_hash: str | None = None,
    lease_expires_at: datetime | None = None,
    last_heartbeat_at: datetime | None = None,
) -> GenerationAttempt:
    locked_request = session.scalar(
        select(GenerationRequest)
        .where(GenerationRequest.id == request.id)
        .with_for_update()
    )
    if locked_request is None:
        raise RuntimeError("Generation Request 不存在")
    assert_generation_request_eligible(session, locked_request, lock_sources=True)
    attempt_no = int(
        session.scalar(
            select(func.max(GenerationAttempt.attempt_no)).where(
                GenerationAttempt.request_id == request.id
            )
        )
        or 0
    ) + 1
    now = now_utc()
    attempt = GenerationAttempt(
        request_id=request.id,
        attempt_no=attempt_no,
        retry_kind=retry_kind,
        status="running",
        estimator_version=estimator_version,
        input_tokens_estimated=input_tokens_estimated,
        output_tokens_reserved=output_tokens_reserved,
        runner_id=runner_id,
        runner_environment_id=runner_environment_id,
        runner_cli_version=runner_cli_version,
        lease_token_hash=lease_token_hash,
        lease_expires_at=lease_expires_at,
        last_heartbeat_at=last_heartbeat_at,
        started_at=now,
        created_at=now,
        updated_at=now,
    )
    session.add(attempt)
    session.flush()
    return attempt


def schedule_automatic_generation_retry(
    session: Session, request: GenerationRequest
) -> bool:
    if session.scalar(
        select(GenerationAttempt.id)
        .where(
            GenerationAttempt.request_id == request.id,
            GenerationAttempt.retry_kind == "automatic",
        )
        .limit(1)
    ) is not None:
        return False
    request = locked_generation_request(session, request)
    if session.scalar(
        select(GenerationResult.id)
        .where(GenerationResult.request_id == request.id)
        .limit(1)
    ) is not None:
        return False
    try:
        assert_generation_request_eligible(session, request, lock_sources=True)
    except GenerationPrivacyError:
        return False
    admission = generation_admission(session, request, lock=True)
    if admission.canceled_at is not None:
        return False
    admission.approval_status = "awaiting"
    admission.approved_at = None
    admission.consumed_at = None
    admission.next_attempt_kind = "automatic"
    admission.admission_status = "awaiting"
    admission.admission_reason = ""
    admission.updated_at = now_utc()
    session.flush()
    return True


def fail_generation_attempt(
    session: Session,
    attempt: GenerationAttempt,
    error: str,
    *,
    failure_class: GenerationFailureClass = "validation",
    input_tokens: int | None = None,
    output_tokens: int | None = None,
) -> None:
    attempt.status = "canceled" if failure_class == "canceled" else "failed"
    attempt.error = error.strip() or "生成失败"
    attempt.failure_class = failure_class
    attempt.input_tokens_actual = input_tokens
    attempt.output_tokens_actual = output_tokens
    attempt.finished_at = now_utc()
    attempt.updated_at = attempt.finished_at
    session.flush()


def token_usage(payload: dict[str, object]) -> tuple[int | None, int | None]:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None, None

    def value(*keys: str) -> int | None:
        for key in keys:
            candidate = usage.get(key)
            if (
                isinstance(candidate, int)
                and not isinstance(candidate, bool)
                and 0 <= candidate <= POSTGRES_INTEGER_MAX
            ):
                return candidate
        return None

    return value("input_tokens", "prompt_tokens"), value(
        "output_tokens", "completion_tokens"
    )


def complete_generation_attempt(
    session: Session,
    *,
    attempt: GenerationAttempt,
    payload: dict[str, object],
    input_tokens: int | None,
    output_tokens: int | None,
) -> tuple[GenerationResult, GenerationApplication]:
    request = session.get(GenerationRequest, attempt.request_id)
    if request is None:
        raise RuntimeError("Generation Request 不存在")
    result = GenerationResult(
        request_id=attempt.request_id,
        attempt_id=attempt.id,
        payload_json=payload,
        payload_fingerprint=stable_hash(
            {
                "payload": payload,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            }
        ),
        output_fingerprint=stable_hash(payload),
        schema_version=request.schema_version,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    session.add(result)
    attempt.status = "complete"
    attempt.input_tokens_actual = input_tokens
    attempt.output_tokens_actual = output_tokens
    attempt.finished_at = now_utc()
    attempt.updated_at = attempt.finished_at
    session.flush()
    application = GenerationApplication(
        request_id=attempt.request_id,
        result_id=result.id,
        status="pending",
    )
    session.add(application)
    if input_tokens is None or output_tokens is None:
        control = locked_generation_control(session)
        control.auto_run = False
        control.updated_at = now_utc()
    session.flush()
    return result, application


def generation_result_for_request(
    session: Session, request_id: int
) -> tuple[GenerationResult, GenerationApplication] | None:
    row = session.execute(
        select(GenerationResult, GenerationApplication)
        .join(
            GenerationApplication,
            GenerationApplication.result_id == GenerationResult.id,
        )
        .where(GenerationResult.request_id == request_id)
        .order_by(GenerationResult.id.desc())
        .limit(1)
    ).first()
    return (row[0], row[1]) if row is not None else None


def latest_generation_request(
    session: Session,
    *,
    task_type: str,
    target_type: str,
    target_id: int,
    input_fingerprint: str,
) -> GenerationRequest | None:
    return session.scalar(
        select(GenerationRequest)
        .where(
            GenerationRequest.task_type == task_type,
            GenerationRequest.target_type == target_type,
            GenerationRequest.target_id == target_id,
            GenerationRequest.input_fingerprint == input_fingerprint,
        )
        .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
        .limit(1)
    )


def generation_task_context(
    session: Session, requests: list[GenerationRequest]
) -> GenerationTaskContext:
    request_ids = [request.id for request in requests]
    attempts: dict[int, list[GenerationAttempt]] = defaultdict(list)
    for attempt in session.scalars(
        select(GenerationAttempt)
        .where(GenerationAttempt.request_id.in_(request_ids))
        .order_by(GenerationAttempt.request_id, GenerationAttempt.attempt_no)
    ).all():
        attempts[attempt.request_id].append(attempt)
    admissions = {
        admission.request_id: admission
        for admission in session.scalars(
            select(GenerationAdmission).where(
                GenerationAdmission.request_id.in_(request_ids)
            )
        ).all()
    }
    payloads = {
        payload.request_id: payload
        for payload in session.scalars(
            select(GenerationRequestPayload).where(
                GenerationRequestPayload.request_id.in_(request_ids)
            )
        ).all()
    }
    result_applications: dict[
        int, tuple[GenerationResult, GenerationApplication]
    ] = {}
    for result, application in session.execute(
        select(GenerationResult, GenerationApplication)
        .join(
            GenerationApplication,
            GenerationApplication.result_id == GenerationResult.id,
        )
        .where(GenerationResult.request_id.in_(request_ids))
        .order_by(GenerationResult.request_id, GenerationResult.id.desc())
    ).all():
        result_applications.setdefault(result.request_id, (result, application))
    sources: dict[int, list[GenerationRequestSource]] = defaultdict(list)
    for source in session.scalars(
        select(GenerationRequestSource)
        .where(GenerationRequestSource.request_id.in_(request_ids))
        .order_by(
            GenerationRequestSource.request_id,
            GenerationRequestSource.source_id,
        )
    ).all():
        sources[source.request_id].append(source)
    source_ids = {
        source.source_id for request_sources in sources.values() for source in request_sources
    }
    current_sources = (
        {
            source.id: source
            for source in session.scalars(
                select(Source).where(Source.id.in_(source_ids))
            ).all()
        }
        if source_ids
        else {}
    )
    attempt_ids = [attempt.id for rows in attempts.values() for attempt in rows]
    runner_audits = (
        {
            audit.attempt_id: audit
            for audit in session.scalars(
                select(GenerationAttemptRunnerAudit).where(
                    GenerationAttemptRunnerAudit.attempt_id.in_(attempt_ids)
                )
            ).all()
        }
        if attempt_ids
        else {}
    )
    applications = [application for _result, application in result_applications.values()]
    synthesis_ids = {
        application.artifact_id
        for application in applications
        if application.artifact_type == "synthesis-version"
        and application.artifact_id is not None
    }
    evidence_ids = {
        application.artifact_id
        for application in applications
        if application.artifact_type == "evidence-review"
        and application.artifact_id is not None
    }
    artifact_uids = {
        ("synthesis-version", artifact_id): uid
        for artifact_id, uid in (
            session.execute(
                select(SynthesisVersion.id, SynthesisVersion.uid).where(
                    SynthesisVersion.id.in_(synthesis_ids)
                )
            ).all()
            if synthesis_ids
            else []
        )
    }
    artifact_uids.update(
        {
            ("evidence-review", artifact_id): uid
            for artifact_id, uid in (
                session.execute(
                    select(EvidenceReview.id, EvidenceReview.uid).where(
                        EvidenceReview.id.in_(evidence_ids)
                    )
                ).all()
                if evidence_ids
                else []
            )
        }
    )
    return GenerationTaskContext(
        control=current_generation_control(session),
        admissions=admissions,
        payloads=payloads,
        attempts=dict(attempts),
        result_applications=result_applications,
        sources=dict(sources),
        current_sources=current_sources,
        runner_audits=runner_audits,
        artifact_uids=artifact_uids,
    )


def generation_task_out(
    session: Session,
    request: GenerationRequest,
    context: GenerationTaskContext | None = None,
) -> GenerationTaskOut:
    attempts = (
        context.attempts.get(request.id, [])
        if context is not None
        else list(
            session.scalars(
                select(GenerationAttempt)
                .where(GenerationAttempt.request_id == request.id)
                .order_by(GenerationAttempt.attempt_no)
            ).all()
        )
    )
    attempt = attempts[-1] if attempts else None
    admission = (
        context.admissions.get(request.id)
        if context is not None
        else session.get(GenerationAdmission, request.id)
    ) or GenerationAdmission(
        request_id=request.id,
        approval_status="awaiting",
        admission_status="awaiting",
        admission_reason="",
    )
    control = context.control if context is not None else current_generation_control(session)
    payload = (
        context.payloads.get(request.id)
        if context is not None
        else session.get(GenerationRequestPayload, request.id)
    )
    input_tokens_estimated = (
        admission.input_tokens_estimated
        if admission.input_tokens_estimated is not None
        else estimate_generation_payload(payload.payload_json, control.input_estimator)
        if payload is not None and payload.payload_json is not None
        else None
    )
    output_tokens_reserved = (
        admission.output_tokens_reserved
        if admission.output_tokens_reserved is not None
        else control.output_reserve_tokens
        if payload is not None and payload.payload_json is not None
        else None
    )
    blocked_is_newer_than_attempt = admission.admission_status.startswith(
        "blocked_"
    ) and (
        attempt is None
        or (
            admission.updated_at is not None
            and attempt.updated_at is not None
            and utc_time(admission.updated_at) > utc_time(attempt.updated_at)
        )
    )
    visible_attempt = None if blocked_is_newer_than_attempt else attempt
    if blocked_is_newer_than_attempt:
        result_application = None
    elif context is not None:
        result_application = context.result_applications.get(request.id)
    else:
        result_application = generation_result_for_request(session, request.id)
    result, application = (
        result_application if result_application is not None else (None, None)
    )
    result_currency = (
        generation_result_currency_for_listing(
            attempt=attempt,
            result=result,
            application=application,
        )
        if context is not None
        else generation_result_currency(
            session,
            request=request,
            attempt=attempt,
            result=result,
            application=application,
        )
    )
    stale_result = result_currency == "stale"
    request_sources = (
        context.sources.get(request.id, [])
        if context is not None
        else generation_request_sources(session, request.id)
    )
    privacy_status, privacy_reason = generation_privacy_status(
        session,
        request,
        snapshots=request_sources,
        current_sources=(context.current_sources if context is not None else None),
    )
    can_reapply = bool(
        result is not None
        and application is not None
        and application.status in {"pending", "failed"}
        and result_currency == "current"
        and privacy_status != "blocked"
        and payload is not None
        and (
            payload.payload_json is not None
            or payload.application_context_json is not None
        )
    )
    retry_pending = admission.next_attempt_kind in {"automatic", "manual"}
    canceled = (
        admission.admission_status == "canceled"
        and not retry_pending
        and (attempt is None or attempt.status != "running")
    )
    if canceled:
        status = "canceled"
    elif blocked_is_newer_than_attempt:
        status = "blocked"
    elif attempt is None and privacy_status == "blocked":
        status = "blocked"
    elif (
        attempt is not None
        and attempt.status == "complete"
        and application is not None
        and application.status != "applied"
        and privacy_status == "blocked"
    ):
        status = "blocked"
    elif stale_result:
        status = "stale_result"
    elif visible_attempt is None or retry_pending:
        status = "pending"
    elif visible_attempt.status != "complete":
        status = "failed" if visible_attempt.status == "expired" else visible_attempt.status
    elif application is not None and application.status == "applied":
        status = "complete"
    elif application is not None and application.status == "failed":
        status = "apply_failed"
    elif application is not None and application.status == "pending":
        status = "apply_pending"
    else:
        status = "running"
    error = privacy_reason or (
        application.error
        if application is not None and application.error
        else visible_attempt.error
        if visible_attempt is not None
        else ""
    )
    sources = request_sources
    retry_count = sum(attempt.retry_kind != "initial" for attempt in attempts)
    runner_audits = (
        context.runner_audits
        if context is not None
        else {
            audit.attempt_id: audit
            for audit in session.scalars(
                select(GenerationAttemptRunnerAudit).where(
                    GenerationAttemptRunnerAudit.attempt_id.in_(
                        [row.id for row in attempts]
                    )
                )
            ).all()
        }
    )
    artifact_uid = None
    if (
        application is not None
        and application.artifact_type == "synthesis-version"
        and application.artifact_id is not None
    ):
        artifact_uid = (
            context.artifact_uids.get(
                (application.artifact_type, application.artifact_id)
            )
            if context is not None
            else session.scalar(
                select(SynthesisVersion.uid).where(
                    SynthesisVersion.id == application.artifact_id
                )
            )
        )
    elif (
        application is not None
        and application.artifact_type == "evidence-review"
        and application.artifact_id is not None
    ):
        artifact_uid = (
            context.artifact_uids.get(
                (application.artifact_type, application.artifact_id)
            )
            if context is not None
            else session.scalar(
                select(EvidenceReview.uid).where(
                    EvidenceReview.id == application.artifact_id
                )
            )
        )
    elif (
        application is not None
        and application.artifact_type == "report"
        and application.artifact_id is not None
    ):
        artifact_uid = request.target_uid
    return GenerationTaskOut(
        request_uid=request.uid,
        task_type=request.task_type,
        reason=request.reason,
        target_type=request.target_type,
        target_uid=request.target_uid,
        provider=request.provider,
        model=request.model,
        payload_retention=(
            "not_stored"
            if payload is None
            else "purged"
            if payload.payload_json is None
            else "retained"
        ),
        payload_purged_at=(
            utc_time(payload.purged_at) if payload is not None else None
        ),
        status=status,
        privacy_status=privacy_status,
        privacy_reason=privacy_reason,
        source_policy_fingerprint=request.source_policy_fingerprint,
        sources=[
            {
                "source_id": source.source_id,
                "source_name": source.source_name,
                "privacy_class": source.privacy_class,
                "external_generation_allowed": source.external_generation_allowed,
                "source_policy_version": source.source_policy_version,
            }
            for source in sources
        ],
        approval_status=admission.approval_status,
        admission_status=admission.admission_status,
        admission_reason=admission.admission_reason,
        input_tokens_estimated=input_tokens_estimated,
        output_tokens_reserved=output_tokens_reserved,
        application_status=(
            application.status if application is not None else "not_started"
        ),
        result_currency=result_currency,
        can_reapply=can_reapply,
        result_uid=result.uid if result is not None else None,
        result_fingerprint=(
            (result.output_fingerprint or result.payload_fingerprint)
            if result is not None
            else None
        ),
        result_schema_version=(
            (result.schema_version or request.schema_version)
            if result is not None
            else None
        ),
        apply_attempt_count=(
            application.apply_attempt_count if application is not None else 0
        ),
        last_apply_error=(application.last_error if application is not None else ""),
        artifact_type=(
            application.artifact_type
            if application is not None and application.artifact_type
            else None
        ),
        artifact_uid=artifact_uid,
        attempts=[
            {
                "attempt_uid": row.uid,
                "attempt_no": row.attempt_no,
                "status": row.status,
                "input_tokens": row.input_tokens_actual,
                "output_tokens": row.output_tokens_actual,
                "started_at": utc_time(row.started_at),
                "finished_at": utc_time(row.finished_at),
                "error": row.error,
                "runner_events_retention": (
                    "not_recorded"
                    if row.id not in runner_audits
                    else "purged"
                    if runner_audits[row.id].events_json is None
                    else "retained"
                ),
                "runner_events_purged_at": (
                    utc_time(runner_audits[row.id].purged_at)
                    if row.id in runner_audits
                    else None
                ),
            }
            for row in attempts
        ],
        input_tokens=(
            result.input_tokens
            if result is not None
            else visible_attempt.input_tokens_actual
            if visible_attempt is not None
            else None
        ),
        output_tokens=(
            result.output_tokens
            if result is not None
            else visible_attempt.output_tokens_actual
            if visible_attempt is not None
            else None
        ),
        retry_count=retry_count,
        failure_class=(visible_attempt.failure_class if visible_attempt is not None else None),
        cancel_requested=bool(
            visible_attempt is not None and visible_attempt.cancel_requested_at is not None
        ),
        created_at=utc_time(request.created_at),
        started_at=(
            utc_time(visible_attempt.started_at)
            if visible_attempt is not None
            else None
        ),
        finished_at=(
            utc_time(visible_attempt.finished_at)
            if visible_attempt is not None
            else None
        ),
        error=error,
    )


def generation_result_currency(
    session: Session,
    *,
    request: GenerationRequest,
    attempt: GenerationAttempt | None,
    result: GenerationResult | None,
    application: GenerationApplication | None,
) -> str:
    if result is None:
        return "none"
    if application is not None and application.status == "applied":
        return "current"
    if request.task_type == "event-synthesis" and request.target_type == "event":
        from .event_synthesis import (
            SynthesisValidationError,
            generation_synthesis_application_context,
        )

        try:
            context = generation_synthesis_application_context(
                session,
                request=request,
                result=result,
            )
        except SynthesisValidationError:
            return "stale"
        return "current" if context is not None else "stale"
    if request.task_type == "evidence-review" and request.target_type == "event":
        from .event_synthesis import (
            SynthesisValidationError,
            generation_evidence_review_application_context,
        )

        try:
            context = generation_evidence_review_application_context(
                session,
                request=request,
                result=result,
            )
        except SynthesisValidationError:
            return "stale"
        return "current" if context is not None else "stale"
    if request.task_type in {"item-summary", "cluster-synthesis"}:
        from .generation_producers import (
            GenerationProducerValidationError,
            producer_application_context,
        )

        try:
            context = producer_application_context(
                session, request=request, result=result
            )
        except GenerationProducerValidationError:
            return "stale"
        return "current" if context is not None else "stale"
    if request.task_type.startswith("report:") and request.target_type == "report":
        from .report_generation import (
            ReportValidationError,
            report_generation_application_context,
        )

        try:
            context = report_generation_application_context(
                session,
                request=request,
                result=result,
            )
        except ReportValidationError:
            return "stale"
        return "current" if context is not None else "stale"
    if attempt is None or result.attempt_id != attempt.id:
        return "stale"
    return "current"


def generation_result_currency_for_listing(
    *,
    attempt: GenerationAttempt | None,
    result: GenerationResult | None,
    application: GenerationApplication | None,
) -> str:
    if result is None:
        return "none"
    if application is not None and application.status == "applied":
        return "current"
    if attempt is None or result.attempt_id != attempt.id:
        return "stale"
    # Currentness depends on mutable target state and task-specific validation.
    # The bounded list query must not guess from localized errors or stale
    # snapshots; an explicit single-task read performs the exact validation.
    return "unverified"


def list_generation_tasks(
    session: Session,
    limit: int,
    offset: int = 0,
    before: GenerationRequest | None = None,
) -> list[GenerationTaskOut]:
    statement = select(GenerationRequest).order_by(
        GenerationRequest.created_at.desc(),
        GenerationRequest.id.desc(),
    )
    if before is not None:
        statement = statement.where(
            or_(
                GenerationRequest.created_at < before.created_at,
                and_(
                    GenerationRequest.created_at == before.created_at,
                    GenerationRequest.id < before.id,
                ),
            )
        )
    else:
        statement = statement.offset(offset)
    requests = session.scalars(statement.limit(limit)).all()
    return generation_tasks_out(session, list(requests))


def generation_tasks_out(
    session: Session, requests: list[GenerationRequest]
) -> list[GenerationTaskOut]:
    if not requests:
        return []
    context = generation_task_context(session, requests)
    return [generation_task_out(session, request, context) for request in requests]
