from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .event_synthesis import (
    EVIDENCE_REVIEW_TASK_TYPE,
    SYNTHESIS_TASK_TYPE,
    GenerationResultNotCurrentError,
    SynthesisValidationError,
    apply_generation_evidence_review_result,
    apply_generation_synthesis_result,
    fail_generation_application,
)
from .generation_lifecycle import (
    GenerationPrivacyError,
    assert_generation_request_eligible,
    lock_generation_lifecycle,
)
from .generation_producers import (
    CLUSTER_SYNTHESIS_TASK_TYPE,
    ITEM_SUMMARY_TASK_TYPE,
    GenerationProducerValidationError,
    apply_producer_result,
)
from .models import (
    Cluster,
    ContentItem,
    Event,
    GenerationApplication,
    GenerationRequest,
    GenerationResult,
)
from .report_generation import (
    ReportValidationError,
    apply_report_result,
    report_period,
)


class UnsupportedGenerationError(RuntimeError):
    pass


class GenerationApplicationError(RuntimeError):
    pass


class GenerationResultReplayConflict(RuntimeError):
    pass


def _lock_target(
    session: Session, request: GenerationRequest
) -> Event | ContentItem | Cluster | None:
    target: type[Event] | type[ContentItem] | type[Cluster]
    if (
        request.task_type in {SYNTHESIS_TASK_TYPE, EVIDENCE_REVIEW_TASK_TYPE}
        and request.target_type == "event"
    ):
        target = Event
    elif request.task_type == ITEM_SUMMARY_TASK_TYPE and request.target_type == "item":
        target = ContentItem
    elif (
        request.task_type == CLUSTER_SYNTHESIS_TASK_TYPE
        and request.target_type == "cluster"
    ):
        target = Cluster
    else:
        raise UnsupportedGenerationError("生成任务类型尚不支持重新应用")
    return session.scalar(
        select(target)
        .where(target.id == request.target_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def reapply_generation_result(
    session: Session,
    *,
    request: GenerationRequest,
    pending_only: bool = False,
) -> GenerationRequest:
    target = _lock_target(session, request)
    if target is None:
        raise SynthesisValidationError("Generation Request 的业务目标不存在")
    assert_generation_request_eligible(session, request, lock_sources=True)
    row = session.execute(
        select(GenerationResult, GenerationApplication)
        .join(
            GenerationApplication,
            GenerationApplication.result_id == GenerationResult.id,
        )
        .where(GenerationResult.request_id == request.id)
        .order_by(GenerationResult.id.desc())
        .with_for_update()
        .execution_options(populate_existing=True)
        .limit(1)
    ).first()
    if row is None:
        raise GenerationResultReplayConflict("生成结果不存在，不能重新应用")
    result, application = row
    if application.status == "applied":
        return request
    if pending_only and application.status != "pending":
        return request
    application_id = application.id
    request_id = request.id
    try:
        if isinstance(target, Event) and request.task_type == SYNTHESIS_TASK_TYPE:
            with session.begin_nested():
                apply_generation_synthesis_result(
                    session,
                    request=request,
                    result=result,
                    application=application,
                    event=target,
                )
        elif isinstance(target, Event):
            apply_generation_evidence_review_result(
                session,
                request=request,
                result=result,
                application=application,
                event=target,
            )
        else:
            with session.begin_nested():
                apply_producer_result(
                    session,
                    request=request,
                    result=result,
                    application=application,
                )
    except GenerationResultNotCurrentError as exc:
        fail_generation_application(session, application_id, str(exc))
        session.commit()
        raise
    except (
        SQLAlchemyError,
        SynthesisValidationError,
        GenerationProducerValidationError,
    ):
        fail_generation_application(
            session, application_id, "生成结果保存失败，请重试"
        )
        session.commit()
        raise GenerationApplicationError("生成结果保存失败，请重试") from None
    session.commit()
    refreshed = session.get(GenerationRequest, request_id)
    assert refreshed is not None
    return refreshed


def reapply_report_result(
    session: Session,
    *,
    request: GenerationRequest,
    pending_only: bool = False,
) -> GenerationRequest:
    if report_period(request.task_type) is None or request.target_type != "report":
        raise UnsupportedGenerationError("生成任务类型尚不支持重新应用")
    lock_generation_lifecycle(session)
    assert_generation_request_eligible(session, request, lock_sources=True)
    row = session.execute(
        select(GenerationResult, GenerationApplication)
        .join(
            GenerationApplication,
            GenerationApplication.result_id == GenerationResult.id,
        )
        .where(GenerationResult.request_id == request.id)
        .order_by(GenerationResult.id.desc())
        .with_for_update()
        .execution_options(populate_existing=True)
        .limit(1)
    ).first()
    if row is None:
        raise GenerationResultReplayConflict("生成结果不存在，不能重新应用")
    result, application = row
    if application.status == "applied":
        return request
    if pending_only and application.status != "pending":
        return request
    application_id = application.id
    request_id = request.id
    try:
        with session.begin_nested():
            apply_report_result(
                session,
                request=request,
                result=result,
                application=application,
            )
    except GenerationResultNotCurrentError as exc:
        fail_generation_application(session, application_id, str(exc))
        session.commit()
        raise
    except (SQLAlchemyError, ReportValidationError):
        fail_generation_application(
            session, application_id, "生成结果保存失败，请重试"
        )
        session.commit()
        raise GenerationApplicationError("生成结果保存失败，请重试") from None
    session.commit()
    refreshed = session.get(GenerationRequest, request_id)
    assert refreshed is not None
    return refreshed
