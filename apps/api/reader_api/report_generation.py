from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, case, false, func, or_, select
from sqlalchemy.orm import Session

from .digest import clean_title
from .content_filters import unfiltered_content_clause
from .event_projection import (
    cluster_current_event_state_projection,
    cluster_event_identities_for,
)
from .event_synthesis import GenerationResultNotCurrentError
from .generation_lifecycle import latest_attempt, stable_hash
from .uninterested import ordinary_content_clause
from .models import (
    Cluster,
    ClusterItem,
    ContentItem,
    Document,
    Event,
    EventRevision,
    EvidenceReview,
    GenerationApplication,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    Source,
    SynthesisBlock,
    SynthesisVersion,
    now_utc,
)


REPORT_PROMPT_VERSION = "report-v3"
REPORT_SCHEMA_VERSION = "report-schema-v4"
REPORT_SELECTION_RULE_VERSION = "event-material-update-report-selection-v3"
REPORT_SYSTEM_PROMPT = (
    "你是个人信息阅读器的报告助手。只输出 JSON，字段为 title 和 body；"
    "body 用中文，按要点分段；每个正文段落或列表项都必须用 [1] 这类编号引用冻结输入。"
)
REPORT_OUTPUT_SCHEMA: dict[str, object] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "title": {"type": "string", "maxLength": 500},
        "body": {"type": "string", "minLength": 1, "maxLength": 100000},
    },
    "required": ["title", "body"],
    "additionalProperties": False,
}
_CITATION_PATTERN = re.compile(r"\[(\d+)]")


class ReportValidationError(RuntimeError):
    pass


def _report_claims(body: str) -> list[str]:
    claims: list[str] = []
    current: list[str] = []
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if (
            not line
            or re.match(r"^#{1,6}\s+", line)
            or re.fullmatch(r"(?:[-*_]\s*){3,}", line)
        ):
            if current:
                claims.append(" ".join(current))
                current = []
            continue
        if re.match(r"^(?:[-+*]|\d+[.)])\s+", line) and current:
            claims.append(" ".join(current))
            current = []
        current.append(line)
    if current:
        claims.append(" ".join(current))
    return claims


def report_period(task_type: str) -> str | None:
    if not task_type.startswith("report:"):
        return None
    period = task_type.split(":", 1)[1]
    return period if period in {"day", "week", "month"} else None


def report_application_context(
    task_type: str,
    payload: dict[str, object],
) -> dict[str, object] | None:
    period = report_period(task_type)
    input_data = payload.get("input_data")
    if period is None or not isinstance(input_data, dict):
        return None
    source_ids = input_data.get("source_ids")
    items = input_data.get("items")
    citations = (
        [
            {
                "cluster_id": item.get("cluster_id"),
                "event_uid": item.get("event_uid"),
                "event_revision_uid": item.get("event_revision_uid"),
            }
            for item in items
            if isinstance(item, dict)
        ]
        if isinstance(items, list)
        else []
    )
    context = {
        "period": period,
        "start": input_data.get("start"),
        "end": input_data.get("end"),
        "source_ids": source_ids,
        "provenance_complete": input_data.get("provenance_complete"),
        "input_fingerprint": input_data.get("input_fingerprint"),
        "citations": citations,
    }
    if (
        not all(
            isinstance(context[key], str) and str(context[key]).strip()
            for key in ("start", "end", "input_fingerprint")
        )
        or not isinstance(source_ids, list)
        or not all(isinstance(value, int) and not isinstance(value, bool) for value in source_ids)
        or not isinstance(context["provenance_complete"], bool)
        or not isinstance(items, list)
        or len(citations) != len(items)
        or any(
            not isinstance(citation["cluster_id"], int)
            or not isinstance(citation["event_uid"], str)
            or not citation["event_uid"].strip()
            or not isinstance(citation["event_revision_uid"], str)
            or not citation["event_revision_uid"].strip()
            for citation in citations
        )
    ):
        return None
    return context


def report_clusters(
    session: Session, start: datetime, end: datetime
) -> list[Cluster]:
    current_event_state = cluster_current_event_state_projection(session)
    window_material = (
        select(
            EvidenceReview.event_id.label("event_id"),
            func.max(EventRevision.created_at).label("material_update_at"),
        )
        .join(EventRevision, EventRevision.id == EvidenceReview.target_revision_id)
        .where(
            EvidenceReview.result == "material",
            EventRevision.created_at >= start,
            EventRevision.created_at < end,
        )
        .group_by(EvidenceReview.event_id)
        .subquery()
    )
    material_update_at = window_material.c.material_update_at
    report_at = case(
        (
            or_(
                Cluster.first_seen_at.is_(None),
                material_update_at > Cluster.first_seen_at,
            ),
            material_update_at,
        ),
        else_=Cluster.first_seen_at,
    ).label("report_at")
    effective_read_status = case(
        (
            current_event_state.c.material_update_revision_uid.is_not(None),
            "unread",
        ),
        else_=func.coalesce(current_event_state.c.read_status, "unread"),
    )
    return list(
        session.scalars(
            select(Cluster, report_at)
            .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
            .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
            .join(Source, Source.id == ContentItem.source_id)
            .join(
                current_event_state,
                current_event_state.c.cluster_id == Cluster.id,
                isouter=True,
            )
            .join(
                window_material,
                window_material.c.event_id == current_event_state.c.event_id,
                isouter=True,
            )
            .where(
                or_(
                    and_(
                        Cluster.first_seen_at >= start,
                        Cluster.first_seen_at < end,
                    ),
                    material_update_at.is_not(None),
                )
            )
            .where(
                Source.status == "active",
                Source.media_type == "article",
                unfiltered_content_clause(ContentItem.id),
                ordinary_content_clause(session, ContentItem.id),
                func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            )
            .where(effective_read_status != "dismissed")
            .distinct()
            .order_by(report_at.desc().nullslast(), Cluster.id.desc())
            .limit(30)
        ).all()
    )


def _cluster_documents(session: Session, cluster_id: int) -> list[dict[str, object]]:
    rows = session.execute(
        select(ContentItem, Document, Source)
        .join(Document, Document.id == ContentItem.document_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .where(
            ClusterItem.cluster_id == cluster_id,
            Source.status == "active",
            Source.media_type == "article",
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .order_by(ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
        .limit(8)
    ).all()
    return [
        {
            "content_item_id": item.id,
            "document_id": document.id,
            "raw_entry_id": document.raw_entry_id,
            "source_id": source.id,
            "source_name": source.name,
            "title": clean_title(item.title, clean_title(item.summary or item.content_text[:220])),
            "url": item.url,
            "published_at": (
                _legacy_report_timestamp(item.published_at)
                if item.published_at
                else None
            ),
        }
        for item, document, source in rows
    ]


def _current_event_report_text(
    session: Session,
    cluster_ids: list[int],
) -> dict[int, tuple[str, str]]:
    if not cluster_ids:
        return {}
    current_event_state = cluster_current_event_state_projection(session)
    rows = session.execute(
        select(
            current_event_state.c.cluster_id,
            EventRevision.title_snapshot,
            SynthesisBlock.body,
        )
        .select_from(current_event_state)
        .join(Event, Event.id == current_event_state.c.event_id)
        .join(
            EventRevision,
            EventRevision.id == current_event_state.c.current_revision_id,
        )
        .outerjoin(
            SynthesisVersion,
            and_(
                SynthesisVersion.id == Event.current_synthesis_version_id,
                SynthesisVersion.target_revision_id
                == current_event_state.c.current_revision_id,
            ),
        )
        .outerjoin(
            SynthesisBlock,
            and_(
                SynthesisBlock.synthesis_version_id == SynthesisVersion.id,
                SynthesisBlock.kind == "summary",
            ),
        )
        .where(current_event_state.c.cluster_id.in_(cluster_ids))
        .order_by(current_event_state.c.cluster_id, SynthesisBlock.position)
    ).all()
    values: dict[int, tuple[str, str]] = {}
    for cluster_id, title, summary in rows:
        current_title, current_summary = values.get(cluster_id, ("", ""))
        values[cluster_id] = (
            current_title or str(title or ""),
            current_summary or str(summary or ""),
        )
    return values


def freeze_report_input(
    session: Session,
    *,
    period: str,
    start: datetime,
    end: datetime,
    clusters: list[Cluster],
    source_ids: list[int],
    provenance_complete: bool,
) -> dict[str, object]:
    identities = cluster_event_identities_for(
        session, [cluster.id for cluster in clusters]
    )
    event_text = _current_event_report_text(
        session, [cluster.id for cluster in clusters]
    )
    items: list[dict[str, object]] = []
    for citation_no, cluster in enumerate(clusters, 1):
        identity = identities.get(cluster.id)
        if identity is None:
            raise ReportValidationError("报告入选 Cluster 缺少 Event/Revision 映射")
        documents = _cluster_documents(session, cluster.id)
        if not documents:
            raise ReportValidationError("报告入选事件缺少可验证来源")
        event_title, event_summary = event_text.get(cluster.id, ("", ""))
        items.append(
            {
                "citation_no": citation_no,
                "cluster_id": cluster.id,
                "event_uid": identity.event_uid,
                "event_revision_uid": identity.current_revision_uid,
                "title": clean_title(
                    event_title or cluster.generated_title or cluster.title
                ),
                "summary": (
                    event_summary
                    or event_title
                    or cluster.title
                ),
                "first_seen_at": (
                    _legacy_report_timestamp(cluster.first_seen_at)
                    if cluster.first_seen_at
                    else None
                ),
                "last_seen_at": (
                    _legacy_report_timestamp(cluster.last_seen_at)
                    if cluster.last_seen_at
                    else None
                ),
                "documents": documents,
            }
        )
    frozen: dict[str, object] = {
        "period": period,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "selection": {
            "rule_version": REPORT_SELECTION_RULE_VERSION,
            "limit": 30,
            "order": "event-first-seen-or-material-revision-at-desc",
            "source_status": "active",
            "media_type": "article",
            "exclude_read_status": "dismissed",
        },
        "items": items,
        "source_ids": source_ids,
        "provenance_complete": provenance_complete,
    }
    frozen["input_fingerprint"] = stable_hash(frozen)
    return frozen


def _legacy_report_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def report_model_request(input_data: dict[str, object]) -> dict[str, object]:
    items = input_data.get("items")
    if not isinstance(items, list) or not items:
        raise ReportValidationError("报告冻结输入为空")
    input_text = "\n\n".join(
        f"[{item['citation_no']}] {item['title']}\n"
        f"时间：{item['first_seen_at'] or '未知'}\n摘要：{item['summary']}"
        for item in items
        if isinstance(item, dict)
    )
    return {
        "system_prompt": REPORT_SYSTEM_PROMPT,
        "input": input_text,
        "output_schema": REPORT_OUTPUT_SCHEMA,
        "input_data": input_data,
    }


def validate_report_output(
    payload: dict[str, object], input_data: dict[str, object]
) -> dict[str, object]:
    if set(payload) != {"title", "body"}:
        raise ReportValidationError("报告结果字段无效")
    title = payload.get("title")
    body = payload.get("body")
    if not isinstance(title, str) or not isinstance(body, str) or not body.strip():
        raise ReportValidationError("生成结果缺少报告正文")
    if len(title) > 500 or len(body) > 100000:
        raise ReportValidationError("报告结果超过长度限制")
    items = input_data.get("items")
    if not isinstance(items, list) or not items:
        raise ReportValidationError("报告冻结输入为空")
    cited = list(dict.fromkeys(int(value) for value in _CITATION_PATTERN.findall(body)))
    if not cited:
        raise ReportValidationError("报告正文缺少来源引用")
    if any(number < 1 or number > len(items) for number in cited):
        raise ReportValidationError("报告引用不属于冻结输入")
    if any(not _CITATION_PATTERN.search(claim) for claim in _report_claims(body)):
        raise ReportValidationError("报告每个正文段落或列表项都必须包含来源引用")
    citations = []
    cluster_ids = []
    for number in cited:
        item = items[number - 1]
        if not isinstance(item, dict):
            raise ReportValidationError("报告冻结输入格式无效")
        documents = item.get("documents")
        if not isinstance(documents, list) or not documents:
            raise ReportValidationError("报告冻结来源为空")
        cluster_id = item.get("cluster_id")
        if not isinstance(cluster_id, int):
            raise ReportValidationError("报告冻结 Cluster 引用无效")
        event_uid = item.get("event_uid")
        event_revision_uid = item.get("event_revision_uid")
        if (
            not isinstance(event_uid, str)
            or not event_uid.strip()
            or not isinstance(event_revision_uid, str)
            or not event_revision_uid.strip()
        ):
            raise ReportValidationError("报告冻结 Event/Revision 引用无效")
        cluster_ids.append(cluster_id)
        citations.append(
            {
                "citation_no": number,
                "cluster_id": cluster_id,
                "event_uid": event_uid,
                "event_revision_uid": event_revision_uid,
                "title": str(item.get("title") or ""),
                "first_seen_at": item.get("first_seen_at"),
                "last_seen_at": item.get("last_seen_at"),
                "sources": [
                    {
                        "source_name": str(document.get("source_name") or ""),
                        "title": str(document.get("title") or ""),
                        "url": str(document.get("url") or ""),
                        "published_at": document.get("published_at"),
                    }
                    for document in documents
                    if isinstance(document, dict)
                ],
            }
        )
    return {
        "title": title.strip(),
        "body": body.strip(),
        "citations": citations,
        "cluster_ids": cluster_ids,
    }


def frozen_report_input(
    request: GenerationRequest, payload: dict[str, object]
) -> dict[str, object]:
    period = report_period(request.task_type)
    input_data = payload.get("input_data")
    try:
        start = datetime.fromisoformat(str(input_data["start"]))
        target_id = int(
            start.astimezone(timezone(timedelta(hours=8))).strftime("%Y%m%d")
        )
    except (KeyError, TypeError, ValueError):
        target_id = None
    if (
        period is None
        or request.target_type != "report"
        or target_id is None
        or request.target_id != target_id
        or request.target_uid != f"report:{period}:{target_id}"
        or payload.get("system_prompt") != REPORT_SYSTEM_PROMPT
        or payload.get("output_schema") != REPORT_OUTPUT_SCHEMA
        or not isinstance(input_data, dict)
        or input_data.get("period") != period
        or input_data.get("input_fingerprint") != request.input_fingerprint
        or stable_hash(
            {key: value for key, value in input_data.items() if key != "input_fingerprint"}
        )
        != request.input_fingerprint
    ):
        raise ReportValidationError("Generation Request 的报告冻结输入无效")
    return input_data


def report_generation_application_context(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
) -> tuple[str, datetime, datetime, dict[str, object]] | None:
    frozen = session.get(GenerationRequestPayload, request.id)
    if frozen is None:
        raise ReportValidationError("Generation Request 的冻结 payload 不存在")
    period = report_period(request.task_type)
    if period is None:
        raise ReportValidationError("Generation Request 的报告类型无效")
    if frozen.payload_json is not None:
        input_data = frozen_report_input(request, frozen.payload_json)
        retained = report_application_context(request.task_type, frozen.payload_json)
    else:
        input_data = None
        retained = frozen.application_context_json
    if (
        not isinstance(retained, dict)
        or retained.get("period") != period
        or retained.get("input_fingerprint") != request.input_fingerprint
        or not isinstance(retained.get("source_ids"), list)
        or not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in retained["source_ids"]
        )
        or not isinstance(retained.get("provenance_complete"), bool)
    ):
        raise ReportValidationError("Generation Request 的报告应用上下文无效")
    try:
        start = datetime.fromisoformat(str(retained["start"]))
        end = datetime.fromisoformat(str(retained["end"]))
        target_id = int(
            start.astimezone(timezone(timedelta(hours=8))).strftime("%Y%m%d")
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ReportValidationError("报告冻结时间范围无效") from exc
    if (
        start.tzinfo is None
        or end.tzinfo is None
        or request.target_id != target_id
        or request.target_uid != f"report:{period}:{target_id}"
    ):
        raise ReportValidationError("Generation Request 的报告应用上下文无效")
    latest_request = session.scalar(
        select(GenerationRequest)
        .where(
            GenerationRequest.task_type == request.task_type,
            GenerationRequest.target_type == "report",
            GenerationRequest.target_id == request.target_id,
        )
        .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
        .limit(1)
    )
    current_attempt = latest_attempt(session, request.id)
    current_clusters = report_clusters(session, start, end)
    current_input = freeze_report_input(
        session,
        period=period,
        start=start,
        end=end,
        clusters=current_clusters,
        source_ids=list(retained["source_ids"]),
        provenance_complete=retained["provenance_complete"],
    )
    if (
        latest_request is None
        or latest_request.id != request.id
        or current_attempt is None
        or current_attempt.id != result.attempt_id
        or current_input["input_fingerprint"] != request.input_fingerprint
    ):
        return None
    canonical = validate_report_output(
        {
            "title": result.payload_json.get("title"),
            "body": result.payload_json.get("body"),
        },
        input_data or current_input,
    )
    if result.payload_json != canonical:
        raise ReportValidationError("Generation Result 的报告内容无效")
    return period, start, end, input_data or current_input


def apply_report_result(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
    application: GenerationApplication,
) -> None:
    context = report_generation_application_context(
        session, request=request, result=result
    )
    if context is None:
        raise GenerationResultNotCurrentError("生成结果已过期，不能应用")
    period, start, _end, _input_data = context
    prefix = {"day": 1, "week": 2, "month": 3}[period]
    report_date = int(
        start.astimezone(timezone(timedelta(hours=8))).strftime("%Y%m%d")
    )
    applied_at = now_utc()
    application.apply_attempt_count += 1
    application.status = "applied"
    application.artifact_type = "report"
    application.artifact_id = prefix * 100000000 + report_date
    application.error = ""
    application.applied_at = applied_at
    application.updated_at = applied_at
    session.flush()
