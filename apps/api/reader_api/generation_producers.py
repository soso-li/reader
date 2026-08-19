from __future__ import annotations

import json
import re
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from .content_filters import unfiltered_content_clause
from .digest import clean_preview, clean_title
from .event_synthesis import GenerationResultNotCurrentError
from .generation_lifecycle import (
    generation_result_for_request,
    latest_attempt,
    latest_generation_request,
    stable_hash,
)
from .models import (
    Cluster,
    ClusterItem,
    ContentItem,
    DELETED_SOURCE_STATUS,
    Document,
    GenerationApplication,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    RawEntry,
    Source,
    now_utc,
)
from .uninterested import ordinary_content_clause


ITEM_SUMMARY_TASK_TYPE = "item-summary"
ITEM_SUMMARY_PROMPT_VERSION = "item-summary-v1"
ITEM_SUMMARY_SCHEMA_VERSION = "item-summary-schema-v1"
CLUSTER_SYNTHESIS_TASK_TYPE = "cluster-synthesis"
CLUSTER_SYNTHESIS_PROMPT_VERSION = "cluster-synthesis-v4"
CLUSTER_SYNTHESIS_SCHEMA_VERSION = "cluster-synthesis-schema-v1"

ITEM_SUMMARY_SYSTEM_PROMPT = (
    "你是个人信息阅读器的摘要助手。用中文输出 3-5 条要点，保留关键实体和时间。"
    '只输出 JSON：{"summary":"摘要"}。'
)
CLUSTER_SYNTHESIS_SYSTEM_PROMPT = (
    "你是个人信息阅读器的事件合成助手。只输出 JSON，字段为 title、summary 和 content。"
    "title 是事件标题；summary 是 1-3 句短摘要，放在标题下方摘要框；content 是“后续补充”，不是完整重写。"
    "先选择时间最早、信息最完整或最接近原始报道的来源作为主来源；主来源原文会由界面单独展示，content 不要重复改写主来源全文。"
    "content 只写其它来源相对主来源新增的事实、时间、数字、背景或修正；如果其它来源没有明显新增信息，只简短说明没有新增信息。"
    "必须保留引用编号，不要编造来源没有给出的补充信息，不要按来源逐项对照。"
    "只使用输入里的标题、时间和来源原文，不补充背景、推断、评价或后续展望；来源原文很短时 content 也可以短。"
    "暂不合成图片，Markdown 图片行不要写入 content；图片由界面在各来源正文末尾去重展示。"
    "正文和摘要都用中文并保留 [1] 这类来源编号。"
)

ITEM_SUMMARY_OUTPUT_SCHEMA: dict[str, object] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["summary"],
    "properties": {"summary": {"type": "string", "minLength": 1}},
}
CLUSTER_SYNTHESIS_OUTPUT_SCHEMA: dict[str, object] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "summary", "content"],
    "properties": {
        "title": {"type": "string"},
        "summary": {"type": "string"},
        "content": {"type": "string", "minLength": 1},
    },
}


class GenerationProducerValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class PreparedGeneration:
    task_type: str
    target_type: str
    target_id: int
    target_uid: str
    prompt_version: str
    schema_version: str
    input_fingerprint: str
    source_ids: list[int]
    payload: dict[str, object]


def _citation(item: ContentItem, source: Source) -> dict[str, object]:
    return {
        "source_id": source.id,
        "source_name": source.name,
        "title": clean_title(
            item.title, clean_title(item.summary or item.content_text[:220])
        ),
        "url": item.url,
        "published_at": (
            item.published_at.isoformat() if item.published_at else None
        ),
    }


def prepare_item_summary(
    session: Session, item_id: int, *, reasoning_effort: str | None = None
) -> PreparedGeneration:
    row = session.execute(
        select(ContentItem, Document, RawEntry, Source)
        .join(Document, Document.id == ContentItem.document_id)
        .join(RawEntry, RawEntry.id == Document.raw_entry_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            ContentItem.id == item_id,
            Source.status != DELETED_SOURCE_STATUS,
        )
    ).first()
    if row is None:
        raise GenerationProducerValidationError("条目不存在")
    item, document, raw, source = row
    input_data: dict[str, object] = {
        "schema_version": ITEM_SUMMARY_SCHEMA_VERSION,
        "item_id": item.id,
        "item_content_hash": item.content_hash,
        "document_id": document.id,
        "raw_entry_id": raw.id,
        "raw_revision_no": raw.revision_no,
        "raw_payload_fingerprint": raw.payload_fingerprint,
        "document_content_fingerprint": stable_hash(
            {
                "title": document.title,
                "summary": document.summary,
                "content": document.content_text,
                "item_title": item.title,
                "item_content": item.content_text,
            }
        ),
        "source": _citation(item, source),
    }
    payload: dict[str, object] = {
        "system_prompt": ITEM_SUMMARY_SYSTEM_PROMPT,
        "input": (
            f"来源：{source.name}\n标题：{item.title}\n"
            f"时间：{item.published_at or '未知'}\n"
            f"RSS正文：{(item.content_text or '').strip()[:4000]}"
        ),
        "output_schema": ITEM_SUMMARY_OUTPUT_SCHEMA,
        "input_data": input_data,
    }
    if reasoning_effort is not None:
        payload["reasoning_effort"] = reasoning_effort
    return PreparedGeneration(
        task_type=ITEM_SUMMARY_TASK_TYPE,
        target_type="item",
        target_id=item.id,
        target_uid=str(item.id),
        prompt_version=ITEM_SUMMARY_PROMPT_VERSION,
        schema_version=ITEM_SUMMARY_SCHEMA_VERSION,
        input_fingerprint=stable_hash(input_data),
        source_ids=[source.id],
        payload=payload,
    )


def prepare_cluster_synthesis(
    session: Session, cluster_id: int, *, reasoning_effort: str | None = None
) -> PreparedGeneration:
    cluster = session.get(Cluster, cluster_id)
    if cluster is None:
        raise GenerationProducerValidationError("事件聚类不存在")
    rows = session.execute(
        select(ClusterItem, ContentItem, Document, RawEntry, Source)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Document, Document.id == ContentItem.document_id)
        .join(RawEntry, RawEntry.id == Document.raw_entry_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            ClusterItem.cluster_id == cluster_id,
            Source.status == "active",
            Source.media_type == "article",
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .order_by(ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
    ).all()
    if not rows:
        raise GenerationProducerValidationError("事件聚类没有可合成的来源")
    citations = [
        _citation(item, source)
        for _membership, item, _document, _raw, source in rows
    ]
    input_data: dict[str, object] = {
        "schema_version": CLUSTER_SYNTHESIS_SCHEMA_VERSION,
        "cluster_id": cluster.id,
        "cluster_key": cluster.cluster_key,
        "members": [
            {
                "cluster_item_id": membership.id,
                "item_id": item.id,
                "item_content_hash": item.content_hash,
                "item_content_fingerprint": stable_hash(
                    {
                        "title": item.title,
                        "summary": item.summary,
                        "content": item.content_text,
                    }
                ),
                "document_id": document.id,
                "raw_entry_id": raw.id,
                "raw_revision_no": raw.revision_no,
                "raw_payload_fingerprint": raw.payload_fingerprint,
                "source_id": source.id,
            }
            for membership, item, document, raw, source in rows
        ],
        "citations": citations,
    }
    payload: dict[str, object] = {
        "system_prompt": CLUSTER_SYNTHESIS_SYSTEM_PROMPT,
        "input": "\n\n".join(
            f"[{index}] 来源：{source.name}\n标题：{item.title}\n"
            f"时间：{item.published_at or '未知'}\n"
            f"来源原文：{(item.content_text or '').strip()}"
            for index, (_membership, item, _document, _raw, source) in enumerate(
                rows, 1
            )
        ),
        "output_schema": CLUSTER_SYNTHESIS_OUTPUT_SCHEMA,
        "input_data": input_data,
    }
    if reasoning_effort is not None:
        payload["reasoning_effort"] = reasoning_effort
    return PreparedGeneration(
        task_type=CLUSTER_SYNTHESIS_TASK_TYPE,
        target_type="cluster",
        target_id=cluster.id,
        target_uid=str(cluster.id),
        prompt_version=CLUSTER_SYNTHESIS_PROMPT_VERSION,
        schema_version=CLUSTER_SYNTHESIS_SCHEMA_VERSION,
        input_fingerprint=stable_hash(input_data),
        source_ids=sorted(
            {
                source.id
                for _membership, _item, _document, _raw, source in rows
            }
        ),
        payload=payload,
    )


def validate_producer_result(
    request: GenerationRequest,
    payload: dict[str, object],
    frozen_payload: dict[str, object],
) -> dict[str, object]:
    if request.task_type == ITEM_SUMMARY_TASK_TYPE:
        if set(payload) != {"summary"} or not isinstance(
            payload.get("summary"), str
        ):
            raise GenerationProducerValidationError("摘要输出结构无效")
        summary = str(payload["summary"]).strip()
        if not summary:
            raise GenerationProducerValidationError("模型未返回可用摘要")
        return {"summary": summary}
    if request.task_type != CLUSTER_SYNTHESIS_TASK_TYPE:
        raise GenerationProducerValidationError("生成任务类型不受支持")
    if set(payload) != {"title", "summary", "content"} or not all(
        isinstance(payload.get(field), str)
        for field in ("title", "summary", "content")
    ):
        raise GenerationProducerValidationError("合成输出结构无效")
    title = str(payload["title"]).strip()
    summary = str(payload["summary"]).strip()
    content = str(payload["content"]).strip()
    if not content:
        raise GenerationProducerValidationError("模型未返回可用合成全文")
    input_data = frozen_payload.get("input_data")
    citations = input_data.get("citations") if isinstance(input_data, dict) else None
    if not isinstance(citations, list) or not citations:
        raise GenerationProducerValidationError("合成任务缺少冻结引用")
    references = {
        int(value)
        for value in re.findall(r"\[(\d+)\]", f"{summary}\n{content}")
    }
    if not references or min(references) < 1 or max(references) > len(citations):
        raise GenerationProducerValidationError("合成结果包含无效引用")
    return {"title": title, "summary": summary, "content": content}


def producer_application_context(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
) -> tuple[ContentItem | Cluster, dict[str, object]] | None:
    if request.task_type == ITEM_SUMMARY_TASK_TYPE and request.target_type == "item":
        prepared = prepare_item_summary(session, request.target_id)
        target: ContentItem | Cluster | None = session.get(
            ContentItem, request.target_id
        )
    elif (
        request.task_type == CLUSTER_SYNTHESIS_TASK_TYPE
        and request.target_type == "cluster"
    ):
        prepared = prepare_cluster_synthesis(session, request.target_id)
        target = session.get(Cluster, request.target_id)
    else:
        return None
    latest_request = latest_generation_request(
        session,
        task_type=request.task_type,
        target_type=request.target_type,
        target_id=request.target_id,
        input_fingerprint=request.input_fingerprint,
    )
    attempt = latest_attempt(session, request.id)
    frozen = session.get(GenerationRequestPayload, request.id)
    frozen_payload: dict[str, object] | None = None
    if frozen is not None:
        if frozen.application_context_json is not None:
            frozen_payload = {"input_data": frozen.application_context_json}
        else:
            frozen_payload = frozen.payload_json
    if (
        target is None
        or latest_request is None
        or latest_request.id != request.id
        or attempt is None
        or attempt.id != result.attempt_id
        or prepared.input_fingerprint != request.input_fingerprint
        or request.prompt_version != prepared.prompt_version
        or request.schema_version != prepared.schema_version
        or frozen_payload is None
    ):
        return None
    validate_producer_result(request, result.payload_json, frozen_payload)
    return target, frozen_payload


def apply_producer_result(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
    application: GenerationApplication,
) -> None:
    context = producer_application_context(session, request=request, result=result)
    if context is None:
        raise GenerationResultNotCurrentError("生成结果已过期，不能应用")
    target, frozen_payload = context
    normalized = validate_producer_result(request, result.payload_json, frozen_payload)
    if isinstance(target, Cluster):
        input_data = frozen_payload["input_data"]
        assert isinstance(input_data, dict)
        citations = input_data["citations"]
        target.generated_title = str(normalized["title"]) or target.title
        target.generated_summary = str(normalized["summary"]) or clean_preview(
            str(normalized["content"]), 500
        )
        target.generated_content = str(normalized["content"])
        target.citations = json.dumps(citations, ensure_ascii=False)
        target.model_version = request.model
        target.prompt_version = request.prompt_version
        artifact_type = "cluster-synthesis"
    else:
        artifact_type = "item-summary"
    applied_at = now_utc()
    application.apply_attempt_count += 1
    application.status = "applied"
    application.artifact_type = artifact_type
    application.artifact_id = target.id
    application.error = ""
    application.applied_at = applied_at
    application.updated_at = applied_at
    session.flush()


def latest_item_summary_generation(
    session: Session, item_id: int
) -> tuple[GenerationRequest, GenerationResult | None, GenerationApplication | None] | None:
    request = session.scalar(
        select(GenerationRequest)
        .where(
            GenerationRequest.task_type == ITEM_SUMMARY_TASK_TYPE,
            GenerationRequest.target_type == "item",
            GenerationRequest.target_id == item_id,
        )
        .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
        .limit(1)
    )
    if request is None:
        return None
    result_application = generation_result_for_request(session, request.id)
    if result_application is None:
        return request, None, None
    return request, *result_application


def latest_applied_item_summary_generation(
    session: Session, item_id: int
) -> tuple[GenerationRequest, GenerationResult, GenerationApplication] | None:
    row = session.execute(
        select(GenerationRequest, GenerationResult, GenerationApplication)
        .join(GenerationResult, GenerationResult.request_id == GenerationRequest.id)
        .join(
            GenerationApplication,
            GenerationApplication.result_id == GenerationResult.id,
        )
        .where(
            GenerationRequest.task_type == ITEM_SUMMARY_TASK_TYPE,
            GenerationRequest.target_type == "item",
            GenerationRequest.target_id == item_id,
            GenerationApplication.status == "applied",
        )
        .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
        .limit(1)
    ).first()
    return (row[0], row[1], row[2]) if row is not None else None
