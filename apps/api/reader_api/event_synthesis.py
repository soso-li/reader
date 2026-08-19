from __future__ import annotations

from dataclasses import dataclass
import json
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, undefer

from .content_filters import unfiltered_content_clause
from .generation_lifecycle import (
    generation_application_context,
    generation_task_out,
    latest_attempt,
    latest_generation_request,
    stable_hash,
)
from .models import (
    EvidenceReview,
    EvidenceReviewCitation,
    EvidenceSnapshot,
    EvidenceSnapshotMember,
    Event,
    EventEvidence,
    EventEvidenceVersion,
    EventRevision,
    EventRevisionEvidence,
    GenerationApplication,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    LLMTask,
    Source,
    SynthesisBlock,
    SynthesisCitation,
    SynthesisVersion,
    SYNTHESIS_BLOCK_KINDS,
    SYNTHESIS_CITATION_SIDE_MAX_LENGTH,
    now_utc,
)
from .schemas import (
    EventEvidenceSourceOut,
    EventSynthesisFreshnessOut,
    EventSynthesisStateOut,
    GenerationTaskOut,
    SynthesisBlockOut,
    SynthesisCitationOut,
    SynthesisVersionOut,
)
from .translations import llm_text, unfence_json


SYNTHESIS_TASK_TYPE = "event-synthesis"
EVIDENCE_REVIEW_TASK_TYPE = "evidence-review"
SYNTHESIS_POLICY_VERSION = "event-synthesis-policy-v2"
SYNTHESIS_PROMPT_VERSION = "event-synthesis-prompt-v2"
SYNTHESIS_COMPATIBLE_PROMPT_VERSIONS = frozenset(
    {"event-synthesis-prompt-v1", "event-synthesis-prompt-v2"}
)
SYNTHESIS_SCHEMA_VERSION = "event-synthesis-schema-v1"
EVIDENCE_REVIEW_POLICY_VERSION = "evidence-review-policy-v1"
EVIDENCE_REVIEW_PROMPT_VERSION = "evidence-review-prompt-v1"
EVIDENCE_REVIEW_SCHEMA_VERSION = "evidence-review-schema-v1"
SYNTHESIS_SYSTEM_PROMPT = (
    "你是个人信息阅读器的事件合成助手。只使用输入证据，输出严格 JSON。"
    "结果字段 blocks 是有序数组；kind 只允许 summary、fact、viewpoint、"
    "disagreement、uncertainty，body 使用中文。每个 block 必须引用输入中的"
    " evidence_version_uid；viewpoint 必须填写 attribution；disagreement 必须用"
    "不同 side 分别引用至少两份冲突证据。顶层只允许 blocks；每个 block 只允许"
    " kind、body、attribution、citations；每个 citation 只允许"
    " evidence_version_uid、side。不得输出任何其他字段，不得补充输入外事实。"
)
EVIDENCE_REVIEW_SYSTEM_PROMPT = (
    "你是个人信息阅读器的证据审阅助手。只比较输入中的基准证据和目标证据，"
    "输出严格 JSON。result 只允许 ordinary、material、uncertain；ordinary 表示"
    "新增证据仅佐证既有事实，不改变核心事实、可信度、状态、影响或重要分歧。"
    "reason 使用中文，citations 必须引用目标快照内的 evidence_version_uid。"
    "material 必须在同一结果的 synthesis 字段返回符合合成稿 schema 的 blocks；"
    "ordinary 与 uncertain 不得返回 synthesis。"
)
@dataclass(frozen=True)
class SynthesisEvidenceSource:
    id: int
    name: str
    media_type: str


SynthesisEvidenceRow = tuple[
    EventRevisionEvidence,
    EventEvidenceVersion,
    EventEvidence,
    SynthesisEvidenceSource,
]
SynthesisEvidenceKey = tuple[int, str, str]
SynthesisEvidenceCoverage = tuple[set[SynthesisEvidenceKey], set[int]]
SYNTHESIS_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "required": ["blocks"],
    "additionalProperties": False,
    "properties": {
        "blocks": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["kind", "body", "citations"],
                "additionalProperties": False,
                "properties": {
                    "kind": {"enum": list(SYNTHESIS_BLOCK_KINDS)},
                    "body": {"type": "string", "minLength": 1},
                    "attribution": {"type": "string"},
                    "citations": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "required": ["evidence_version_uid", "side"],
                            "additionalProperties": False,
                            "properties": {
                                "evidence_version_uid": {"type": "string"},
                                "side": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": SYNTHESIS_CITATION_SIDE_MAX_LENGTH,
                                },
                            },
                        },
                    },
                },
            },
        }
    },
}
EVIDENCE_REVIEW_OUTPUT_SCHEMA: dict[str, object] = {
    "type": "object",
    "required": ["result", "reason", "citations"],
    "additionalProperties": False,
    "properties": {
        "result": {"enum": ["ordinary", "material", "uncertain"]},
        "reason": {"type": "string", "minLength": 1},
        "citations": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["evidence_version_uid"],
                "additionalProperties": False,
                "properties": {"evidence_version_uid": {"type": "string"}},
            },
        },
        "synthesis": SYNTHESIS_OUTPUT_SCHEMA,
    },
}


class SynthesisValidationError(ValueError):
    pass


class GenerationResultNotCurrentError(SynthesisValidationError):
    pass


def synthesis_version_is_compatible(
    version: SynthesisVersion, snapshot: EvidenceSnapshot
) -> bool:
    compatible_prompt_versions = (
        SYNTHESIS_COMPATIBLE_PROMPT_VERSIONS
        if SYNTHESIS_PROMPT_VERSION in SYNTHESIS_COMPATIBLE_PROMPT_VERSIONS
        else frozenset({SYNTHESIS_PROMPT_VERSION})
    )
    return (
        version.prompt_version in compatible_prompt_versions
        and version.schema_version == SYNTHESIS_SCHEMA_VERSION
        and snapshot.policy_version == SYNTHESIS_POLICY_VERSION
    )


def synthesis_evidence_input(
    *,
    evidence: EventEvidence,
    version: EventEvidenceVersion,
    source: SynthesisEvidenceSource,
    evidence_type: str,
    role: str,
) -> dict[str, object]:
    return {
        "evidence_uid": evidence.uid,
        "evidence_version_uid": version.uid,
        "evidence_type": evidence_type,
        "role": role,
        "source_id": source.id,
        "source_name": source.name,
        "media_type": source.media_type,
        "title": version.title_snapshot,
        "url": version.url_snapshot,
        "author": version.author_snapshot,
        "published_at": (
            version.published_at_snapshot.isoformat()
            if version.published_at_snapshot
            else None
        ),
        "content": version.content_snapshot,
    }


def create_evidence_snapshot(
    session: Session, event: Event, revision: EventRevision
) -> tuple[EvidenceSnapshot, list[dict[str, object]], int, str]:
    rows = synthesis_evidence_rows(session, revision)
    source_count, source_coverage_fingerprint, content_fingerprint = (
        synthesis_evidence_fingerprints(rows)
    )
    if source_count < 2:
        raise SynthesisValidationError("单一来源内容直接使用来源视图，无需生成合成稿")
    snapshot = session.scalar(
        select(EvidenceSnapshot).where(
            EvidenceSnapshot.event_id == event.id,
            EvidenceSnapshot.target_revision_id == revision.id,
            EvidenceSnapshot.source_coverage_fingerprint
            == source_coverage_fingerprint,
            EvidenceSnapshot.content_fingerprint == content_fingerprint,
            EvidenceSnapshot.policy_version == SYNTHESIS_POLICY_VERSION,
        )
    )
    snapshot_is_new = snapshot is None
    if snapshot is None:
        snapshot = EvidenceSnapshot(
            uid=str(uuid4()),
            event_id=event.id,
            target_revision_id=revision.id,
            source_coverage_fingerprint=source_coverage_fingerprint,
            content_fingerprint=content_fingerprint,
            policy_version=SYNTHESIS_POLICY_VERSION,
        )
        session.add(snapshot)
        session.flush()
    evidence: list[dict[str, object]] = []
    for position, (link, version, event_evidence, source) in enumerate(rows, 1):
        if snapshot_is_new:
            session.add(
                EvidenceSnapshotMember(
                    snapshot_id=snapshot.id,
                    target_revision_id=revision.id,
                    evidence_version_id=version.id,
                    evidence_type=link.evidence_type,
                    role=link.role,
                    position=position,
                )
            )
        evidence.append(
            synthesis_evidence_input(
                evidence=event_evidence,
                version=version,
                source=source,
                evidence_type=link.evidence_type,
                role=link.role,
            )
        )
    session.flush()
    generation_fingerprint = synthesis_generation_fingerprint(
        source_coverage_fingerprint,
        content_fingerprint,
    )
    return snapshot, evidence, source_count, generation_fingerprint


def synthesis_generation_for_revision(
    session: Session, revision: EventRevision
) -> tuple[int, str]:
    (
        source_count,
        _source_fingerprint,
        _content_fingerprint,
        generation_fingerprint,
    ) = synthesis_fingerprints_for_rows(synthesis_evidence_rows(session, revision))
    if generation_fingerprint is None:
        raise SynthesisValidationError("单一来源内容直接使用来源视图，无需生成合成稿")
    return source_count, generation_fingerprint


def synthesis_source_ids_for_revision(
    session: Session, revision: EventRevision
) -> list[int]:
    return sorted(
        {
            version.source_id
            for _link, version, _evidence, _source in synthesis_evidence_rows(
                session, revision
            )
        }
    )


def synthesis_fingerprints_for_rows(
    rows: list[SynthesisEvidenceRow],
) -> tuple[int, str, str, str | None]:
    source_count, source_fingerprint, content_fingerprint = (
        synthesis_evidence_fingerprints(rows)
    )
    if source_count < 2:
        return source_count, source_fingerprint, content_fingerprint, None
    return (
        source_count,
        source_fingerprint,
        content_fingerprint,
        synthesis_generation_fingerprint(source_fingerprint, content_fingerprint),
    )


def canonical_synthesis_evidence_rows(
    rows: list[SynthesisEvidenceRow],
) -> list[SynthesisEvidenceRow]:
    return sorted(
        rows,
        key=lambda row: (
            row[2].identity_fingerprint,
            row[1].version_fingerprint,
            row[0].evidence_type,
            row[0].role,
        ),
    )


def snapshot_evidence_coverage_for(
    session: Session, snapshot_ids: list[int]
) -> dict[int, SynthesisEvidenceCoverage]:
    coverage: dict[int, SynthesisEvidenceCoverage] = {
        snapshot_id: (set(), set()) for snapshot_id in snapshot_ids
    }
    if not snapshot_ids:
        return coverage
    for snapshot_id, version_id, evidence_type, role, source_id in session.execute(
        select(
            EvidenceSnapshotMember.snapshot_id,
            EvidenceSnapshotMember.evidence_version_id,
            EvidenceSnapshotMember.evidence_type,
            EvidenceSnapshotMember.role,
            EventEvidenceVersion.source_id,
        )
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id == EvidenceSnapshotMember.evidence_version_id,
        )
        .where(EvidenceSnapshotMember.snapshot_id.in_(snapshot_ids))
    ):
        keys, source_ids = coverage[snapshot_id]
        keys.add((version_id, evidence_type, role))
        source_ids.add(source_id)
    return coverage


def unreviewed_evidence_difference(
    rows: list[SynthesisEvidenceRow], coverage: SynthesisEvidenceCoverage
) -> tuple[int, int]:
    covered_keys, covered_source_ids = coverage
    uncovered = [
        row
        for row in rows
        if (row[0].evidence_version_id, row[0].evidence_type, row[0].role)
        not in covered_keys
    ]
    return (
        len(uncovered),
        len({version.source_id for _link, version, _evidence, _source in uncovered} - covered_source_ids),
    )


def synthesis_evidence_rows(
    session: Session, revision: EventRevision
) -> list[SynthesisEvidenceRow]:
    rows = session.execute(
        select(
            EventRevisionEvidence,
            EventEvidenceVersion,
            EventEvidence,
            Source.id,
            Source.name,
            Source.media_type,
        )
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id == EventRevisionEvidence.evidence_version_id,
        )
        .join(EventEvidence, EventEvidence.id == EventEvidenceVersion.evidence_id)
        .join(Source, Source.id == EventEvidenceVersion.source_id)
        .where(
            EventRevisionEvidence.revision_id == revision.id,
            Source.status == "active",
            unfiltered_content_clause(EventEvidenceVersion.legacy_content_item_id),
        )
    ).all()
    return canonical_synthesis_evidence_rows(
        [
            (
                link,
                version,
                evidence,
                SynthesisEvidenceSource(
                    id=source_id,
                    name=source_name,
                    media_type=media_type,
                ),
            )
            for link, version, evidence, source_id, source_name, media_type in rows
        ]
    )


def synthesis_snapshot_evidence(
    session: Session, snapshot: EvidenceSnapshot
) -> list[dict[str, object]]:
    rows = session.execute(
        select(
            EvidenceSnapshotMember,
            EventEvidenceVersion,
            EventEvidence,
            Source.id,
            Source.name,
            Source.media_type,
        )
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id == EvidenceSnapshotMember.evidence_version_id,
        )
        .join(EventEvidence, EventEvidence.id == EventEvidenceVersion.evidence_id)
        .join(Source, Source.id == EventEvidenceVersion.source_id)
        .where(EvidenceSnapshotMember.snapshot_id == snapshot.id)
        .order_by(EvidenceSnapshotMember.position)
    ).all()
    return [
        synthesis_evidence_input(
            evidence=evidence,
            version=version,
            source=SynthesisEvidenceSource(
                id=source_id,
                name=source_name,
                media_type=media_type,
            ),
            evidence_type=member.evidence_type,
            role=member.role,
        )
        for member, version, evidence, source_id, source_name, media_type in rows
    ]


def synthesis_evidence_fingerprints(
    rows: list[SynthesisEvidenceRow],
) -> tuple[int, str, str]:
    source_count = len(
        {version.source_id for _link, version, _evidence, _source in rows}
    )
    source_coverage_fingerprint = stable_hash(
        [
            [evidence.identity_fingerprint, link.evidence_type, link.role]
            for link, _version, evidence, _source in rows
        ]
    )
    content_fingerprint = stable_hash(
        [
            [
                evidence.identity_fingerprint,
                version.version_fingerprint,
                link.evidence_type,
                link.role,
            ]
            for link, version, evidence, _source in rows
        ]
    )
    return source_count, source_coverage_fingerprint, content_fingerprint


def synthesis_input_data(
    event: Event,
    revision: EventRevision,
    snapshot: EvidenceSnapshot,
    evidence: list[dict[str, object]],
    generation_fingerprint: str,
) -> dict[str, object]:
    return {
        "event_uid": event.uid,
        "target_revision_uid": revision.uid,
        "snapshot_uid": snapshot.uid,
        "policy_version": SYNTHESIS_POLICY_VERSION,
        "prompt_version": SYNTHESIS_PROMPT_VERSION,
        "schema_version": SYNTHESIS_SCHEMA_VERSION,
        "source_coverage_fingerprint": snapshot.source_coverage_fingerprint,
        "content_fingerprint": snapshot.content_fingerprint,
        "generation_fingerprint": generation_fingerprint,
        "evidence": evidence,
    }


def synthesis_model_request(input_data: dict[str, object]) -> dict[str, object]:
    return {
        "system_prompt": SYNTHESIS_SYSTEM_PROMPT,
        "input": json.dumps(input_data, ensure_ascii=False, separators=(",", ":")),
        "input_data": input_data,
        "output_schema": SYNTHESIS_OUTPUT_SCHEMA,
    }


def evidence_review_comparison_fingerprint(
    event: Event,
    baseline_revision: EventRevision,
    baseline_snapshot: EvidenceSnapshot,
    target_revision: EventRevision,
    target_snapshot: EvidenceSnapshot,
) -> str:
    return stable_hash(
        {
            "event_uid": event.uid,
            "baseline_revision_uid": baseline_revision.uid,
            "baseline_source_fingerprint": (
                baseline_snapshot.source_coverage_fingerprint
            ),
            "baseline_content_fingerprint": baseline_snapshot.content_fingerprint,
            "target_revision_uid": target_revision.uid,
            "target_source_fingerprint": target_snapshot.source_coverage_fingerprint,
            "target_content_fingerprint": target_snapshot.content_fingerprint,
            "policy_version": EVIDENCE_REVIEW_POLICY_VERSION,
            "prompt_version": EVIDENCE_REVIEW_PROMPT_VERSION,
            "schema_version": EVIDENCE_REVIEW_SCHEMA_VERSION,
        }
    )


def evidence_review_input_data(
    session: Session,
    event: Event,
    baseline_snapshot: EvidenceSnapshot,
    target_snapshot: EvidenceSnapshot,
) -> tuple[dict[str, object], int]:
    baseline_revision = session.get(EventRevision, baseline_snapshot.target_revision_id)
    target_revision = session.get(EventRevision, target_snapshot.target_revision_id)
    if baseline_revision is None or target_revision is None:
        raise SynthesisValidationError("Evidence Review 的 Revision 不存在")
    baseline_evidence = synthesis_snapshot_evidence(session, baseline_snapshot)
    target_evidence = synthesis_snapshot_evidence(session, target_snapshot)
    baseline_version_uids = {
        str(row["evidence_version_uid"]) for row in baseline_evidence
    }
    baseline_source_ids = {int(row["source_id"]) for row in baseline_evidence}
    new_evidence = [
        row
        for row in target_evidence
        if str(row["evidence_version_uid"]) not in baseline_version_uids
    ]
    new_source_count = len(
        {int(row["source_id"]) for row in new_evidence} - baseline_source_ids
    )
    comparison_fingerprint = evidence_review_comparison_fingerprint(
        event,
        baseline_revision,
        baseline_snapshot,
        target_revision,
        target_snapshot,
    )
    return (
        {
            "event_uid": event.uid,
            "baseline_snapshot_uid": baseline_snapshot.uid,
            "baseline_revision_uid": baseline_revision.uid,
            "target_snapshot_uid": target_snapshot.uid,
            "target_revision_uid": target_revision.uid,
            "policy_version": EVIDENCE_REVIEW_POLICY_VERSION,
            "prompt_version": EVIDENCE_REVIEW_PROMPT_VERSION,
            "schema_version": EVIDENCE_REVIEW_SCHEMA_VERSION,
            "comparison_fingerprint": comparison_fingerprint,
            "new_source_count": new_source_count,
            "baseline_evidence": baseline_evidence,
            "target_evidence": target_evidence,
            "new_evidence": new_evidence,
        },
        new_source_count,
    )


def evidence_review_model_request(
    input_data: dict[str, object],
) -> dict[str, object]:
    return {
        "system_prompt": EVIDENCE_REVIEW_SYSTEM_PROMPT,
        "input": json.dumps(input_data, ensure_ascii=False, separators=(",", ":")),
        "input_data": input_data,
        "output_schema": EVIDENCE_REVIEW_OUTPUT_SCHEMA,
    }


def immutable_evidence_review_provenance(
    input_data: dict[str, object],
) -> dict[str, object]:
    canonical = dict(input_data)
    for field in ("baseline_evidence", "target_evidence", "new_evidence"):
        rows = canonical.get(field)
        if isinstance(rows, list):
            canonical[field] = [
                {
                    key: value
                    for key, value in row.items()
                    if key not in {"source_name", "media_type"}
                }
                if isinstance(row, dict)
                else row
                for row in rows
            ]
    return canonical


def evidence_review_task_snapshots_from_request(
    session: Session,
    event: Event,
    request: dict[str, object],
) -> tuple[EvidenceSnapshot, EvidenceSnapshot, set[str], str, int]:
    input_data = request.get("input_data")
    if not isinstance(input_data, dict):
        raise SynthesisValidationError("Evidence Review 任务缺少 provenance")
    baseline_snapshot = session.scalar(
        select(EvidenceSnapshot).where(
            EvidenceSnapshot.uid
            == str(input_data.get("baseline_snapshot_uid") or "").strip()
        )
    )
    target_snapshot = session.scalar(
        select(EvidenceSnapshot).where(
            EvidenceSnapshot.uid
            == str(input_data.get("target_snapshot_uid") or "").strip()
        )
    )
    if (
        baseline_snapshot is None
        or target_snapshot is None
        or baseline_snapshot.event_id != event.id
        or target_snapshot.event_id != event.id
    ):
        raise SynthesisValidationError("Evidence Review 的 Snapshot 无效")
    expected = evidence_review_model_request(input_data)
    reasoning_effort = request.get("reasoning_effort")
    if reasoning_effort is not None:
        if not isinstance(reasoning_effort, str) or not reasoning_effort.strip():
            raise SynthesisValidationError("历史任务的 reasoning effort 无效")
        expected["reasoning_effort"] = reasoning_effort
    if request != expected:
        raise SynthesisValidationError("Evidence Review 任务的冻结请求不匹配")
    expected_input, new_source_count = evidence_review_input_data(
        session, event, baseline_snapshot, target_snapshot
    )
    if immutable_evidence_review_provenance(
        input_data
    ) != immutable_evidence_review_provenance(expected_input):
        raise SynthesisValidationError("Evidence Review 任务的冻结请求不匹配")
    allowed_version_uids = {
        str(row["evidence_version_uid"])
        for row in expected_input["target_evidence"]
        if isinstance(row, dict)
    }
    return (
        baseline_snapshot,
        target_snapshot,
        allowed_version_uids,
        str(expected_input["comparison_fingerprint"]),
        new_source_count,
    )


def validate_evidence_review_output(
    payload: dict[str, object], allowed_version_uids: set[str]
) -> tuple[str, str, list[str], dict[str, object] | None]:
    parsed = synthesis_payload(payload)
    if set(parsed) - {"result", "reason", "citations", "synthesis"} or not {
        "result",
        "reason",
        "citations",
    }.issubset(parsed):
        raise SynthesisValidationError("Evidence Review 包含未知或缺失字段")
    result = parsed.get("result")
    reason = parsed.get("reason")
    citations = parsed.get("citations")
    if result not in {"ordinary", "material", "uncertain"}:
        raise SynthesisValidationError("Evidence Review result 无效")
    if not isinstance(reason, str) or not reason.strip():
        raise SynthesisValidationError("Evidence Review reason 不能为空")
    if not isinstance(citations, list) or not citations:
        raise SynthesisValidationError("Evidence Review 必须引用目标证据")
    version_uids: list[str] = []
    for citation in citations:
        if not isinstance(citation, dict) or set(citation) != {
            "evidence_version_uid"
        }:
            raise SynthesisValidationError("Evidence Review citation 格式无效")
        version_uid = citation.get("evidence_version_uid")
        if not isinstance(version_uid, str) or version_uid not in allowed_version_uids:
            raise SynthesisValidationError(
                "Evidence Review 引用不属于目标 Evidence Snapshot"
            )
        if version_uid in version_uids:
            raise SynthesisValidationError("Evidence Review 不得重复引用同一证据")
        version_uids.append(version_uid)
    synthesis = parsed.get("synthesis")
    if synthesis is not None and not isinstance(synthesis, dict):
        raise SynthesisValidationError("Evidence Review synthesis 格式无效")
    if result != "material" and synthesis is not None:
        raise SynthesisValidationError("只有 material Review 可以返回新合成稿")
    return str(result), reason.strip(), version_uids, synthesis


def validated_event_generation_result(
    task_type: str,
    payload: dict[str, object],
    allowed_version_uids: set[str],
) -> tuple[dict[str, object], bool]:
    if task_type == SYNTHESIS_TASK_TYPE:
        return {
            "blocks": validate_synthesis_output(payload, allowed_version_uids)
        }, False
    if task_type != EVIDENCE_REVIEW_TASK_TYPE:
        raise SynthesisValidationError("Event Generation task type 无效")
    review_result, reason, citations, synthesis = validate_evidence_review_output(
        payload, allowed_version_uids
    )
    result: dict[str, object] = {
        "result": review_result,
        "reason": reason,
        "citations": [
            {"evidence_version_uid": version_uid} for version_uid in citations
        ],
    }
    if synthesis is not None:
        result["synthesis"] = {
            "blocks": validate_synthesis_output(
                synthesis, allowed_version_uids
            )
        }
    return result, review_result == "material" and synthesis is None


def artifact_revision_order(
    session: Session, event: Event, target_revision_id: int, artifact_id: int
) -> tuple[int, int]:
    revision = session.get(EventRevision, target_revision_id)
    if revision is None or revision.event_id != event.id:
        raise SynthesisValidationError("派生产物的目标 Revision 无效")
    return revision.revision_no, artifact_id


def advance_review_pointer(
    session: Session, event: Event, candidate: EvidenceReview
) -> bool:
    candidate_order = artifact_revision_order(
        session, event, candidate.target_revision_id, candidate.id
    )
    current = (
        session.get(EvidenceReview, event.reviewed_evidence_review_id)
        if event.reviewed_evidence_review_id is not None
        else None
    )
    if current is not None and candidate_order < artifact_revision_order(
        session, event, current.target_revision_id, current.id
    ):
        return False
    event.reviewed_evidence_review_id = candidate.id
    return True


def save_evidence_review(
    session: Session,
    *,
    event: Event,
    baseline_snapshot: EvidenceSnapshot,
    target_snapshot: EvidenceSnapshot,
    comparison_fingerprint: str,
    result: str,
    reason: str,
    cited_version_uids: list[str],
    provider: str,
    model: str,
) -> EvidenceReview:
    if result not in {"ordinary", "material", "uncertain"}:
        raise SynthesisValidationError("Evidence Review result 无效")
    existing = session.scalar(
        select(EvidenceReview).where(
            EvidenceReview.event_id == event.id,
            EvidenceReview.comparison_fingerprint == comparison_fingerprint,
        )
    )
    if existing is not None:
        existing_citations = list(
            session.scalars(
                select(EventEvidenceVersion.uid)
                .join(
                    EvidenceReviewCitation,
                    EvidenceReviewCitation.evidence_version_id
                    == EventEvidenceVersion.id,
                )
                .where(EvidenceReviewCitation.review_id == existing.id)
                .order_by(EvidenceReviewCitation.position)
            )
        )
        if (
            existing.result != result
            or existing.reason != reason
            or existing_citations != cited_version_uids
            or existing.provider != provider
            or existing.model != model
            or existing.policy_version != EVIDENCE_REVIEW_POLICY_VERSION
        ):
            raise SynthesisValidationError("Evidence Review 已完成且内容冲突")
        if existing.result == "ordinary":
            advance_review_pointer(session, event, existing)
        return existing
    member_rows = session.execute(
        select(EvidenceSnapshotMember, EventEvidenceVersion)
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id == EvidenceSnapshotMember.evidence_version_id,
        )
        .where(EvidenceSnapshotMember.snapshot_id == target_snapshot.id)
    ).all()
    members_by_uid = {
        version.uid: (member, version) for member, version in member_rows
    }
    review = EvidenceReview(
        uid=str(uuid4()),
        event_id=event.id,
        baseline_revision_id=baseline_snapshot.target_revision_id,
        baseline_snapshot_id=baseline_snapshot.id,
        target_revision_id=target_snapshot.target_revision_id,
        target_snapshot_id=target_snapshot.id,
        comparison_fingerprint=comparison_fingerprint,
        result=result,
        reason=reason,
        provider=provider,
        model=model,
        policy_version=EVIDENCE_REVIEW_POLICY_VERSION,
    )
    session.add(review)
    session.flush()
    for position, version_uid in enumerate(cited_version_uids, 1):
        member, version = members_by_uid[version_uid]
        session.add(
            EvidenceReviewCitation(
                review_id=review.id,
                target_snapshot_id=target_snapshot.id,
                evidence_version_id=version.id,
                evidence_type=member.evidence_type,
                role=member.role,
                position=position,
            )
        )
    if result == "ordinary":
        advance_review_pointer(session, event, review)
    session.flush()
    return review


def save_material_review_without_synthesis(
    session: Session,
    *,
    event: Event,
    request: dict[str, object],
    payload: dict[str, object],
    provider: str,
    model: str,
) -> bool:
    baseline, target, allowed_uids, fingerprint, _ = (
        evidence_review_task_snapshots_from_request(session, event, request)
    )
    result, reason, citations, synthesis = validate_evidence_review_output(
        payload, allowed_uids
    )
    if result != "material" or synthesis is not None:
        return False
    save_evidence_review(
        session,
        event=event,
        baseline_snapshot=baseline,
        target_snapshot=target,
        comparison_fingerprint=fingerprint,
        result=result,
        reason=reason,
        cited_version_uids=citations,
        provider=provider,
        model=model,
    )
    return True


def reject_material_review_without_synthesis(
    session: Session,
    *,
    event: Event,
    request: dict[str, object],
    payload: dict[str, object],
    provider: str,
    model: str,
) -> None:
    save_material_review_without_synthesis(
        session,
        event=event,
        request=request,
        payload=payload,
        provider=provider,
        model=model,
    )
    raise SynthesisValidationError(
        "material Review 必须在同一次结果中返回新合成稿"
    )


def synthesis_task_snapshot(
    session: Session,
    event: Event,
    input_data: dict[str, object],
) -> tuple[EvidenceSnapshot, set[str], int, str]:
    snapshot_uid = str(input_data.get("snapshot_uid") or "").strip()
    snapshot = session.scalar(
        select(EvidenceSnapshot).where(EvidenceSnapshot.uid == snapshot_uid)
    )
    if (
        snapshot is None
        or snapshot.event_id != event.id
        or snapshot.policy_version != SYNTHESIS_POLICY_VERSION
    ):
        raise SynthesisValidationError("历史任务的 Evidence Snapshot 无效")
    revision = session.get(EventRevision, snapshot.target_revision_id)
    if revision is None:
        raise SynthesisValidationError("历史任务的目标 Revision 不存在")
    expected_generation_fingerprint = synthesis_generation_fingerprint(
        snapshot.source_coverage_fingerprint,
        snapshot.content_fingerprint,
    )
    evidence = synthesis_snapshot_evidence(session, snapshot)
    expected_input_data = synthesis_input_data(
        event,
        revision,
        snapshot,
        evidence,
        expected_generation_fingerprint,
    )
    if input_data != expected_input_data:
        raise SynthesisValidationError("历史任务的合成 provenance 不匹配")
    allowed_version_uids = {
        str(row["evidence_version_uid"]) for row in evidence
    }
    source_count = len({int(row["source_id"]) for row in evidence})
    if source_count < 2:
        raise SynthesisValidationError("历史任务不包含足够的独立来源")
    return (
        snapshot,
        allowed_version_uids,
        source_count,
        expected_generation_fingerprint,
    )


def synthesis_task_snapshot_from_request(
    session: Session,
    event: Event,
    request: dict[str, object],
) -> tuple[EvidenceSnapshot, set[str], int, str]:
    input_data = request.get("input_data")
    if not isinstance(input_data, dict):
        raise SynthesisValidationError("历史合成任务缺少 provenance")
    snapshot = synthesis_task_snapshot(session, event, input_data)
    expected = synthesis_model_request(input_data)
    reasoning_effort = request.get("reasoning_effort")
    if reasoning_effort is not None:
        if not isinstance(reasoning_effort, str) or not reasoning_effort.strip():
            raise SynthesisValidationError("历史任务的 reasoning effort 无效")
        expected["reasoning_effort"] = reasoning_effort
    if request != expected:
        raise SynthesisValidationError("历史任务的冻结请求不匹配")
    return snapshot


def synthesis_task_snapshot_from_application_context(
    session: Session,
    event: Event,
    application_context: dict[str, object],
) -> tuple[EvidenceSnapshot, set[str], int, str]:
    snapshot_uid = application_context.get("snapshot_uid")
    if not isinstance(snapshot_uid, str) or not snapshot_uid.strip():
        raise SynthesisValidationError("Generation Request 的应用校验上下文无效")
    snapshot = session.scalar(
        select(EvidenceSnapshot).where(EvidenceSnapshot.uid == snapshot_uid)
    )
    if snapshot is None or snapshot.event_id != event.id:
        raise SynthesisValidationError("Generation Request 的 Evidence Snapshot 无效")
    revision = session.get(EventRevision, snapshot.target_revision_id)
    if revision is None:
        raise SynthesisValidationError("Generation Request 的目标 Revision 不存在")
    generation_fingerprint = synthesis_generation_fingerprint(
        snapshot.source_coverage_fingerprint,
        snapshot.content_fingerprint,
    )
    input_data = synthesis_input_data(
        event,
        revision,
        snapshot,
        synthesis_snapshot_evidence(session, snapshot),
        generation_fingerprint,
    )
    expected_context = generation_application_context(
        SYNTHESIS_TASK_TYPE,
        {"input_data": input_data},
    )
    if expected_context != application_context:
        raise SynthesisValidationError(
            "Generation Request 的应用校验上下文不匹配"
        )
    return synthesis_task_snapshot(session, event, input_data)


def validate_synthesis_output(
    payload: dict[str, object], allowed_version_uids: set[str]
) -> list[dict[str, object]]:
    parsed = synthesis_payload(payload)
    if set(parsed) - {"blocks"}:
        raise SynthesisValidationError("合成稿包含未知字段")
    raw_blocks = parsed.get("blocks")
    if not isinstance(raw_blocks, list) or not raw_blocks:
        raise SynthesisValidationError("合成稿缺少有效 blocks")
    blocks: list[dict[str, object]] = []
    for raw_block in raw_blocks:
        if not isinstance(raw_block, dict):
            raise SynthesisValidationError("合成稿 block 格式无效")
        if set(raw_block) - {"kind", "body", "attribution", "citations"}:
            raise SynthesisValidationError("合成稿 block 包含未知字段")
        raw_kind = raw_block.get("kind")
        raw_body = raw_block.get("body")
        raw_attribution = raw_block.get("attribution", "")
        if not isinstance(raw_kind, str) or not isinstance(raw_body, str):
            raise SynthesisValidationError("合成稿 block 字段类型无效")
        if not isinstance(raw_attribution, str):
            raise SynthesisValidationError("合成稿 attribution 类型无效")
        kind = raw_kind.strip()
        body = raw_body.strip()
        attribution = raw_attribution.strip()
        if kind not in SYNTHESIS_BLOCK_KINDS:
            raise SynthesisValidationError("合成稿 block kind 无效")
        if not body:
            raise SynthesisValidationError("合成稿 block 正文不能为空")
        if kind == "viewpoint" and not attribution:
            raise SynthesisValidationError("观点 block 必须注明归属")
        raw_citations = raw_block.get("citations")
        if not isinstance(raw_citations, list) or not raw_citations:
            raise SynthesisValidationError("每个合成稿 block 都必须有引用")
        citations: list[dict[str, str]] = []
        cited_uids: set[str] = set()
        sides: set[str] = set()
        for raw_citation in raw_citations:
            if not isinstance(raw_citation, dict):
                raise SynthesisValidationError("合成稿引用格式无效")
            if set(raw_citation) - {"evidence_version_uid", "side"}:
                raise SynthesisValidationError("合成稿引用包含未知字段")
            raw_version_uid = raw_citation.get("evidence_version_uid")
            raw_side = raw_citation.get("side")
            if not isinstance(raw_version_uid, str) or not isinstance(raw_side, str):
                raise SynthesisValidationError("合成稿引用字段类型无效")
            version_uid = raw_version_uid.strip()
            side = raw_side.strip()
            if version_uid not in allowed_version_uids:
                raise SynthesisValidationError("合成稿引用不属于当前 Evidence Snapshot")
            if not side:
                raise SynthesisValidationError("合成稿引用 side 不能为空")
            if len(side) > SYNTHESIS_CITATION_SIDE_MAX_LENGTH:
                raise SynthesisValidationError("合成稿引用 side 过长")
            if version_uid in cited_uids:
                raise SynthesisValidationError("同一 block 不得重复引用同一证据")
            cited_uids.add(version_uid)
            sides.add(side)
            citations.append({"evidence_version_uid": version_uid, "side": side})
        if kind == "disagreement" and (len(cited_uids) < 2 or len(sides) < 2):
            raise SynthesisValidationError("分歧 block 必须分别引用至少两侧证据")
        blocks.append(
            {
                "kind": kind,
                "body": body,
                "attribution": attribution,
                "citations": citations,
            }
        )
    return blocks


def apply_generation_synthesis_result(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
    application: GenerationApplication,
    event: Event,
) -> SynthesisVersion:
    context = generation_synthesis_application_context(
        session,
        request=request,
        result=result,
        event=event,
    )
    if context is None:
        raise GenerationResultNotCurrentError("生成结果已过期，不能应用")
    snapshot, allowed_version_uids, source_count, generation_fingerprint = context
    if request.input_fingerprint != generation_fingerprint:
        raise SynthesisValidationError("Generation Request 的 input fingerprint 不匹配")
    blocks = validate_synthesis_output(result.payload_json, allowed_version_uids)
    version = session.scalar(
        select(SynthesisVersion).where(
            SynthesisVersion.event_id == event.id,
            SynthesisVersion.generation_fingerprint == generation_fingerprint,
        )
    )
    if version is None:
        version = save_synthesis_version(
            session,
            event=event,
            snapshot=snapshot,
            source_count=source_count,
            provider=request.provider,
            model=request.model,
            generation_fingerprint=generation_fingerprint,
            blocks=blocks,
        )
    else:
        advance_synthesis_pointer(session, event, version)
        advance_material_review_for_synthesis(session, event, version)
    applied_at = now_utc()
    application.apply_attempt_count += 1
    application.status = "applied"
    application.artifact_type = "synthesis-version"
    application.artifact_id = version.id
    application.error = ""
    application.applied_at = applied_at
    application.updated_at = applied_at
    session.flush()
    return version


def apply_generation_evidence_review_result(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
    application: GenerationApplication,
    event: Event,
) -> EvidenceReview:
    context = generation_evidence_review_application_context(
        session,
        request=request,
        result=result,
        event=event,
    )
    if context is None:
        raise GenerationResultNotCurrentError("证据复审结果已过期，不能应用")
    (
        baseline_snapshot,
        target_snapshot,
        allowed_version_uids,
        comparison_fingerprint,
    ) = context
    review_result, reason, cited_version_uids, synthesis = (
        validate_evidence_review_output(result.payload_json, allowed_version_uids)
    )
    if review_result == "material" and synthesis is None:
        raise SynthesisValidationError(
            "material Review 必须在同一次结果中返回新合成稿"
        )
    review = (
        save_evidence_review(
            session,
            event=event,
            baseline_snapshot=baseline_snapshot,
            target_snapshot=target_snapshot,
            comparison_fingerprint=comparison_fingerprint,
            result=review_result,
            reason=reason,
            cited_version_uids=cited_version_uids,
            provider=request.provider,
            model=request.model,
        )
        if review_result == "material"
        else None
    )
    with session.begin_nested():
        if review is None:
            review = save_evidence_review(
                session,
                event=event,
                baseline_snapshot=baseline_snapshot,
                target_snapshot=target_snapshot,
                comparison_fingerprint=comparison_fingerprint,
                result=review_result,
                reason=reason,
                cited_version_uids=cited_version_uids,
                provider=request.provider,
                model=request.model,
            )
        if synthesis is not None:
            blocks = validate_synthesis_output(synthesis, allowed_version_uids)
            generation_fingerprint = synthesis_generation_fingerprint(
                target_snapshot.source_coverage_fingerprint,
                target_snapshot.content_fingerprint,
            )
            version = session.scalar(
                select(SynthesisVersion).where(
                    SynthesisVersion.event_id == event.id,
                    SynthesisVersion.generation_fingerprint
                    == generation_fingerprint,
                )
            )
            if version is None:
                evidence = synthesis_snapshot_evidence(session, target_snapshot)
                save_synthesis_version(
                    session,
                    event=event,
                    snapshot=target_snapshot,
                    source_count=len(
                        {int(row["source_id"]) for row in evidence}
                    ),
                    provider=request.provider,
                    model=request.model,
                    generation_fingerprint=generation_fingerprint,
                    blocks=blocks,
                )
            else:
                advance_synthesis_pointer(session, event, version)
                advance_material_review_for_synthesis(session, event, version)
        applied_at = now_utc()
        application.apply_attempt_count += 1
        application.status = "applied"
        application.artifact_type = "evidence-review"
        application.artifact_id = review.id
        application.error = ""
        application.applied_at = applied_at
        application.updated_at = applied_at
        session.flush()
    assert review is not None
    return review


def generation_evidence_review_application_context(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
    event: Event | None = None,
) -> tuple[EvidenceSnapshot, EvidenceSnapshot, set[str], str] | None:
    if request.task_type != EVIDENCE_REVIEW_TASK_TYPE or request.target_type != "event":
        return None
    event = event or session.get(Event, request.target_id)
    if event is None or event.status != "active":
        return None
    current_request = latest_generation_request(
        session,
        task_type=request.task_type,
        target_type=request.target_type,
        target_id=request.target_id,
        input_fingerprint=request.input_fingerprint,
    )
    current_attempt = latest_attempt(session, request.id)
    if (
        current_request is None
        or current_request.id != request.id
        or current_attempt is None
        or current_attempt.id != result.attempt_id
    ):
        return None
    frozen = session.get(GenerationRequestPayload, request.id)
    if frozen is None:
        raise SynthesisValidationError("Generation Request 的冻结 payload 不存在")
    if frozen.payload_json is not None:
        baseline, target, allowed_version_uids, fingerprint, _ = (
            evidence_review_task_snapshots_from_request(
                session, event, frozen.payload_json
            )
        )
    elif frozen.application_context_json is not None:
        baseline, target, allowed_version_uids, fingerprint = (
            evidence_review_task_snapshots_from_application_context(
                session, event, frozen.application_context_json
            )
        )
    else:
        raise SynthesisValidationError(
            "Generation Request payload 已清理且缺少应用校验上下文"
        )
    if request.input_fingerprint != fingerprint:
        raise SynthesisValidationError(
            "Generation Request 的 input fingerprint 不匹配"
        )
    return baseline, target, allowed_version_uids, fingerprint


def evidence_review_task_snapshots_from_application_context(
    session: Session,
    event: Event,
    application_context: dict[str, object],
) -> tuple[EvidenceSnapshot, EvidenceSnapshot, set[str], str]:
    baseline_uid = application_context.get("baseline_snapshot_uid")
    target_uid = application_context.get("target_snapshot_uid")
    if not isinstance(baseline_uid, str) or not isinstance(target_uid, str):
        raise SynthesisValidationError("Generation Request 的应用校验上下文无效")
    baseline = session.scalar(
        select(EvidenceSnapshot).where(EvidenceSnapshot.uid == baseline_uid)
    )
    target = session.scalar(
        select(EvidenceSnapshot).where(EvidenceSnapshot.uid == target_uid)
    )
    if (
        baseline is None
        or target is None
        or baseline.event_id != event.id
        or target.event_id != event.id
    ):
        raise SynthesisValidationError("Generation Request 的 Evidence Snapshot 无效")
    expected_input, _ = evidence_review_input_data(
        session, event, baseline, target
    )
    expected_context = generation_application_context(
        EVIDENCE_REVIEW_TASK_TYPE,
        {"input_data": expected_input},
    )
    if expected_context != application_context:
        raise SynthesisValidationError(
            "Generation Request 的应用校验上下文不匹配"
        )
    allowed_version_uids = {
        str(row["evidence_version_uid"])
        for row in expected_input["target_evidence"]
        if isinstance(row, dict)
    }
    return (
        baseline,
        target,
        allowed_version_uids,
        str(expected_input["comparison_fingerprint"]),
    )


def generation_synthesis_application_context(
    session: Session,
    *,
    request: GenerationRequest,
    result: GenerationResult,
    event: Event | None = None,
) -> tuple[EvidenceSnapshot, set[str], int, str] | None:
    if request.task_type != SYNTHESIS_TASK_TYPE or request.target_type != "event":
        return None
    event = event or session.get(Event, request.target_id)
    if event is None or event.id != request.target_id:
        return None
    current_request = latest_generation_request(
        session,
        task_type=request.task_type,
        target_type=request.target_type,
        target_id=request.target_id,
        input_fingerprint=request.input_fingerprint,
    )
    current_attempt = latest_attempt(session, request.id)
    current_revision = (
        session.get(EventRevision, event.current_revision_id)
        if event.current_revision_id is not None
        else None
    )
    try:
        current_fingerprint = (
            synthesis_generation_for_revision(session, current_revision)[1]
            if current_revision is not None and event.status == "active"
            else None
        )
    except SynthesisValidationError:
        current_fingerprint = None
    if (
        current_request is None
        or current_request.id != request.id
        or current_attempt is None
        or current_attempt.id != result.attempt_id
        or current_fingerprint != request.input_fingerprint
    ):
        return None
    frozen = session.get(GenerationRequestPayload, request.id)
    if frozen is None:
        raise SynthesisValidationError("Generation Request 的冻结 payload 不存在")
    if frozen.payload_json is not None:
        application_context = synthesis_task_snapshot_from_request(
            session, event, frozen.payload_json
        )
    elif frozen.application_context_json is not None:
        application_context = synthesis_task_snapshot_from_application_context(
            session,
            event,
            frozen.application_context_json,
        )
    else:
        raise SynthesisValidationError(
            "Generation Request payload 已清理且缺少应用校验上下文"
        )
    (
        snapshot,
        allowed_version_uids,
        source_count,
        generation_fingerprint,
    ) = application_context
    if snapshot.target_revision_id != event.current_revision_id:
        return None
    return snapshot, allowed_version_uids, source_count, generation_fingerprint


def fail_generation_application(
    session: Session, application_id: int, error: str
) -> None:
    application = session.scalar(
        select(GenerationApplication)
        .where(GenerationApplication.id == application_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if application is None or application.status == "applied":
        return
    message = error.strip() or "生成结果应用失败"
    application.apply_attempt_count += 1
    application.status = "failed"
    application.artifact_type = ""
    application.artifact_id = None
    application.error = message
    application.last_error = message
    application.applied_at = None
    application.updated_at = now_utc()
    session.flush()


def save_synthesis_version(
    session: Session,
    *,
    event: Event,
    snapshot: EvidenceSnapshot,
    source_count: int,
    provider: str,
    model: str,
    generation_fingerprint: str,
    blocks: list[dict[str, object]],
) -> SynthesisVersion:
    member_rows = session.execute(
        select(EvidenceSnapshotMember, EventEvidenceVersion)
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id == EvidenceSnapshotMember.evidence_version_id,
        )
        .where(EvidenceSnapshotMember.snapshot_id == snapshot.id)
        .order_by(EvidenceSnapshotMember.position)
    ).all()
    member_by_uid = {version.uid: (member, version) for member, version in member_rows}
    if not blocks:
        raise SynthesisValidationError("合成稿缺少有效 blocks")
    version = SynthesisVersion(
        uid=str(uuid4()),
        event_id=event.id,
        target_revision_id=snapshot.target_revision_id,
        snapshot_id=snapshot.id,
        source_count=source_count,
        provider=provider,
        model=model,
        prompt_version=SYNTHESIS_PROMPT_VERSION,
        schema_version=SYNTHESIS_SCHEMA_VERSION,
        generation_fingerprint=generation_fingerprint,
    )
    session.add(version)
    session.flush()
    for block_position, block_data in enumerate(blocks, 1):
        block = SynthesisBlock(
            uid=str(uuid4()),
            synthesis_version_id=version.id,
            position=block_position,
            kind=str(block_data["kind"]),
            body=str(block_data["body"]),
            attribution=str(block_data["attribution"]),
        )
        session.add(block)
        session.flush()
        for citation_position, citation_data in enumerate(block_data["citations"], 1):
            assert isinstance(citation_data, dict)
            member, evidence_version = member_by_uid[
                str(citation_data["evidence_version_uid"])
            ]
            session.add(
                SynthesisCitation(
                    block_id=block.id,
                    synthesis_version_id=version.id,
                    snapshot_id=snapshot.id,
                    evidence_version_id=evidence_version.id,
                    evidence_type=member.evidence_type,
                    role=member.role,
                    side=str(citation_data["side"]),
                    position=citation_position,
                )
            )
    session.flush()
    advance_synthesis_pointer(session, event, version)
    advance_material_review_for_synthesis(session, event, version)
    session.flush()
    return version


def advance_synthesis_pointer(
    session: Session, event: Event, candidate: SynthesisVersion
) -> bool:
    candidate_order = artifact_revision_order(
        session, event, candidate.target_revision_id, candidate.id
    )
    current = (
        session.get(SynthesisVersion, event.current_synthesis_version_id)
        if event.current_synthesis_version_id is not None
        else None
    )
    current_revision = (
        session.get(EventRevision, event.current_revision_id)
        if event.current_revision_id is not None
        else None
    )
    if current is not None and current_revision is not None:
        try:
            _source_count, current_generation_fingerprint = (
                synthesis_generation_for_revision(session, current_revision)
            )
        except SynthesisValidationError:
            current_generation_fingerprint = None
        if (
            current.generation_fingerprint == current_generation_fingerprint
            and candidate.generation_fingerprint != current_generation_fingerprint
        ):
            return False
    if current is not None and candidate_order < artifact_revision_order(
        session, event, current.target_revision_id, current.id
    ):
        return False
    event.current_synthesis_version_id = candidate.id
    return True


def advance_material_review_for_synthesis(
    session: Session, event: Event, version: SynthesisVersion
) -> None:
    review = session.scalar(
        select(EvidenceReview)
        .where(
            EvidenceReview.event_id == event.id,
            EvidenceReview.target_snapshot_id == version.snapshot_id,
            EvidenceReview.result == "material",
            EvidenceReview.policy_version == EVIDENCE_REVIEW_POLICY_VERSION,
        )
        .order_by(EvidenceReview.id.desc())
        .limit(1)
    )
    if review is not None:
        advance_review_pointer(session, event, review)


def ordinary_review_in_synthesis_lineage(
    current: SynthesisVersion | None,
    latest: EvidenceReview | None,
    reviews: list[EvidenceReview],
) -> EvidenceReview | None:
    if (
        current is None
        or latest is None
        or latest.event_id != current.event_id
        or latest.result != "ordinary"
        or latest.policy_version != EVIDENCE_REVIEW_POLICY_VERSION
    ):
        return None
    reviews_by_target: dict[int, list[EvidenceReview]] = {}
    for review in reviews:
        if (
            review.event_id == current.event_id
            and review.result == "ordinary"
            and review.policy_version == EVIDENCE_REVIEW_POLICY_VERSION
            and review.id <= latest.id
        ):
            reviews_by_target.setdefault(review.target_snapshot_id, []).append(
                review
            )
    cursor = latest
    visited: set[int] = set()
    while cursor.baseline_snapshot_id != current.snapshot_id:
        if cursor.id in visited:
            return None
        visited.add(cursor.id)
        predecessors = [
            review
            for review in reviews_by_target.get(cursor.baseline_snapshot_id, [])
            if review.id < cursor.id
        ]
        if len(predecessors) != 1:
            return None
        cursor = predecessors[0]
    return latest


def latest_ordinary_review_for_synthesis(
    session: Session,
    event: Event,
    current: SynthesisVersion | None,
) -> EvidenceReview | None:
    if current is None or event.reviewed_evidence_review_id is None:
        return None
    reviews = list(
        session.scalars(
            select(EvidenceReview).where(
                EvidenceReview.event_id == event.id,
                EvidenceReview.result == "ordinary",
                EvidenceReview.id <= event.reviewed_evidence_review_id,
            )
        )
    )
    latest = next(
        (
            review
            for review in reviews
            if review.id == event.reviewed_evidence_review_id
        ),
        None,
    )
    return ordinary_review_in_synthesis_lineage(current, latest, reviews)


def matching_ordinary_review(
    session: Session,
    event: Event,
    current: SynthesisVersion | None,
    revision: EventRevision,
) -> EvidenceReview | None:
    review = latest_ordinary_review_for_synthesis(session, event, current)
    return review if review is not None and review.target_revision_id == revision.id else None


def evidence_review_task_status(
    session: Session,
    event: Event,
    current: SynthesisVersion | None,
    revision: EventRevision,
    source_fingerprint: str,
    content_fingerprint: str,
) -> tuple[str, GenerationTaskOut | None]:
    if current is None:
        return "idle", None
    latest_review = latest_ordinary_review_for_synthesis(session, event, current)
    baseline_snapshot = session.get(
        EvidenceSnapshot,
        latest_review.target_snapshot_id
        if latest_review is not None
        else current.snapshot_id,
    )
    target_snapshot = session.scalar(
        select(EvidenceSnapshot).where(
            EvidenceSnapshot.event_id == event.id,
            EvidenceSnapshot.target_revision_id == revision.id,
            EvidenceSnapshot.source_coverage_fingerprint == source_fingerprint,
            EvidenceSnapshot.content_fingerprint == content_fingerprint,
            EvidenceSnapshot.policy_version == SYNTHESIS_POLICY_VERSION,
        )
    )
    if baseline_snapshot is None or target_snapshot is None:
        return "idle", None
    baseline_revision = session.get(
        EventRevision, baseline_snapshot.target_revision_id
    )
    if baseline_revision is None:
        return "idle", None
    comparison_fingerprint = evidence_review_comparison_fingerprint(
        event,
        baseline_revision,
        baseline_snapshot,
        revision,
        target_snapshot,
    )
    request = latest_generation_request(
        session,
        task_type=EVIDENCE_REVIEW_TASK_TYPE,
        target_type="event",
        target_id=event.id,
        input_fingerprint=comparison_fingerprint,
    )
    if request is not None:
        task = generation_task_out(session, request)
        return task.status, task
    task = session.scalar(
        select(LLMTask)
        .where(
            LLMTask.task_type == EVIDENCE_REVIEW_TASK_TYPE,
            LLMTask.object_type == "event",
            LLMTask.object_id == event.id,
            LLMTask.input_fingerprint == comparison_fingerprint,
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
        .limit(1)
    )
    return (
        (
            task.status
            if task is not None
            and task.status in {"pending", "running", "failed", "complete"}
            else "idle"
        ),
        None,
    )


def event_synthesis_state(
    session: Session, event: Event, revision: EventRevision
) -> EventSynthesisStateOut:
    evidence_rows = synthesis_evidence_rows(session, revision)
    (
        source_count,
        source_fingerprint,
        content_fingerprint,
        generation_fingerprint,
    ) = synthesis_fingerprints_for_rows(evidence_rows)
    current = (
        session.get(SynthesisVersion, event.current_synthesis_version_id)
        if event.current_synthesis_version_id is not None
        else None
    )
    matching = None
    if current is None or current.generation_fingerprint != generation_fingerprint:
        matching = (
            session.scalar(
                select(SynthesisVersion).where(
                    SynthesisVersion.event_id == event.id,
                    SynthesisVersion.generation_fingerprint
                    == generation_fingerprint,
                )
            )
            if generation_fingerprint is not None
            else None
        )
    evidence_matching_rows = (
        session.execute(
            select(SynthesisVersion, EvidenceSnapshot)
            .join(
                EvidenceSnapshot,
                EvidenceSnapshot.id == SynthesisVersion.snapshot_id,
            )
            .where(
                SynthesisVersion.event_id == event.id,
                EvidenceSnapshot.source_coverage_fingerprint
                == source_fingerprint,
                EvidenceSnapshot.content_fingerprint == content_fingerprint,
            )
            .order_by(SynthesisVersion.id.desc())
        ).all()
        if matching is None
        else []
    )
    compatible_evidence_matching_row = next(
        (
            (version, snapshot)
            for version, snapshot in evidence_matching_rows
            if synthesis_version_is_compatible(version, snapshot)
        ),
        None,
    )
    evidence_matching_row = (
        compatible_evidence_matching_row
        or (evidence_matching_rows[0] if evidence_matching_rows else None)
    )
    evidence_matching = (
        evidence_matching_row[0] if evidence_matching_row is not None else None
    )
    versions = {
        version.id: version
        for version in (current, matching, evidence_matching)
        if version is not None
    }
    reviews = list(
        session.scalars(
            select(EvidenceReview).where(EvidenceReview.event_id == event.id)
        )
    )
    latest_review = next(
        (
            review
            for review in reviews
            if review.id == event.reviewed_evidence_review_id
        ),
        None,
    )
    snapshot_ids_by_version = {
        version.id: version.snapshot_id for version in versions.values()
    }
    snapshot_coverage = snapshot_evidence_coverage_for(
        session,
        list(snapshot_ids_by_version.values())
        + [
            snapshot_id
            for review in reviews
            for snapshot_id in (
                review.baseline_snapshot_id,
                review.target_snapshot_id,
            )
        ],
    )
    revision_ids = {version.target_revision_id for version in versions.values()} | {
        review.target_revision_id for review in reviews
    }
    revision_uids = dict(
        session.execute(
            select(EventRevision.id, EventRevision.uid).where(
                EventRevision.id.in_(revision_ids)
            )
        ).all()
    )
    freshness, current = resolved_synthesis_freshness_projection(
        event=event,
        revision=revision,
        generation_fingerprint=generation_fingerprint,
        current=current,
        matching=matching,
        evidence_matching=evidence_matching,
        evidence_matching_is_compatible=(
            compatible_evidence_matching_row is not None
        ),
        evidence_rows=evidence_rows,
        snapshot_ids_by_version=snapshot_ids_by_version,
        snapshot_coverage=snapshot_coverage,
        latest_review=latest_review,
        reviews=reviews,
        revision_uids=revision_uids,
    )
    current_out = synthesis_version_out(session, current) if current else None
    generation_request = (
        latest_generation_request(
            session,
            task_type=SYNTHESIS_TASK_TYPE,
            target_type="event",
            target_id=event.id,
            input_fingerprint=generation_fingerprint,
        )
        if generation_fingerprint is not None
        else None
    )
    generation_task = (
        generation_task_out(session, generation_request)
        if generation_request is not None
        else None
    )
    legacy_task_status = synthesis_task_status(
        session, event.id, generation_fingerprint
    )
    legacy_task_is_current = legacy_task_status in {"pending", "running"} or (
        legacy_task_status == "complete" and current_out is not None
    )
    if legacy_task_is_current:
        generation_task_status = legacy_task_status
        generation_task = None
    else:
        generation_task_status = (
            generation_task.status
            if generation_task is not None
            else legacy_task_status
        )
    review_task_status, review_task = evidence_review_task_status(
        session,
        event,
        current,
        revision,
        source_fingerprint,
        content_fingerprint,
    )
    return EventSynthesisStateOut(
        **freshness.model_dump(),
        target_revision_uid=revision.uid,
        source_view_revision_uid=revision.uid,
        source_count=source_count,
        can_generate=event.status == "active" and source_count > 1,
        default_view="source",
        task_status=(
            generation_task_status
            if freshness.status == "stale" and generation_task_status != "idle"
            else review_task_status
            if review_task_status != "idle"
            else generation_task_status
            if generation_task_status != "idle"
            else "complete"
            if current_out
            else "idle"
        ),
        task=review_task or generation_task,
        current=current_out,
    )


def resolve_synthesis_freshness(
    *,
    event_status: str,
    generation_fingerprint: str | None,
    current: SynthesisVersion | None,
    matching: SynthesisVersion | None,
    evidence_matching: SynthesisVersion | None,
    evidence_matching_is_compatible: bool,
    has_uncovered_evidence: bool,
    ordinary_review_matches: bool = False,
) -> tuple[str, SynthesisVersion | None]:
    if current is not None and (
        current.generation_fingerprint == generation_fingerprint
    ):
        return "current", current
    if matching is not None:
        return "current", matching
    if event_status != "active":
        return ("current", current) if current is not None else ("missing", None)
    if current is not None and ordinary_review_matches:
        return "current", current
    if evidence_matching is not None:
        return (
            ("current", evidence_matching)
            if evidence_matching_is_compatible
            else ("missing", None)
        )
    if current is not None:
        return ("unreviewed", current) if has_uncovered_evidence else ("missing", current)
    return "missing", None


def resolved_synthesis_freshness_projection(
    *,
    event: Event,
    revision: EventRevision,
    generation_fingerprint: str | None,
    current: SynthesisVersion | None,
    matching: SynthesisVersion | None,
    evidence_matching: SynthesisVersion | None,
    evidence_matching_is_compatible: bool,
    evidence_rows: list[SynthesisEvidenceRow],
    snapshot_ids_by_version: dict[int, int],
    snapshot_coverage: dict[int, SynthesisEvidenceCoverage],
    latest_review: EvidenceReview | None,
    reviews: list[EvidenceReview],
    revision_uids: dict[int, str],
) -> tuple[EventSynthesisFreshnessOut, SynthesisVersion | None]:
    _, resolved = resolve_synthesis_freshness(
        event_status=event.status,
        generation_fingerprint=generation_fingerprint,
        current=current,
        matching=matching,
        evidence_matching=evidence_matching,
        evidence_matching_is_compatible=evidence_matching_is_compatible,
        has_uncovered_evidence=True,
    )
    resolved_snapshot_id = (
        snapshot_ids_by_version[resolved.id] if resolved is not None else None
    )
    resolved_coverage = (
        snapshot_coverage[resolved_snapshot_id]
        if resolved_snapshot_id is not None
        else (set(), set())
    )
    review = ordinary_review_in_synthesis_lineage(resolved, latest_review, reviews)
    review_baseline_snapshot_id = (
        review.target_snapshot_id if review is not None else resolved_snapshot_id
    )
    material_review = max(
        (
            candidate
            for candidate in reviews
            if candidate.result == "material"
            and candidate.policy_version == EVIDENCE_REVIEW_POLICY_VERSION
            and candidate.baseline_snapshot_id == review_baseline_snapshot_id
            and candidate.target_revision_id == revision.id
        ),
        key=lambda candidate: candidate.id,
        default=None,
    )
    reviewed_coverage = (
        snapshot_coverage[review.target_snapshot_id]
        if review is not None
        else resolved_coverage
    )
    current_source_ids = {version.source_id for _, version, _, _ in evidence_rows}
    unreviewed_evidence_count, unreviewed_source_count = (
        unreviewed_evidence_difference(evidence_rows, reviewed_coverage)
    )
    status, resolved = resolve_synthesis_freshness(
        event_status=event.status,
        generation_fingerprint=generation_fingerprint,
        current=current,
        matching=matching,
        evidence_matching=evidence_matching,
        evidence_matching_is_compatible=evidence_matching_is_compatible,
        has_uncovered_evidence=unreviewed_evidence_count > 0,
        ordinary_review_matches=(
            review is not None and review.target_revision_id == revision.id
        ),
    )
    if (
        material_review is not None
        and resolved is not None
        and resolved.target_revision_id != material_review.target_revision_id
    ):
        status = "stale"
    if status != "unreviewed":
        unreviewed_evidence_count = unreviewed_source_count = 0
    reviewed_revision_id = (
        review.target_revision_id
        if review is not None
        else resolved.target_revision_id
        if resolved is not None
        else None
    )
    return (
        EventSynthesisFreshnessOut(
            status=status,
            current_revision_uid=revision.uid,
            covered_revision_uid=(
                revision_uids[resolved.target_revision_id]
                if resolved is not None
                else None
            ),
            reviewed_revision_uid=(
                revision_uids[reviewed_revision_id]
                if reviewed_revision_id is not None
                else None
            ),
            new_source_count=(
                len(
                    (
                        snapshot_coverage[review.target_snapshot_id][1]
                        & current_source_ids
                    )
                    - resolved_coverage[1]
                )
                if review is not None
                else 0
            ),
            unreviewed_evidence_count=unreviewed_evidence_count,
            unreviewed_source_count=unreviewed_source_count,
        ),
        resolved,
    )


def synthesis_candidate_version_rows(
    session: Session,
    event_rows: list[tuple[Event, EventRevision]],
    fingerprints_by_event: dict[int, tuple[str, str, str | None]],
) -> list[tuple[SynthesisVersion, EvidenceSnapshot]]:
    current_version_ids = [
        event.current_synthesis_version_id
        for event, _revision in event_rows
        if event.current_synthesis_version_id is not None
    ]
    criteria = []
    if current_version_ids:
        criteria.append(SynthesisVersion.id.in_(current_version_ids))
    for event, _revision in event_rows:
        source_fingerprint, content_fingerprint, generation_fingerprint = (
            fingerprints_by_event[event.id]
        )
        if generation_fingerprint is not None:
            criteria.append(
                and_(
                    SynthesisVersion.event_id == event.id,
                    SynthesisVersion.generation_fingerprint == generation_fingerprint,
                )
            )
        criteria.append(
            and_(
                SynthesisVersion.event_id == event.id,
                EvidenceSnapshot.source_coverage_fingerprint == source_fingerprint,
                EvidenceSnapshot.content_fingerprint == content_fingerprint,
            )
        )
    if not criteria:
        return []
    return list(
        session.execute(
            select(SynthesisVersion, EvidenceSnapshot)
            .join(
                EvidenceSnapshot,
                EvidenceSnapshot.id == SynthesisVersion.snapshot_id,
            )
            .where(or_(*criteria))
            .order_by(SynthesisVersion.id)
        ).all()
    )


def event_synthesis_freshness_for(
    session: Session, revision_uids_by_event: dict[str, str]
) -> dict[str, EventSynthesisFreshnessOut]:
    if not revision_uids_by_event:
        return {}
    candidate_rows = list(
        session.execute(
            select(Event, EventRevision)
            .options(
                undefer(Event.current_synthesis_version_id),
                undefer(Event.reviewed_evidence_review_id),
            )
            .join(EventRevision, EventRevision.event_id == Event.id)
            .where(
                Event.uid.in_(revision_uids_by_event),
                EventRevision.uid.in_(revision_uids_by_event.values()),
            )
        ).all()
    )
    rows_by_event_uid = {
        event.uid: (event, revision)
        for event, revision in candidate_rows
        if revision.uid == revision_uids_by_event[event.uid]
    }
    if rows_by_event_uid.keys() != revision_uids_by_event.keys():
        raise SynthesisValidationError("Event synthesis Revision 归属不匹配")
    event_rows = [rows_by_event_uid[event_uid] for event_uid in revision_uids_by_event]
    revision_ids = [revision.id for _event, revision in event_rows]
    evidence_rows_by_revision: dict[
        int,
        list[
            tuple[
                EventRevisionEvidence,
                EventEvidenceVersion,
                EventEvidence,
                Source,
            ]
        ],
    ] = {revision_id: [] for revision_id in revision_ids}
    if revision_ids:
        for row in session.execute(
            select(
                EventRevisionEvidence,
                EventEvidenceVersion,
                EventEvidence,
                Source,
            )
            .join(
                EventEvidenceVersion,
                EventEvidenceVersion.id
                == EventRevisionEvidence.evidence_version_id,
            )
            .join(
                EventEvidence,
                EventEvidence.id == EventEvidenceVersion.evidence_id,
            )
            .join(Source, Source.id == EventEvidenceVersion.source_id)
            .where(
                EventRevisionEvidence.revision_id.in_(revision_ids),
                Source.status == "active",
                unfiltered_content_clause(
                    EventEvidenceVersion.legacy_content_item_id
                ),
            )
        ):
            evidence_rows_by_revision[row[0].revision_id].append(row)
    fingerprints_by_event: dict[int, tuple[str, str, str | None]] = {}
    for event, revision in event_rows:
        rows = canonical_synthesis_evidence_rows(evidence_rows_by_revision[revision.id])
        (
            _source_count,
            source_fingerprint,
            content_fingerprint,
            generation_fingerprint,
        ) = synthesis_fingerprints_for_rows(rows)
        fingerprints_by_event[event.id] = (
            source_fingerprint,
            content_fingerprint,
            generation_fingerprint,
        )

    version_rows = synthesis_candidate_version_rows(
        session, event_rows, fingerprints_by_event
    )
    versions = [version for version, _snapshot in version_rows]
    version_by_id = {version.id: version for version in versions}
    matching_by_key = {
        (version.event_id, version.generation_fingerprint): version
        for version in versions
    }
    evidence_matching_by_key = {
        (
            version.event_id,
            snapshot.source_coverage_fingerprint,
            snapshot.content_fingerprint,
        ): version
        for version, snapshot in version_rows
    }
    compatible_evidence_matching_by_key = {
        (
            version.event_id,
            snapshot.source_coverage_fingerprint,
            snapshot.content_fingerprint,
        ): version
        for version, snapshot in version_rows
        if synthesis_version_is_compatible(version, snapshot)
    }
    snapshot_by_version_id = {
        version.id: snapshot for version, snapshot in version_rows
    }
    snapshot_ids_by_version = {
        version_id: snapshot.id
        for version_id, snapshot in snapshot_by_version_id.items()
    }
    event_ids = [event.id for event, _revision in event_rows]
    reviews = list(
        session.scalars(
            select(EvidenceReview).where(EvidenceReview.event_id.in_(event_ids))
        )
    )
    review_by_id = {review.id: review for review in reviews}
    reviews_by_event: dict[int, list[EvidenceReview]] = {}
    for review in reviews:
        reviews_by_event.setdefault(review.event_id, []).append(review)
    snapshot_coverage = snapshot_evidence_coverage_for(
        session,
        [snapshot.id for snapshot in snapshot_by_version_id.values()]
        + [
            snapshot_id
            for review in reviews
            for snapshot_id in (
                review.baseline_snapshot_id,
                review.target_snapshot_id,
            )
        ],
    )
    revision_ids_by_version = {
        version.target_revision_id for version in versions
    } | {review.target_revision_id for review in reviews}
    revision_uids = dict(
        session.execute(
            select(EventRevision.id, EventRevision.uid).where(
                EventRevision.id.in_(revision_ids_by_version)
            )
        ).all()
    )
    freshness_by_event: dict[str, EventSynthesisFreshnessOut] = {}
    for event, revision in event_rows:
        source_fingerprint, content_fingerprint, generation_fingerprint = (
            fingerprints_by_event[event.id]
        )
        current = (
            version_by_id.get(event.current_synthesis_version_id)
            if event.current_synthesis_version_id is not None
            else None
        )
        matching = matching_by_key.get((event.id, generation_fingerprint))
        compatible_evidence_matching = compatible_evidence_matching_by_key.get(
            (event.id, source_fingerprint, content_fingerprint)
        )
        evidence_matching = compatible_evidence_matching or evidence_matching_by_key.get(
            (event.id, source_fingerprint, content_fingerprint)
        )
        freshness_by_event[event.uid], _resolved = (
            resolved_synthesis_freshness_projection(
                event=event,
                revision=revision,
                generation_fingerprint=generation_fingerprint,
                current=current,
                matching=matching,
                evidence_matching=evidence_matching,
                evidence_matching_is_compatible=(
                    compatible_evidence_matching is not None
                ),
                evidence_rows=canonical_synthesis_evidence_rows(
                    evidence_rows_by_revision[revision.id]
                ),
                snapshot_ids_by_version=snapshot_ids_by_version,
                snapshot_coverage=snapshot_coverage,
                latest_review=(
                    review_by_id.get(event.reviewed_evidence_review_id)
                    if event.reviewed_evidence_review_id is not None
                    else None
                ),
                reviews=reviews_by_event.get(event.id, []),
                revision_uids=revision_uids,
            )
        )
    return freshness_by_event


def synthesis_version_out(
    session: Session, version: SynthesisVersion
) -> SynthesisVersionOut:
    snapshot = session.get(EvidenceSnapshot, version.snapshot_id)
    revision = session.get(EventRevision, version.target_revision_id)
    assert snapshot is not None and revision is not None
    blocks: list[SynthesisBlockOut] = []
    for block in session.scalars(
        select(SynthesisBlock)
        .where(SynthesisBlock.synthesis_version_id == version.id)
        .order_by(SynthesisBlock.position)
    ):
        citations = [
            SynthesisCitationOut(
                evidence_version_uid=evidence_version.uid,
                evidence_type=citation.evidence_type,
                role=citation.role,
                side=citation.side,
                source=EventEvidenceSourceOut(
                    source_id=source.id,
                    name=source.name,
                    feed_url=source.url,
                    site_url=source.site_url,
                    media_type=source.media_type,
                ),
                legacy_content_item_id_snapshot=(
                    evidence_version.legacy_content_item_id_snapshot
                ),
                title=evidence_version.title_snapshot,
                url=evidence_version.url_snapshot,
                published_at=evidence_version.published_at_snapshot,
            )
            for citation, evidence_version, source in session.execute(
                select(SynthesisCitation, EventEvidenceVersion, Source)
                .join(
                    EventEvidenceVersion,
                    EventEvidenceVersion.id == SynthesisCitation.evidence_version_id,
                )
                .join(Source, Source.id == EventEvidenceVersion.source_id)
                .where(SynthesisCitation.block_id == block.id)
                .order_by(SynthesisCitation.position)
            )
        ]
        blocks.append(
            SynthesisBlockOut(
                block_uid=block.uid,
                position=block.position,
                kind=block.kind,
                body=block.body,
                attribution=block.attribution,
                citations=citations,
            )
        )
    return SynthesisVersionOut(
        version_uid=version.uid,
        snapshot_uid=snapshot.uid,
        target_revision_uid=revision.uid,
        source_count=version.source_count,
        provider=version.provider,
        model=version.model,
        prompt_version=version.prompt_version,
        schema_version=version.schema_version,
        generation_fingerprint=version.generation_fingerprint,
        snapshot_created_at=snapshot.created_at,
        created_at=version.created_at,
        blocks=blocks,
    )


def synthesis_task_status(
    session: Session, event_id: int, generation_fingerprint: str | None
) -> str:
    if generation_fingerprint is None:
        return "idle"
    task = session.scalar(
        select(LLMTask)
        .where(
            LLMTask.task_type == SYNTHESIS_TASK_TYPE,
            LLMTask.object_type == "event",
            LLMTask.object_id == event_id,
            LLMTask.input_fingerprint == generation_fingerprint,
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
        .limit(1)
    )
    if task is None:
        return "idle"
    return (
        task.status
        if task.status in {"pending", "running", "failed", "complete"}
        else "idle"
    )


def task_request(task: LLMTask) -> dict[str, object]:
    try:
        value = json.loads(task.result_json)
    except json.JSONDecodeError:
        return {}
    request = value.get("request") if isinstance(value, dict) else None
    return request if isinstance(request, dict) else {}


def synthesis_payload(payload: dict[str, object]) -> dict[str, object]:
    if isinstance(payload.get("blocks"), list) or isinstance(
        payload.get("result"), str
    ):
        return payload
    text_value = llm_text(payload)
    try:
        parsed = json.loads(unfence_json(text_value))
    except json.JSONDecodeError:
        raise SynthesisValidationError("合成稿不是有效 JSON") from None
    if not isinstance(parsed, dict):
        raise SynthesisValidationError("合成稿不是 JSON 对象")
    return parsed


def synthesis_generation_fingerprint(
    source_coverage_fingerprint: str,
    content_fingerprint: str,
) -> str:
    return stable_hash(
        {
            "source_coverage_fingerprint": source_coverage_fingerprint,
            "content_fingerprint": content_fingerprint,
            "policy_version": SYNTHESIS_POLICY_VERSION,
            "prompt_version": SYNTHESIS_PROMPT_VERSION,
            "schema_version": SYNTHESIS_SCHEMA_VERSION,
        }
    )
