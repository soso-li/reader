import type { EventReadTarget } from "./event-user-state.ts";
import type { EventGenerationTaskStatus } from "./generation-task-status.ts";
import type { SourceMediaType } from "./source-media.ts";

export type SynthesisCitation = {
  evidence_version_uid: string;
  evidence_type: string;
  role: string;
  side: string;
  source: {
    source_id: number;
    name: string;
    feed_url: string;
    site_url: string;
    media_type: SourceMediaType;
  };
  legacy_content_item_id_snapshot: number;
  title: string;
  url: string;
  published_at: string | null;
};

export type SynthesisBlock = {
  block_uid: string;
  position: number;
  kind: "summary" | "fact" | "viewpoint" | "disagreement" | "uncertainty";
  body: string;
  attribution: string;
  citations: SynthesisCitation[];
};

export type EventSynthesisState = {
  status: "missing" | "current" | "unreviewed" | "stale";
  current_revision_uid: string;
  covered_revision_uid: string | null;
  reviewed_revision_uid: string | null;
  new_source_count: number;
  unreviewed_evidence_count: number;
  unreviewed_source_count: number;
  target_revision_uid: string;
  source_view_revision_uid: string;
  source_count: number;
  can_generate: boolean;
  default_view: "synthesis" | "source";
  task_status: EventGenerationTaskStatus;
  task: { admission_reason: string; privacy_reason: string } | null;
  current: {
    version_uid: string;
    snapshot_uid: string;
    target_revision_uid: string;
    source_count: number;
    provider: string;
    model: string;
    prompt_version: string;
    schema_version: string;
    generation_fingerprint: string;
    snapshot_created_at: string;
    created_at: string;
    blocks: SynthesisBlock[];
  } | null;
};

export type SourceViewEvidence = {
  evidence_version_uid: string;
  source_id: number;
  legacy_content_item_id_snapshot: number;
};

function renderedEventRevisionUid(
  synthesis:
    | Pick<EventSynthesisState, "source_view_revision_uid" | "current">
    | null
    | undefined,
  mode: "synthesis" | "source",
  fallbackCurrentRevisionUid: string | null | undefined
): string | null {
  if (mode === "synthesis" && synthesis?.current?.target_revision_uid) {
    return synthesis.current.target_revision_uid;
  }
  return synthesis?.source_view_revision_uid ?? fallbackCurrentRevisionUid ?? null;
}

export function renderedEventReadTarget(
  eventUid: string | null | undefined,
  synthesis:
    | Pick<EventSynthesisState, "source_view_revision_uid" | "current">
    | null
    | undefined,
  mode: "synthesis" | "source",
  fallbackCurrentRevisionUid: string | null | undefined
): EventReadTarget {
  return {
    event_uid: eventUid ?? null,
    observed_revision_uid: renderedEventRevisionUid(
      synthesis,
      mode,
      fallbackCurrentRevisionUid
    )
  };
}

type OriginalOpenedSource = {
  source_id: number;
  item_id?: number;
  evidence_version_uid?: string;
  url?: string;
};

type OriginalOpenedSynthesis = {
  source_view_revision_uid: string;
  current: {
    target_revision_uid: string;
    blocks: Array<{
      citations: Array<{
        evidence_version_uid: string;
        legacy_content_item_id_snapshot: number;
        source: { source_id: number };
        url: string;
      }>;
    }>;
  } | null;
};

export type OriginalOpenedSelection = {
  item_id: number;
  url: string;
  target: EventReadTarget;
};

export function originalOpenedSelectionForView({
  event_uid,
  synthesis,
  current_revision_uid,
  mode,
  source_view_evidence,
  source,
  fallback_to_first_synthesis_evidence = false
}: {
  event_uid: string | null | undefined;
  synthesis: OriginalOpenedSynthesis | null | undefined;
  current_revision_uid: string | null | undefined;
  mode: "synthesis" | "source";
  source_view_evidence: SourceViewEvidence[] | null | undefined;
  source?: OriginalOpenedSource;
  fallback_to_first_synthesis_evidence?: boolean;
}): OriginalOpenedSelection | null {
  if (!event_uid) return null;
  if (mode === "source") {
    if (!source?.item_id || !source.url) return null;
    const evidence = source_view_evidence?.find(
      (candidate) =>
        candidate.source_id === source.source_id &&
        candidate.legacy_content_item_id_snapshot === source.item_id
    );
    const revisionUid =
      synthesis?.source_view_revision_uid ?? current_revision_uid ?? null;
    if (!evidence || !revisionUid) return null;
    return {
      item_id: source.item_id,
      url: source.url,
      target: {
        event_uid,
        observed_revision_uid: revisionUid,
        evidence: {
          source_id: evidence.source_id,
          evidence_version_uid: evidence.evidence_version_uid
        }
      }
    };
  }

  if (!synthesis?.current) return null;
  const citations = synthesis.current.blocks.flatMap((block) => block.citations);
  const citation = source?.evidence_version_uid
    ? citations.find(
        (candidate) =>
          source.evidence_version_uid === candidate.evidence_version_uid
      )
    : citations.find(
        (candidate) =>
          candidate.source.source_id === source?.source_id &&
          candidate.legacy_content_item_id_snapshot === source.item_id
      ) ?? (fallback_to_first_synthesis_evidence ? citations[0] : undefined);
  if (!citation) return null;
  return {
    item_id: citation.legacy_content_item_id_snapshot,
    url: citation.url,
    target: {
      event_uid,
      observed_revision_uid: synthesis.current.target_revision_uid,
      evidence: {
        source_id: citation.source.source_id,
        evidence_version_uid: citation.evidence_version_uid
      }
    }
  };
}

export type ClusterSynthesisFields = {
  synthesis_freshness?: Pick<
    EventSynthesisState,
    | "status"
    | "current_revision_uid"
    | "covered_revision_uid"
    | "reviewed_revision_uid"
    | "new_source_count"
    | "unreviewed_evidence_count"
    | "unreviewed_source_count"
  > | null;
  synthesis?: EventSynthesisState | null;
};

export function synthesisViewAvailable(
  synthesis:
    | Pick<EventSynthesisState, "source_count" | "current">
    | null
    | undefined
) {
  return Boolean(synthesis && (synthesis.source_count > 1 || synthesis.current));
}

export function synthesisRequestAvailable(
  synthesis: Pick<EventSynthesisState, "status" | "can_generate" | "task_status">
) {
  return (
    synthesis.can_generate &&
    (synthesis.status === "missing" ||
      synthesis.status === "unreviewed" ||
      synthesis.status === "stale") &&
    synthesis.task_status !== "pending" &&
    synthesis.task_status !== "running" &&
    synthesis.task_status !== "canceled" &&
    synthesis.task_status !== "apply_pending" &&
    synthesis.task_status !== "apply_failed"
  );
}

export function synthesisStatusLabel(
  status: EventSynthesisState["status"] | null | undefined,
  newSourceCount = 0,
  hasMaterialUpdate = false
) {
  if (hasMaterialUpdate) return "看过后有更新";
  if (status === "unreviewed") return "有未审新证据";
  if (status === "stale") return "有新证据尚未纳入";
  return status === "current" && newSourceCount > 0
    ? `新增 ${newSourceCount} 个来源`
    : "";
}

type CitationSourceItem = {
  id: number;
  source_id: number;
  url: string;
};

export function citationTarget(
  citation: SynthesisCitation,
  items: CitationSourceItem[],
  sourceViewEvidence: SourceViewEvidence[]
):
  | { kind: "source"; itemId: number }
  | { kind: "external"; url: string; sourceId: number } {
  const evidence = sourceViewEvidence.find(
    (candidate) =>
      candidate.evidence_version_uid === citation.evidence_version_uid &&
      candidate.source_id === citation.source.source_id &&
      candidate.legacy_content_item_id_snapshot ===
        citation.legacy_content_item_id_snapshot
  );
  const item =
    evidence &&
    items.find(
      (candidate) =>
        candidate.id === evidence.legacy_content_item_id_snapshot &&
        candidate.source_id === evidence.source_id
    );
  return item
    ? { kind: "source", itemId: item.id }
    : {
        kind: "external",
        url: citation.url,
        sourceId: citation.source.source_id
      };
}
