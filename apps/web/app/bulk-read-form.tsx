"use client";

import { useEffect, useRef, useState } from "react";
import { Check } from "lucide-react";

import {
  bulkReadConfirmationFromPrepared,
  bulkReadPreparationFailure,
  isBulkReadConfirmationReady,
  type FrozenBulkReadBatch,
  type BulkReadPrepared,
  type BulkReadScope
} from "./bulk-read";
import { queryString } from "./url-state";

type Scope = Record<string, string | number | null | undefined>;

export default function BulkReadForm({ objectType = "event", scope }: { objectType?: "event" | "item"; scope: Scope }) {
  const [confirming, setConfirming] = useState(false);
  const [preparing, setPreparing] = useState(false);
  const [batchId, setBatchId] = useState("");
  const [hint, setHint] = useState("");
  const batchRef = useRef<FrozenBulkReadBatch | null>(null);
  const preparingRef = useRef(false);
  const resetTimer = useRef<number | null>(null);

  useEffect(() => () => {
    if (resetTimer.current !== null) window.clearTimeout(resetTimer.current);
  }, []);

  return (
    <form
      className="bulk-read-form"
      action="/actions/bulk-read"
      method="post"
      onSubmit={async (event) => {
        if (isBulkReadConfirmationReady(batchRef.current)) return;
        event.preventDefault();
        if (preparingRef.current) return;
        preparingRef.current = true;
        setPreparing(true);
        setHint("");
        try {
          const response = await fetch("/actions/bulk-read/prepare", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(bulkReadScope(objectType, scope))
          });
          const payload = (await response.json()) as BulkReadPrepared & {
            error?: string;
          };
          if (!response.ok) {
            throw new Error(payload.error || "准备批量已读失败");
          }
          const confirmation = bulkReadConfirmationFromPrepared(payload);
          if (!isBulkReadConfirmationReady(confirmation.batch)) {
            setHint(confirmation.hint);
            return;
          }
          batchRef.current = confirmation.batch;
          setBatchId(confirmation.batch.batch_id);
          setConfirming(true);
          setHint(confirmation.hint);
          if (resetTimer.current !== null) window.clearTimeout(resetTimer.current);
          resetTimer.current = window.setTimeout(() => {
            batchRef.current = null;
            setBatchId("");
            setConfirming(false);
            setHint("");
          }, 5000);
        } catch (error) {
          const failure = bulkReadPreparationFailure(error);
          batchRef.current = failure.batch;
          setBatchId("");
          setConfirming(false);
          setHint(failure.hint);
        } finally {
          preparingRef.current = false;
          setPreparing(false);
        }
      }}
    >
      <input type="hidden" name="batch_id" value={batchId} />
      <input type="hidden" name="redirect" value={`/?${queryString({ ...scope, cluster_id: undefined, item_id: undefined, offset: undefined })}`} />
      <button className={confirming ? "icon confirming" : "icon"} type="submit" disabled={preparing} title={preparing ? "正在固定当前未读清单" : confirming ? "再次点击确认全部已读" : "当前范围全部已读"} aria-label={preparing ? "正在固定当前未读清单" : confirming ? "再次点击确认全部已读" : "当前范围全部已读"}>
        <Check size={17} />
      </button>
      {hint ? <span className="bulk-read-confirm-hint" role="status">{hint}</span> : null}
    </form>
  );
}

function bulkReadScope(
  objectType: "event" | "item",
  scope: Scope
): BulkReadScope {
  const payload: BulkReadScope = { object_type: objectType };
  if (typeof scope.folder_id === "number" && scope.folder_id > 0) {
    payload.folder_id = scope.folder_id;
  }
  if (typeof scope.source_id === "number" && scope.source_id > 0) {
    payload.source_id = scope.source_id;
  }
  if (typeof scope.media === "string" && scope.media.trim()) {
    payload.media_type = scope.media.trim();
  }
  if (typeof scope.q === "string" && scope.q.trim()) {
    payload.q = scope.q.trim();
  }
  return payload;
}
