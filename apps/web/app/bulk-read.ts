import { userFacingErrorMessage } from "./lib/api.ts";

export type BulkReadPrepared = {
  batch_id?: string;
  target_count: number;
};

export type FrozenBulkReadBatch = Readonly<{
  batch_id: string;
}>;

export type BulkReadConfirmation = {
  batch: FrozenBulkReadBatch | null;
  hint: string;
};

export type BulkReadScope = {
  object_type: "event" | "item";
  folder_id?: number;
  source_id?: number;
  media_type?: string;
  q?: string;
};

export function freezeBulkReadBatch(batchId: string): FrozenBulkReadBatch {
  const batch_id = batchId.trim();
  if (!batch_id) throw new Error("批量已读批次 ID 为空");
  return Object.freeze({ batch_id });
}

export function serializeBulkReadBatch(batch: FrozenBulkReadBatch): string {
  return JSON.stringify(batch);
}

export function bulkReadConfirmationFromPrepared(
  prepared: BulkReadPrepared
): BulkReadConfirmation {
  if (prepared.target_count === 0) {
    return { batch: null, hint: "当前范围没有未读内容" };
  }
  if (!prepared.batch_id) {
    throw new Error("批量已读准备结果缺少批次 ID");
  }
  return {
    batch: freezeBulkReadBatch(prepared.batch_id),
    hint: `将标记 ${prepared.target_count} 条，再点确认`
  };
}

export function bulkReadPreparationFailure(error: unknown): BulkReadConfirmation {
  return {
    batch: null,
    hint: userFacingErrorMessage(error, "准备批量已读失败")
  };
}

export function isBulkReadConfirmationReady(
  batch: FrozenBulkReadBatch | null
): batch is FrozenBulkReadBatch {
  return batch !== null;
}

export async function confirmBulkReadWithRetry<T>(
  batch: FrozenBulkReadBatch,
  send: (body: string) => Promise<T>,
  shouldRetry: (error: unknown) => boolean
): Promise<T> {
  const body = serializeBulkReadBatch(batch);
  try {
    return await send(body);
  } catch (error) {
    if (!shouldRetry(error)) throw error;
    return send(body);
  }
}
