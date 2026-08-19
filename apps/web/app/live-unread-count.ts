export const READER_UNREAD_COUNT_CHANGED_EVENT = "reader:unread-count-changed";

type UnreadState = {
  read_status: string;
  has_material_update: boolean;
};

export function effectiveUnreadCountDelta(previous: UnreadState, next: UnreadState) {
  return Number(isEffectivelyUnread(next)) - Number(isEffectivelyUnread(previous));
}

export function applyAllUnreadCountDelta<
  T extends { all_unread_count: number; unread_count: number }
>(sources: T[], delta: number) {
  const total = Math.max(
    0,
    (sources[0]?.all_unread_count ??
      sources.reduce((sum, source) => sum + source.unread_count, 0)) + delta
  );
  return sources.map((source) => ({ ...source, all_unread_count: total }));
}

export function dispatchReaderUnreadCountChanged(delta: number) {
  if (delta === 0) return;
  window.dispatchEvent(
    new window.CustomEvent<number>(READER_UNREAD_COUNT_CHANGED_EVENT, { detail: delta })
  );
}

function isEffectivelyUnread(state: UnreadState) {
  return state.read_status === "unread" || state.has_material_update;
}
