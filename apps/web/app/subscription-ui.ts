import { userFacingErrorMessage } from "./lib/api";

export function dialogControls(dialog: HTMLElement | null) {
  return Array.from(dialog?.querySelectorAll<HTMLElement>('a[href], button:not([disabled]), input:not([disabled]), select:not([disabled])') ?? []).filter((element) => !element.hidden);
}

export function recentEntryLabel(count: number) {
  return count > 0 ? `近 30 天 ${count} 条` : "近 30 天无更新";
}

export function errorMessage(error: unknown) {
  return userFacingErrorMessage(error, "操作失败");
}
