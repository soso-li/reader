type SourceLifecycle = { enabled: boolean; status: string };

export function isLegacySourceStatus(source: Pick<SourceLifecycle, "status">) {
  return source.status === "muted" || source.status === "archived";
}

export function isSourcePaused(source: SourceLifecycle) {
  return !source.enabled || isLegacySourceStatus(source);
}
