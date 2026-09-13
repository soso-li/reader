export const PAGE_FETCH_TIMEOUT_MS = 15000;

export function withPageTimeout(signal: AbortSignal, timeoutMs = PAGE_FETCH_TIMEOUT_MS): AbortSignal {
  return AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]);
}
