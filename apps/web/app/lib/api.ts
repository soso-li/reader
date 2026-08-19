const API_INTERNAL_URL = process.env.API_INTERNAL_URL || process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8007";
const LOCAL_MODEL_ERROR = "本地模型服务未连接，请检查 LM Studio";
const CLOUD_TRANSLATION_ERROR = "云端翻译服务不可用，请检查地址、模型和密钥";
const DEFAULT_ERROR = "操作失败，请稍后重试";

export class ApiFetchError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiFetchError";
    this.status = status;
  }
}

export function apiUrl(path: string) {
  return `${API_INTERNAL_URL}${path}`;
}

export function apiRequest(path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  const token = process.env.READER_API_TOKEN?.trim();
  if (token) headers.set("X-Reader-API-Token", token);
  return fetch(apiUrl(path), { ...init, headers, cache: "no-store" });
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await apiRequest(path, init);
  if (!response.ok) {
    const message = await response.text();
    let parsed: { detail?: unknown; error?: unknown } | null = null;
    try {
      parsed = JSON.parse(message) as { detail?: unknown; error?: unknown };
    } catch {
      parsed = null;
    }
    const detail = typeof parsed?.detail === "string" ? parsed.detail : typeof parsed?.error === "string" ? parsed.error : message;
    throw new ApiFetchError(
      userFacingErrorMessage(detail, `请求失败（${response.status}）`),
      response.status
    );
  }
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export function apiErrorStatus(error: unknown, fallback = 502) {
  return error instanceof ApiFetchError ? error.status : fallback;
}

export function userFacingErrorMessage(error: unknown, fallback = DEFAULT_ERROR) {
  if (error instanceof SyntaxError || error instanceof TypeError) return fallback;
  const message = errorText(error).replace(/\s+/g, " ").trim();
  if (!message) return fallback;
  if (message === CLOUD_TRANSLATION_ERROR) return CLOUD_TRANSLATION_ERROR;
  if (isModelServiceError(message)) return LOCAL_MODEL_ERROR;
  if (isRawException(message)) return fallback;
  return message;
}

function errorText(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error ?? "");
  try {
    const parsed = JSON.parse(message) as { detail?: unknown; error?: unknown };
    const detail = parsed.detail || parsed.error;
    return typeof detail === "string" ? detail : "";
  } catch {
    return message;
  }
}

function isModelServiceError(message: string) {
  return /(?:LLM|Assistant|Embedding|翻译|模型|LM Studio|服务不可用)/i.test(message) && /(?:服务不可用|URLError|URL error|ConnectionError|ECONNREFUSED|Connection refused|timed out|\[Errno \d+\]|urlopen|http:\/\/|https:\/\/)/i.test(message);
}

function isRawException(message: string) {
  return /(?:Traceback|HTTPConnectionPool|URLError|URL error|ConnectionError|ECONNREFUSED|ENOTFOUND|fetch failed|Failed to fetch|Max retries exceeded|Connection refused|timed out|\[Errno \d+\]|urllib|httpx|requests\.|TypeError|SyntaxError|RuntimeError|ValidationError)/i.test(message);
}
