import json
import re
import time
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_MODEL_RETRY_DELAYS = (2.0, 5.0, 10.0)
MAX_MODEL_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_MODEL_ERROR_BYTES = 64 * 1024
TRANSIENT_MODEL_ERROR_MARKERS = (
    "model was unloaded while the request was still in queue",
    "failed to load model",
    "operation canceled",
)
HEADER_SAFE_BEARER_TOKEN_RE = re.compile(r"[\x21-\x7e]+")


class LLMProvider(Protocol):
    def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
        ...


class EmbeddingProvider(Protocol):
    def embed(self, model: str, input_text: str) -> dict[str, object]:
        ...

    def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
        ...


class LocalChatProvider:
    provider_name = "local"

    def __init__(self, base_url: str, timeout_seconds: float, reasoning: str | None = None, retry_delays: tuple[float, ...] = DEFAULT_MODEL_RETRY_DELAYS) -> None:
        self.endpoint = f"{base_url.rstrip('/')}/api/v1/chat"
        self.timeout_seconds = timeout_seconds
        self.reasoning = reasoning
        self.retry_delays = retry_delays

    def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
        payload = {"model": model, "system_prompt": system_prompt, "input": input_text}
        if self.reasoning:
            payload["reasoning"] = self.reasoning
        return post_json(self.endpoint, payload, self.timeout_seconds, "LLM 服务不可用", retry_delays=self.retry_delays)


def openai_chat_endpoint_for(base_url: str) -> str:
    base = base_url.rstrip("/")
    return f"{base}/chat/completions" if base.endswith("/v1") else f"{base}/v1/chat/completions"


class OpenAICompatibleChatProvider:
    provider_name = "openai_compatible"

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        api_key: str,
        *,
        service_label: str = "翻译",
    ) -> None:
        self.endpoint = openai_chat_endpoint_for(base_url)
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key
        safe_label = service_label if service_label in {"翻译", "合成"} else "模型"
        self.failure_message = f"云端{safe_label}服务不可用，请检查地址、模型和密钥"

    def chat(self, model: str, system_prompt: str, input_text: str) -> dict[str, object]:
        if not is_header_safe_bearer_token(self.api_key):
            raise RuntimeError(self.failure_message) from None
        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": input_text})
        try:
            return post_json(
                self.endpoint,
                {"model": model, "messages": messages},
                self.timeout_seconds,
                "云端翻译服务不可用",
                headers={"Authorization": f"Bearer {self.api_key}"},
                retry_delays=(),
            )
        except RuntimeError:
            raise RuntimeError(self.failure_message) from None


def is_header_safe_bearer_token(value: str) -> bool:
    return bool(HEADER_SAFE_BEARER_TOKEN_RE.fullmatch(value))


def embedding_endpoint_for(base_url: str) -> str:
    base = base_url.rstrip("/")
    return f"{base}/embeddings" if base.endswith("/v1") else f"{base}/v1/embeddings"


class LocalEmbeddingProvider:
    def __init__(self, base_url: str, timeout_seconds: float, api_key: str = "", retry_delays: tuple[float, ...] = DEFAULT_MODEL_RETRY_DELAYS) -> None:
        self.endpoint = embedding_endpoint_for(base_url)
        self.timeout_seconds = timeout_seconds
        self.api_key = api_key
        self.retry_delays = retry_delays

    def embed(self, model: str, input_text: str) -> dict[str, object]:
        return self._post({"model": model, "input": input_text})

    def embed_many(self, model: str, input_texts: list[str]) -> dict[str, object]:
        if not input_texts:
            return {"data": []}
        return self._post({"model": model, "input": input_texts})

    def _post(self, payload: dict[str, object]) -> dict[str, object]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return post_json(self.endpoint, payload, self.timeout_seconds, "Embedding 服务不可用", headers=headers, retry_delays=self.retry_delays)


def post_json(endpoint: str, payload: dict[str, object], timeout_seconds: float, error_prefix: str, headers: dict[str, str] | None = None, retry_delays: tuple[float, ...] = DEFAULT_MODEL_RETRY_DELAYS) -> dict[str, object]:
    body = json.dumps(payload).encode("utf-8")
    request_headers = {"Content-Type": "application/json", **(headers or {})}
    attempts = len(retry_delays) + 1
    for attempt in range(attempts):
        request = Request(endpoint, data=body, headers=request_headers, method="POST")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                response_body = response.read(MAX_MODEL_RESPONSE_BYTES + 1)
                if len(response_body) > MAX_MODEL_RESPONSE_BYTES:
                    raise RuntimeError(f"{error_prefix}: 响应超过大小上限")
                raw = response_body.decode("utf-8")
        except HTTPError as exc:
            detail = http_error_detail(exc)
            if attempt < len(retry_delays) and is_transient_model_error(detail):
                time.sleep(retry_delays[attempt])
                continue
            raise RuntimeError(f"{error_prefix}: {detail}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"{error_prefix}: {exc}") from exc
        try:
            parsed = json.loads(raw, object_pairs_hook=merge_duplicate_json_keys)
        except json.JSONDecodeError:
            return {"text": raw}
        return parsed if isinstance(parsed, dict) else {"result": parsed}
    raise RuntimeError(f"{error_prefix}: retry attempts exhausted")


def merge_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    merged: dict[str, object] = {}
    for key, value in pairs:
        existing = merged.get(key)
        if isinstance(existing, list) and isinstance(value, list):
            merged[key] = existing + value
        else:
            merged[key] = value
    return merged


def http_error_detail(exc: HTTPError) -> str:
    try:
        raw = exc.read(MAX_MODEL_ERROR_BYTES + 1)
        body = (
            raw.decode("utf-8", errors="replace").strip()
            if len(raw) <= MAX_MODEL_ERROR_BYTES
            else ""
        )
    except Exception:
        body = ""
    return f"{exc}: {body}" if body else str(exc)


def is_transient_model_error(detail: str) -> bool:
    normalized = detail.lower()
    return any(marker in normalized for marker in TRANSIENT_MODEL_ERROR_MARKERS)


def embedding_from_response(result: dict[str, object]) -> list[float]:
    value = find_embedding_value(result)
    if isinstance(value, str):
        try:
            value = json.loads(value, object_pairs_hook=merge_duplicate_json_keys)
        except json.JSONDecodeError:
            return []
        if isinstance(value, dict):
            return embedding_from_response(value)
    if not isinstance(value, list):
        return []
    try:
        return [float(item) for item in value]
    except (TypeError, ValueError):
        return []


def embeddings_from_response(result: dict[str, object]) -> list[list[float]]:
    data = result.get("data")
    if isinstance(data, list) and data and all(isinstance(item, dict) for item in data):
        return [embedding_from_response(item) for item in data]
    vector = embedding_from_response(result)
    return [vector] if vector else []


def find_embedding_value(value: object) -> object:
    if isinstance(value, list):
        if all(not isinstance(item, (dict, list)) for item in value):
            return value
        for item in value:
            candidate = find_embedding_value(item)
            if candidate is not None:
                return candidate
        return None
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return None
    for key in ("embedding", "embeddings", "vector", "result", "output", "response", "answer", "message", "content"):
        candidate = value.get(key)
        if isinstance(candidate, (dict, list)):
            found = find_embedding_value(candidate)
            if found is not None:
                return found
        if isinstance(candidate, str):
            return candidate
    for key in ("data", "choices"):
        data = value.get(key)
        if isinstance(data, list) and data:
            return find_embedding_value(data[0])
    text = value.get("text")
    if isinstance(text, str):
        return text
    return None
