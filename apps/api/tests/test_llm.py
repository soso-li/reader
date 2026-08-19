import json
import traceback
from io import BytesIO
from urllib.error import HTTPError

import pytest

from reader_api.llm import LocalChatProvider, LocalEmbeddingProvider, OpenAICompatibleChatProvider, embedding_endpoint_for, embedding_from_response, embeddings_from_response


class ChatResponse:
    def __enter__(self) -> "ChatResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        body = b'{"reply":"ok"}'
        return body if size < 0 else body[:size]


class DuplicateDataResponse(ChatResponse):
    def read(self, size: int = -1) -> bytes:
        body = b'{"data":[{"embedding":[1]}],"data":[{"embedding":[2]}]}'
        return body if size < 0 else body[:size]


def test_local_chat_provider_posts_configured_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: float) -> ChatResponse:
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return ChatResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    result = LocalChatProvider("http://example.local:1234", 7).chat("model-a", "system", "hello")

    assert captured == {
        "url": "http://example.local:1234/api/v1/chat",
        "timeout": 7,
        "body": {"model": "model-a", "system_prompt": "system", "input": "hello"},
    }
    assert result == {"reply": "ok"}


def test_local_chat_provider_can_send_reasoning(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: float) -> ChatResponse:
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return ChatResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    LocalChatProvider("http://example.local:1234", 7, reasoning="on").chat("model-a", "system", "hello")

    assert captured["body"] == {"model": "model-a", "system_prompt": "system", "input": "hello", "reasoning": "on"}


def test_openai_compatible_chat_provider_posts_bearer_chat_completion(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class OpenAIResponse(ChatResponse):
        def read(self, size: int = -1) -> bytes:
            body = b'{"choices":[{"message":{"content":"translated"}}]}'
            return body if size < 0 else body[:size]

    def fake_urlopen(request, timeout: float) -> OpenAIResponse:
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["authorization"] = request.get_header("Authorization")
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return OpenAIResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    result = OpenAICompatibleChatProvider("https://cloud.example/v1", 11, "cloud-secret").chat("model-cloud", "translate", "hello")

    assert captured == {
        "url": "https://cloud.example/v1/chat/completions",
        "timeout": 11,
        "authorization": "Bearer cloud-secret",
        "body": {
            "model": "model-cloud",
            "messages": [
                {"role": "system", "content": "translate"},
                {"role": "user", "content": "hello"},
            ],
        },
    }
    assert result == {"choices": [{"message": {"content": "translated"}}]}


@pytest.mark.parametrize(
    ("service_label", "expected"),
    [
        ("翻译", "云端翻译服务不可用，请检查地址、模型和密钥"),
        ("合成", "云端合成服务不可用，请检查地址、模型和密钥"),
        ("Authorization: Bearer injected", "云端模型服务不可用，请检查地址、模型和密钥"),
    ],
)
def test_openai_compatible_chat_provider_sanitizes_upstream_error(
    monkeypatch, service_label: str, expected: str
) -> None:
    api_key = "cloud-" + "secret"

    def fake_urlopen(request, timeout: float) -> ChatResponse:
        raise HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            {},
            BytesIO(b'{"error":"Authorization: Bearer cloud-secret"}'),
        )

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)
    provider = OpenAICompatibleChatProvider(
        "https://cloud.example", 11, api_key, service_label=service_label
    )

    with pytest.raises(RuntimeError) as error:
        provider.chat("model-cloud", "translate", "hello")

    assert str(error.value) == expected
    assert api_key not in str(error.value)
    assert error.value.__cause__ is None
    assert api_key not in "".join(traceback.format_exception(error.value))


def test_openai_compatible_chat_provider_rejects_unsafe_key_without_echoing_it() -> None:
    api_key = "cloud-secret\nAuthorization: Bearer leaked-secret"
    provider = OpenAICompatibleChatProvider("https://cloud.example", 11, api_key)

    with pytest.raises(RuntimeError) as error:
        provider.chat("model-cloud", "translate", "hello")

    assert str(error.value) == "云端翻译服务不可用，请检查地址、模型和密钥"
    assert api_key not in str(error.value)
    assert "leaked-secret" not in "".join(traceback.format_exception(error.value))


def test_local_embedding_provider_posts_embeddings_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout: float) -> ChatResponse:
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["authorization"] = request.get_header("Authorization")
        return ChatResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    result = LocalEmbeddingProvider("http://example.local:8000/v1", 7, api_key="secret").embed("embed-a", "hello")

    assert captured == {
        "url": "http://example.local:8000/v1/embeddings",
        "timeout": 7,
        "body": {"model": "embed-a", "input": "hello"},
        "authorization": "Bearer secret",
    }
    assert result == {"reply": "ok"}


def test_model_provider_rejects_oversized_response(monkeypatch) -> None:
    class OversizedResponse(ChatResponse):
        def read(self, size: int = -1) -> bytes:
            return b"x" * size

    monkeypatch.setattr("reader_api.llm.MAX_MODEL_RESPONSE_BYTES", 8)
    monkeypatch.setattr(
        "reader_api.llm.urlopen",
        lambda *_args, **_kwargs: OversizedResponse(),
    )

    with pytest.raises(RuntimeError, match="响应超过大小上限"):
        LocalEmbeddingProvider(
            "http://example.local:8000/v1", 7, retry_delays=()
        ).embed("embed-a", "hello")


def test_local_embedding_provider_posts_batch_embeddings_payload(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    def fake_urlopen(request, timeout: float) -> ChatResponse:
        captured.append({"url": request.full_url, "timeout": timeout, "body": json.loads(request.data.decode("utf-8"))})
        return ChatResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    result = LocalEmbeddingProvider("http://example.local:1234", 7).embed_many("embed-a", ["hello", "world"])

    assert captured == [
        {"url": "http://example.local:1234/v1/embeddings", "timeout": 7, "body": {"model": "embed-a", "input": ["hello", "world"]}},
    ]
    assert result == {"reply": "ok"}


def test_local_embedding_provider_merges_duplicate_data_keys(monkeypatch) -> None:
    def fake_urlopen(request, timeout: float) -> DuplicateDataResponse:
        return DuplicateDataResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    result = LocalEmbeddingProvider("http://example.local:1234", 7).embed_many("embed-a", ["hello", "world"])

    assert embeddings_from_response(result) == [[1.0], [2.0]]


def test_local_embedding_provider_retries_transient_lmstudio_unload(monkeypatch) -> None:
    calls = 0

    def fake_urlopen(request, timeout: float) -> ChatResponse:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise HTTPError(
                request.full_url,
                400,
                "Bad Request",
                {},
                BytesIO(b'{"error":"Model was unloaded while the request was still in queue.."}'),
            )
        return ChatResponse()

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)
    monkeypatch.setattr("reader_api.llm.time.sleep", lambda _: None)

    result = LocalEmbeddingProvider("http://example.local:1234", 7, retry_delays=(0,)).embed("embed-a", "hello")

    assert calls == 2
    assert result == {"reply": "ok"}


def test_local_embedding_provider_error_includes_response_body(monkeypatch) -> None:
    def fake_urlopen(request, timeout: float) -> ChatResponse:
        raise HTTPError(
            request.full_url,
            400,
            "Bad Request",
            {},
            BytesIO(b'{"error":"bad model"}'),
        )

    monkeypatch.setattr("reader_api.llm.urlopen", fake_urlopen)

    try:
        LocalEmbeddingProvider("http://example.local:1234", 7, retry_delays=()).embed("embed-a", "hello")
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected RuntimeError")

    assert "HTTP Error 400" in message
    assert "bad model" in message


def test_embedding_endpoint_accepts_root_or_v1_base_url() -> None:
    assert embedding_endpoint_for("http://example.local:1234") == "http://example.local:1234/v1/embeddings"
    assert embedding_endpoint_for("http://example.local:8000/v1") == "http://example.local:8000/v1/embeddings"


def test_embedding_from_response_accepts_common_shapes() -> None:
    assert embedding_from_response({"data": [{"embedding": [1, "2.5"]}]}) == [1.0, 2.5]
    assert embedding_from_response({"text": '{"embedding":[0.1,0.2]}'}) == [0.1, 0.2]
    assert embedding_from_response({"response": "[0.3,0.4]"}) == [0.3, 0.4]
    assert embedding_from_response({"choices": [{"message": {"content": '{"vector":[0.5,0.6]}'}}]}) == [0.5, 0.6]
    assert embedding_from_response({"output": [{"type": "message", "content": "[0.7,0.8]"}]}) == [0.7, 0.8]


def test_embeddings_from_response_accepts_openai_batch_shape() -> None:
    assert embeddings_from_response({"data": [{"embedding": [1, "2.5"]}, {"embedding": ["3", 4]}]}) == [[1.0, 2.5], [3.0, 4.0]]
