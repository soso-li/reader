from collections.abc import Callable
from dataclasses import dataclass, replace
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import AI_MODEL_NAME_MAX_LENGTH, settings
from .llm import LLMProvider, LocalChatProvider, OpenAICompatibleChatProvider, is_header_safe_bearer_token, openai_chat_endpoint_for
from .models import AppSetting, Source, now_utc

LOCAL_TRANSLATION_PROVIDER = "local"
CLOUD_TRANSLATION_PROVIDER = "openai_compatible"


class TranslationSettingsError(RuntimeError):
    pass


class SynthesisSettingsError(RuntimeError):
    pass


def valid_model_base_url(value: str, *, https_only: bool = False) -> bool:
    try:
        parsed = urlsplit(value)
        parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in ({"https"} if https_only else {"http", "https"})
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


@dataclass(frozen=True)
class RuntimeAISettings:
    task_provider: str
    llm_base_url: str
    translation_provider: str
    translation_base_url: str
    translation_api_key: str
    embedding_base_url: str
    llm_model: str
    translation_model: str
    embedding_model: str
    timeout_seconds: float
    translation_local_base_url: str = ""
    translation_local_model: str = ""
    translation_cloud_base_url: str = ""
    translation_cloud_model: str = ""
    synthesis_remote_base_url: str = ""
    synthesis_remote_model: str = ""
    synthesis_remote_api_key: str = ""
    synthesis_provider: str = "local"


@dataclass(frozen=True)
class TranslationProviderSpec:
    base_field: str
    model_field: str
    default_base_url: str
    default_model: str
    endpoint: Callable[[str], str]
    create_provider: Callable[[RuntimeAISettings], LLMProvider]
    requires_https: bool = False
    requires_key: bool = False


SETTING_KEYS = {
    "llm_task_provider",
    "synthesis_provider",
    "llm_base_url",
    "translation_provider",
    "translation_base_url",
    "translation_api_key",
    "translation_local_base_url",
    "translation_local_model",
    "translation_cloud_base_url",
    "translation_cloud_model",
    "embedding_base_url",
    "llm_model",
    "translation_model",
    "embedding_model",
    "llm_timeout_seconds",
    "synthesis_remote_base_url",
    "synthesis_remote_model",
    "synthesis_remote_api_key",
}


def runtime_ai_settings(session: Session) -> RuntimeAISettings:
    info = getattr(session, "info", None)
    cached = info.get("runtime_ai_settings") if isinstance(info, dict) else None
    if isinstance(cached, RuntimeAISettings):
        return cached
    default_local_base_url = settings.translation_base_url or settings.llm_base_url
    values = {
        "llm_task_provider": settings.llm_task_provider,
        "synthesis_provider": "",
        "llm_base_url": settings.llm_base_url,
        "translation_provider": settings.translation_provider,
        "translation_base_url": default_local_base_url,
        "translation_api_key": "",
        "translation_local_base_url": "",
        "translation_local_model": "",
        "translation_cloud_base_url": "",
        "translation_cloud_model": "",
        "embedding_base_url": settings.embedding_base_url,
        "llm_model": settings.llm_model,
        "translation_model": settings.translation_model,
        "embedding_model": settings.embedding_model,
        "llm_timeout_seconds": str(settings.llm_timeout_seconds),
        "synthesis_remote_base_url": "",
        "synthesis_remote_model": "",
        "synthesis_remote_api_key": "",
    }
    if hasattr(session, "scalars"):
        rows = session.scalars(select(AppSetting).where(AppSetting.key.in_(SETTING_KEYS))).all()
        for row in rows:
            normalized_value = row.value.strip()
            if (
                row.key in {"llm_model", "synthesis_remote_model"}
                and len(normalized_value) > AI_MODEL_NAME_MAX_LENGTH
            ):
                continue
            if (
                row.key in {"translation_api_key", "synthesis_remote_api_key"}
                or normalized_value
            ):
                values[row.key] = normalized_value
    translation_provider = values["translation_provider"].strip().lower()
    active_profile = translation_provider_spec(translation_provider)
    legacy_base_url = values["translation_base_url"].rstrip("/")
    legacy_model = values["translation_model"]
    profiles: dict[str, tuple[str, str]] = {}
    for provider, profile in TRANSLATION_PROVIDER_SPECS.items():
        fallback_base_url = legacy_base_url if profile is active_profile else profile.default_base_url
        fallback_model = legacy_model if profile is active_profile else profile.default_model
        profiles[provider] = (
            (values[profile.base_field] or fallback_base_url).rstrip("/"),
            values[profile.model_field] or fallback_model,
        )
    active_base_url, active_model = profiles[translation_provider]
    local_base_url, local_model = profiles[LOCAL_TRANSLATION_PROVIDER]
    cloud_base_url, cloud_model = profiles[CLOUD_TRANSLATION_PROVIDER]
    task_provider = values["llm_task_provider"].strip().lower()
    if task_provider not in {"local", "openai_compatible"}:
        task_provider = "local"
    synthesis_provider = values["synthesis_provider"].strip().lower()
    if synthesis_provider not in {"local", "openai_compatible"}:
        synthesis_provider = task_provider
    result = RuntimeAISettings(
        task_provider=task_provider,
        llm_base_url=values["llm_base_url"].rstrip("/"),
        translation_provider=translation_provider,
        translation_base_url=active_base_url,
        translation_api_key=values["translation_api_key"],
        embedding_base_url=values["embedding_base_url"].rstrip("/"),
        llm_model=values["llm_model"],
        translation_model=active_model,
        embedding_model=values["embedding_model"],
        timeout_seconds=float(values["llm_timeout_seconds"]),
        translation_local_base_url=local_base_url,
        translation_local_model=local_model,
        translation_cloud_base_url=cloud_base_url,
        translation_cloud_model=cloud_model,
        synthesis_remote_base_url=values["synthesis_remote_base_url"].rstrip("/"),
        synthesis_remote_model=values["synthesis_remote_model"],
        synthesis_remote_api_key=values["synthesis_remote_api_key"],
        synthesis_provider=synthesis_provider,
    )
    if isinstance(info, dict):
        info["runtime_ai_settings"] = result
    return result


def save_ai_settings(session: Session, changes: dict[str, str | float]) -> None:
    key_map = {
        "task_provider": "llm_task_provider",
        "synthesis_provider": "synthesis_provider",
        "base_url": "llm_base_url",
        "translation_provider": "translation_provider",
        "translation_base_url": "translation_base_url",
        "translation_api_key": "translation_api_key",
        "translation_local_base_url": "translation_local_base_url",
        "translation_local_model": "translation_local_model",
        "translation_cloud_base_url": "translation_cloud_base_url",
        "translation_cloud_model": "translation_cloud_model",
        "embedding_base_url": "embedding_base_url",
        "llm_model": "llm_model",
        "translation_model": "translation_model",
        "embedding_model": "embedding_model",
        "timeout_seconds": "llm_timeout_seconds",
        "synthesis_remote_base_url": "synthesis_remote_base_url",
        "synthesis_remote_model": "synthesis_remote_model",
        "synthesis_remote_api_key": "synthesis_remote_api_key",
    }
    for field, value in changes.items():
        key = key_map[field]
        row = session.get(AppSetting, key) or AppSetting(key=key)
        row.value = str(value).strip()
        row.updated_at = now_utc()
        session.add(row)
    session.info.pop("runtime_ai_settings", None)


def prepare_translation_settings_changes(
    current: RuntimeAISettings,
    changes: dict[str, str | float],
    *,
    clear_api_key: bool,
) -> dict[str, str | float]:
    prepared = dict(changes)
    provider = str(prepared.get("translation_provider", current.translation_provider)).strip().lower()
    profile = translation_provider_spec(provider)
    current_profile = translation_provider_spec(current.translation_provider)
    prepared.setdefault(current_profile.base_field, current.translation_base_url)
    prepared.setdefault(current_profile.model_field, current.translation_model)
    base_url = str(prepared.get("translation_base_url", getattr(current, profile.base_field))).strip()
    model = str(prepared.get("translation_model", getattr(current, profile.model_field))).strip()
    api_key = "" if clear_api_key else str(prepared.get("translation_api_key", current.translation_api_key)).strip()
    validate_translation_profile(provider, base_url, api_key)
    prepared.update(
        {
            "translation_provider": provider,
            "translation_base_url": base_url,
            "translation_model": model,
            profile.base_field: base_url,
            profile.model_field: model,
        }
    )
    if clear_api_key:
        prepared["translation_api_key"] = ""
    return prepared


def translation_chat_provider(ai_settings: RuntimeAISettings) -> LLMProvider:
    validate_translation_profile(ai_settings.translation_provider, ai_settings.translation_base_url, ai_settings.translation_api_key)
    return translation_provider_spec(ai_settings.translation_provider).create_provider(ai_settings)


def translation_settings_for_source(
    ai_settings: RuntimeAISettings, source: Source | None
) -> RuntimeAISettings:
    if ai_settings.translation_provider != CLOUD_TRANSLATION_PROVIDER or (
        source is not None
        and source.privacy_class == "public"
        and source.external_generation_allowed
    ):
        return ai_settings
    return replace(
        ai_settings,
        translation_provider=LOCAL_TRANSLATION_PROVIDER,
        translation_base_url=ai_settings.translation_local_base_url,
        translation_model=ai_settings.translation_local_model,
        translation_api_key="",
    )


def synthesis_remote_provider(
    ai_settings: RuntimeAISettings,
) -> OpenAICompatibleChatProvider:
    validate_synthesis_remote_settings(ai_settings)
    return OpenAICompatibleChatProvider(
        ai_settings.synthesis_remote_base_url,
        ai_settings.timeout_seconds,
        ai_settings.synthesis_remote_api_key,
        service_label="合成",
    )


def validate_synthesis_remote_settings(
    ai_settings: RuntimeAISettings, *, require_api_key: bool = True
) -> None:
    if not ai_settings.synthesis_remote_base_url.startswith("https://"):
        raise SynthesisSettingsError("云端合成地址必须使用 https://")
    if not valid_model_base_url(
        ai_settings.synthesis_remote_base_url, https_only=True
    ):
        raise SynthesisSettingsError(
            "模型地址必须是有效且不含用户名、密码、查询参数或片段的 URL"
        )
    if not ai_settings.synthesis_remote_model:
        raise SynthesisSettingsError("云端合成需要模型名称")
    if require_api_key and not is_header_safe_bearer_token(
        ai_settings.synthesis_remote_api_key
    ):
        raise SynthesisSettingsError("云端合成需要有效 API Key")


def translation_endpoint(ai_settings: RuntimeAISettings) -> str:
    return translation_provider_spec(ai_settings.translation_provider).endpoint(ai_settings.translation_base_url)


def translation_provider_spec(provider: str) -> TranslationProviderSpec:
    try:
        return TRANSLATION_PROVIDER_SPECS[provider]
    except KeyError:
        raise TranslationSettingsError("翻译提供方只支持 local 或 openai_compatible") from None


def validate_translation_profile(provider: str, base_url: str, api_key: str) -> None:
    profile = translation_provider_spec(provider)
    if api_key and not is_header_safe_bearer_token(api_key):
        raise TranslationSettingsError("云端 API Key 格式无效")
    if profile.requires_https and not base_url.startswith("https://"):
        raise TranslationSettingsError("云端翻译地址必须使用 https://")
    if not valid_model_base_url(base_url, https_only=profile.requires_https):
        raise TranslationSettingsError(
            "模型地址必须是有效且不含用户名、密码、查询参数或片段的 URL"
        )
    if profile.requires_key and not api_key:
        raise TranslationSettingsError("云端翻译需要 API Key")


def _local_translation_provider(ai_settings: RuntimeAISettings) -> LLMProvider:
    return LocalChatProvider(ai_settings.translation_base_url, ai_settings.timeout_seconds)


def _cloud_translation_provider(ai_settings: RuntimeAISettings) -> LLMProvider:
    return OpenAICompatibleChatProvider(
        ai_settings.translation_base_url,
        ai_settings.timeout_seconds,
        ai_settings.translation_api_key,
    )


TRANSLATION_PROVIDER_SPECS = {
    LOCAL_TRANSLATION_PROVIDER: TranslationProviderSpec(
        base_field="translation_local_base_url",
        model_field="translation_local_model",
        default_base_url=settings.translation_base_url or settings.llm_base_url,
        default_model=settings.translation_model,
        endpoint=lambda base_url: f"{base_url}/api/v1/chat",
        create_provider=_local_translation_provider,
    ),
    CLOUD_TRANSLATION_PROVIDER: TranslationProviderSpec(
        base_field="translation_cloud_base_url",
        model_field="translation_cloud_model",
        default_base_url="",
        default_model="",
        endpoint=openai_chat_endpoint_for,
        create_provider=_cloud_translation_provider,
        requires_https=True,
        requires_key=True,
    ),
}
