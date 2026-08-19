from __future__ import annotations

import asyncio
import json
import os
import re
import hashlib
import secrets
import subprocess
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit

import httpx
from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, Response, UploadFile
from fastapi.responses import JSONResponse
from redis import Redis
from rq import Queue
from rq.exceptions import NoSuchJobError
from rq.registry import StartedJobRegistry
from sqlalchemy import and_, case, delete, false, func, literal, or_, select, text
from sqlalchemy.orm import Load, Session, aliased, joinedload, undefer
from starlette.background import BackgroundTask

from .ai_runtime import (
    RuntimeAISettings,
    SynthesisSettingsError,
    TranslationSettingsError,
    prepare_translation_settings_changes,
    runtime_ai_settings,
    save_ai_settings,
    synthesis_remote_provider,
    translation_chat_provider,
    translation_endpoint,
    translation_settings_for_source,
    valid_model_base_url,
    validate_synthesis_remote_settings,
)
from .bulk_read import (
    confirm_bulk_read_batch,
    prepare_bulk_read_manifest,
    store_bulk_read_manifest,
)
from .article_image_cache import (
    MAX_ARTICLE_IMAGE_BYTES,
    ON_DEMAND_IMAGE_SECONDS,
    DownloadBudget,
    cache_key_for_url,
    configured_article_image_cache,
    download_image,
    valid_cache_key,
)
from .cluster import (
    EMBEDDING_AUTO_MAX_BATCHES,
    EMBEDDING_BATCH_LIMIT,
    cluster_source_items,
    decluster_source_items,
    embed_items_by_ids,
    embed_pending_items,
    embedding_pending_clause,
)
from .clustering_run import (
    clustering_run_execution_lock,
    defer_clustering_run_lock_until_transaction_end,
)
from .content_filters import (
    FilterRuleError,
    FilterRuleExecutionError,
    active_filter_labels_for_items,
    active_filter_match_exists,
    preview_filter_rule,
    refresh_filter_matches_for_items,
    rebuild_filter_rule_matches,
    unfiltered_content_clause,
    validate_filter_rule,
)
from .config import AI_MODEL_NAME_MAX_LENGTH, settings
from .db import SessionLocal, get_session, prepare_runtime_database
from .discover import discover_feed_candidates
from .digest import (
    clean_preview,
    clean_title,
    content_hash,
    first_markdown_image_url,
    normalize_space,
    strip_html,
)
from .event_projection import (
    ClusterEventIdentity,
    cluster_current_event_state_projection,
    cluster_event_identities_for,
    event_material_updates_for,
)
from .event_interactions import apply_event_user_state_mutation
from .event_synthesis import (
    EVIDENCE_REVIEW_PROMPT_VERSION,
    EVIDENCE_REVIEW_SCHEMA_VERSION,
    EVIDENCE_REVIEW_SYSTEM_PROMPT,
    EVIDENCE_REVIEW_TASK_TYPE,
    GenerationResultNotCurrentError,
    SYNTHESIS_PROMPT_VERSION,
    SYNTHESIS_SCHEMA_VERSION,
    SYNTHESIS_SYSTEM_PROMPT,
    SYNTHESIS_TASK_TYPE,
    SynthesisValidationError,
    advance_material_review_for_synthesis,
    advance_review_pointer,
    create_evidence_snapshot,
    evidence_review_input_data,
    evidence_review_model_request,
    event_synthesis_freshness_for,
    event_synthesis_state,
    latest_ordinary_review_for_synthesis,
    matching_ordinary_review,
    reject_material_review_without_synthesis,
    synthesis_generation_for_revision,
    synthesis_input_data,
    synthesis_model_request,
    synthesis_snapshot_evidence,
    synthesis_source_ids_for_revision,
    validated_event_generation_result,
)
from .feed_metrics import (
    feed_trust_score,
)
from .generation_lifecycle import (
    approve_generation_request,
    assert_generation_request_eligible,
    complete_generation_attempt,
    fail_generation_attempt,
    external_generation_policy,
    estimate_generation_payload,
    GenerationApprovalConsumedError,
    GenerationControlError,
    GenerationPrivacyError,
    GenerationRequestHasResultError,
    GenerationRequestNotRetryableError,
    generation_control_out,
    generation_request_sources,
    generation_task_out,
    generation_tasks_out,
    save_generation_control,
    generation_result_for_request,
    get_or_create_generation_request,
    latest_attempt,
    latest_generation_request,
    list_generation_tasks,
    lock_generation_lifecycle,
    schedule_automatic_generation_retry,
    stable_hash,
    start_admitted_generation_attempt,
    token_usage,
)
from .generation_producers import (
    CLUSTER_SYNTHESIS_TASK_TYPE,
    ITEM_SUMMARY_TASK_TYPE,
    GenerationProducerValidationError,
    PreparedGeneration,
    latest_applied_item_summary_generation,
    latest_item_summary_generation,
    prepare_cluster_synthesis,
    prepare_item_summary,
    validate_producer_result,
)
from .public_fetch import fetch_public_bytes
from .generation_results import (
    GenerationApplicationError,
    GenerationResultReplayConflict,
    UnsupportedGenerationError,
    reapply_generation_result,
    reapply_report_result,
)
from .maintenance import generation_retention_status
from .object_interactions import (
    OBJECT_STATE_TYPES,
    apply_object_user_state_mutation,
)
from .models import (
    Cluster,
    ClusterEventProjection,
    ClusterItem,
    ContentEmbedding,
    ContentItem,
    DELETED_SOURCE_STATUS,
    Document,
    EvidenceReview,
    EvidenceSnapshot,
    Event,
    EventEvidence,
    EventEvidenceVersion,
    EventLineage,
    EventRevision,
    EventRevisionEvidence,
    EventUserState,
    FeedMetric,
    FilterMatch,
    FilterRule,
    Folder,
    GenerationApplication,
    GenerationAttempt,
    GenerationRequest,
    GenerationRequestPayload,
    GenerationResult,
    InteractionEvent,
    LLMTask,
    RawEntry,
    Source,
    SourceEntryIdentity,
    SynthesisVersion,
    TopicGroup,
    UserState,
    now_utc,
)
from .llm import (
    LocalChatProvider,
    LocalEmbeddingProvider,
    embedding_endpoint_for,
    openai_chat_endpoint_for,
)
from .article_fetch import FiveFiltersRules
from .opml import export_opml, import_opml
from .media_types import SOURCE_MEDIA_TYPES, normalize_folder_name
from .queues import FETCH_QUEUE_NAME, LLM_QUEUE_NAME
from .public_rules import (
    ActivePublicRules,
    PublicRuleExtractionPreview,
    PublicRuleUpdateError,
    activate_public_rule_update,
    active_public_rules,
    check_public_rule_update,
)
from .report_generation import (
    REPORT_PROMPT_VERSION,
    REPORT_SCHEMA_VERSION,
    ReportValidationError,
    freeze_report_input,
    frozen_report_input,
    report_clusters,
    report_model_request,
    validate_report_output,
)
from .rss import prepare_entry_reading_body, source_is_fetch_eligible, youtube_watch_url
from .source_url import clean_source_url, source_by_url
from .source_lifecycle import tombstone_source
from .uninterested import (
    UninterestedFeedback,
    apply_uninterested_mutation,
    list_uninterested_targets,
    ordinary_content_clause,
    uninterested_feedback_for_items,
)
from .worker import begin_fetch_refresh, enqueue_source_fetch_job, fetch_refresh_status, wip_job_ids
from . import translations as translation_utils
from .schemas import (
    AIChatIn,
    AIChatOut,
    AISettingsPatch,
    AISettingsOut,
    AISummaryOut,
    ArticlePreviewEntryOut,
    ArticlePreviewIn,
    ArticlePreviewOptionsOut,
    ArticlePreviewOut,
    AssistantCitationOut,
    AssistantOut,
    BrowseMediaSummaryOut,
    BrowseSourceSummaryOut,
    BulkReadConfirmIn,
    BulkReadPrepareIn,
    BulkReadPrepared,
    ClusterDetailOut,
    ClusterOut,
    EventClusterProjectionOut,
    EventEvidenceSnapshotOut,
    EventEvidenceSourceOut,
    EventLineageOut,
    EventReadabilitySourceIn,
    EventReadabilitySourceOut,
    EventReadOut,
    EventRevisionDetailOut,
    EventRevisionSummaryOut,
    EventSuccessorOut,
    EventSourceViewEvidenceOut,
    EventSynthesisFreshnessOut,
    EventSynthesisStateOut,
    EventUserStateReadOut,
    EventUserStateMutationIn,
    EventUserStateMutationOut,
    FilteredItemsOut,
    FilterRuleCreate,
    FilterRuleOut,
    FilterRulePatch,
    FilterRulePreviewOut,
    FilterRuleSpec,
    FetchJobStatusOut,
    FolderCreate,
    FolderOut,
    FolderPatch,
    GenerationTaskOut,
    GenerationControlOut,
    GenerationControlPatch,
    GenerationRetentionOut,
    ItemOut,
    InteractionAuditOut,
    JobOut,
    PublicRulesActivateIn,
    PublicRulesCheckOut,
    PublicRuleExtractionPreviewOut,
    PublicRulesStatusOut,
    ReportOut,
    SourceBulkOut,
    SourceBulkPatch,
    SourceBulkSet,
    SourceCreate,
    SourceDiscoverIn,
    SourceDiscoverOut,
    SourceNavigationOut,
    SourceOut,
    SourcePatch,
    TranslationIn,
    TranslationOut,
    TopicGroupCreate,
    TopicGroupDetailOut,
    TopicGroupOut,
    TopicGroupPatch,
    SynthesisGenerateIn,
    UserStateOut,
    UserStatePatch,
    UninterestedMutationIn,
    UninterestedMutationOut,
    UninterestedTargetsOut,
)

READ_STATUSES = {"unread", "summary_seen", "original_opened", "dismissed"}
REPORT_PERIODS = {"day": "日报", "week": "周报", "month": "月报"}
REPORT_STATE_PREFIX = {"day": 1, "week": 2, "month": 3}
SOURCE_STATUSES = {"active", "trial", "muted", "archived"}
SOURCE_MEDIA_TYPE_ORDER = list(SOURCE_MEDIA_TYPES)
APP_TIMEZONE = timezone(timedelta(hours=8))
FAVICON_SUCCESS_TTL_SECONDS = 7 * 24 * 60 * 60
FAVICON_FAILURE_TTL_SECONDS = 10 * 60
FAVICON_MAX_BYTES = 512 * 1024
MAX_OPML_BYTES = 2 * 1024 * 1024
MAX_OPML_REQUEST_BYTES = MAX_OPML_BYTES + 64 * 1024
FAVICON_MEMORY_CACHE_MAX = 256
FAVICON_MEMORY_CACHE: OrderedDict[str, dict[str, object]] = OrderedDict()
DEFAULT_FAVICON = b'<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32"></svg>'


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    prepare_runtime_database()
    yield


app = FastAPI(title="Reader API", lifespan=lifespan)
app.state.api_auth_disabled_for_tests = False


@app.middleware("http")
async def require_api_token(request: Request, call_next):
    public_route = request.url.path == "/health"
    if not public_route and not request.app.state.api_auth_disabled_for_tests:
        expected = settings.api_token.strip()
        if not expected:
            return JSONResponse(
                status_code=503,
                content={"detail": "API 服务令牌尚未配置"},
            )
        provided = request.headers.get("X-Reader-API-Token", "")
        if not secrets.compare_digest(provided, expected):
            return JSONResponse(status_code=401, content={"detail": "API 服务令牌无效"})

    opml_too_large = False
    if request.url.path == "/imports/opml":
        declared_length = request.headers.get("Content-Length")
        try:
            if declared_length and int(declared_length) > MAX_OPML_REQUEST_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "OPML 文件超过 2MB 限制"},
                )
        except ValueError:
            pass
        received = 0
        receive = request._receive

        async def limited_receive():
            nonlocal opml_too_large, received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > MAX_OPML_REQUEST_BYTES:
                    opml_too_large = True
                    return {
                        "type": "http.request",
                        "body": b"",
                        "more_body": False,
                    }
            return message

        request._receive = limited_receive

    response = await call_next(request)
    if opml_too_large:
        return JSONResponse(
            status_code=413,
            content={"detail": "OPML 文件超过 2MB 限制"},
        )
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"ok": "true"}


@app.get("/about")
def about() -> dict[str, object]:
    health_checks = {
        "db": ("DB", check_db_health),
        "redis": ("Redis", check_redis_health),
    }
    health: dict[str, dict[str, object]] = {}
    try:
        with SessionLocal() as session:
            ai_settings = runtime_ai_settings(session)
    except Exception:
        health.update(
            {
                "llm": {"label": "LM Studio LLM", "ok": False, "detail": "AI 设置读取失败"},
                "embedding": {"label": "Embedding", "ok": False, "detail": "AI 设置读取失败"},
            }
        )
    else:
        health_checks.update(
            {
                "llm": ("LM Studio LLM", lambda: check_http_endpoint_health("LM Studio LLM", f"{ai_settings.llm_base_url}/api/v1/models")),
                "embedding": ("Embedding", lambda: check_http_endpoint_health("Embedding", embedding_endpoint_for(ai_settings.embedding_base_url))),
            }
        )
    with ThreadPoolExecutor(max_workers=len(health_checks)) as executor:
        futures = {key: (label, executor.submit(check)) for key, (label, check) in health_checks.items()}
        for key, (label, future) in futures.items():
            try:
                result = future.result(timeout=2.5)
            except Exception:
                result = {"ok": False, "detail": f"{label} 健康检查失败"}
            health[key] = {"label": label, "ok": bool(result.get("ok")), "detail": str(result.get("detail") or "")}
    return {
        "version": reader_version(),
        "commit": reader_commit(),
        "build_time": os.getenv("READER_BUILD_TIME", "").strip() or "development",
        "deploy_url": os.getenv("READER_DEPLOY_URL", "").strip(),
        "docs": [
            {"label": "产品说明", "href": "/docs/PRODUCT.md"},
            {"label": "架构", "href": "/docs/ARCHITECTURE.md"},
        ],
        "health": health,
        "article_image_cache": article_image_cache_usage(),
    }


def article_image_cache_usage() -> dict[str, int]:
    try:
        cache = configured_article_image_cache()
        return {
            "used_bytes": cache.usage(),
            "max_bytes": cache.max_bytes,
        }
    except OSError:
        return {"used_bytes": 0, "max_bytes": 0}


@app.get("/images/article/{cache_key}")
async def article_image(
    cache_key: str,
    src: str = "",
    x_reader_image_source: str | None = Header(default=None),
) -> Response:
    try:
        return await asyncio.to_thread(
            _article_image,
            cache_key,
            src,
            x_reader_image_source,
        )
    except TimeoutError as exc:
        raise HTTPException(status_code=503, detail="图片缓存繁忙") from exc


def _article_image(
    cache_key: str,
    src: str,
    x_reader_image_source: str | None,
) -> Response:
    if not valid_cache_key(cache_key):
        raise HTTPException(status_code=404, detail="图片缓存键无效")
    try:
        cache = configured_article_image_cache()
        cached = cache.read(cache_key)
    except OSError as exc:
        raise HTTPException(status_code=503, detail="图片缓存不可用") from exc
    if cached is not None:
        return article_image_response(cached.body, cached.content_type)

    eviction: BackgroundTask | None = None
    with cache.proxy_lock(cache_key):
        cached = cache.read(cache_key)
        if cached is not None:
            return article_image_response(cached.body, cached.content_type)
        try:
            registered_url = cache.url_for_key(cache_key)
        except OSError:
            registered_url = ""
        url = registered_url or (x_reader_image_source or src).strip()
        if not url or cache_key_for_url(url) != cache_key:
            raise HTTPException(status_code=404, detail="图片来源不可用")
        if cache.proxy_attempted(cache_key):
            raise HTTPException(status_code=502, detail="图片回源失败")
        try:
            cache.register(url)
        except OSError:
            pass
        image = download_image(
            url,
            deadline=time.monotonic() + ON_DEMAND_IMAGE_SECONDS,
            budget=DownloadBudget(MAX_ARTICLE_IMAGE_BYTES),
        )
        if image is None:
            try:
                cache.mark_proxy_attempted(url)
            except OSError:
                pass
            raise HTTPException(status_code=502, detail="图片回源失败")
        try:
            cache.store(url, image)
            eviction = BackgroundTask(cache.evict)
            if len(image.body) > cache.max_bytes:
                cache.mark_proxy_attempted(url)
        except OSError:
            try:
                cache.mark_proxy_attempted(url)
            except OSError:
                pass
        return article_image_response(
            image.body,
            image.content_type,
            background=eviction,
        )


def article_image_response(
    body: bytes,
    content_type: str,
    *,
    background: BackgroundTask | None = None,
) -> Response:
    return Response(
        content=body,
        media_type=content_type,
        background=background,
        headers={
            "Cache-Control": "public, max-age=86400, stale-while-revalidate=604800",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.get("/images/favicon")
def favicon_image(domain: str) -> Response:
    normalized = normalize_favicon_domain(domain)
    if not normalized:
        raise HTTPException(status_code=400, detail="domain 无效")
    cached = favicon_cache_get(normalized)
    if cached is not None:
        cached = safe_favicon_payload(cached)
        return favicon_response(cached, "hit-negative" if cached.get("negative") else "hit")
    fetched = fetch_favicon_bytes(normalized)
    payload = safe_favicon_payload(
        fetched
        or {
            "body": DEFAULT_FAVICON,
            "content_type": "image/svg+xml",
            "negative": True,
        }
    )
    if "negative" not in payload:
        payload["negative"] = False
    favicon_cache_set(normalized, payload)
    return favicon_response(payload, "miss-negative" if payload.get("negative") else "miss")


def favicon_response(payload: dict[str, object], cache_state: str) -> Response:
    payload = safe_favicon_payload(payload)
    cached_body = payload.get("body")
    body = cached_body if isinstance(cached_body, bytes) and cached_body else DEFAULT_FAVICON
    content_type = str(payload.get("content_type") or "image/svg+xml")
    negative = bool(payload.get("negative"))
    max_age = FAVICON_FAILURE_TTL_SECONDS if negative else FAVICON_SUCCESS_TTL_SECONDS
    return Response(
        content=b"" if negative else body,
        status_code=404 if negative else 200,
        media_type=content_type,
        headers={
            "Cache-Control": f"public, max-age={max_age}, stale-while-revalidate={max_age}",
            "Content-Security-Policy": "default-src 'none'; sandbox",
            "X-Content-Type-Options": "nosniff",
            "X-Reader-Favicon-Cache": cache_state,
            "X-Reader-Favicon-Negative": "1" if negative else "0",
        },
    )


def normalize_favicon_domain(value: str) -> str:
    raw = value.strip().lower()
    if not raw:
        return ""
    parsed = urlsplit(raw if "://" in raw else f"//{raw}")
    host = parsed.hostname or ""
    if not host or not re.fullmatch(r"[a-z0-9.-]+", host):
        return ""
    return host


def favicon_cache_get(domain: str) -> dict[str, object] | None:
    now = time.time()
    cached = FAVICON_MEMORY_CACHE.get(domain)
    if cached and not cached.get("negative") and not cached.get("body"):
        FAVICON_MEMORY_CACHE.pop(domain, None)
        cached = None
    if cached and float(cached.get("expires_at", 0)) > now:
        if hasattr(FAVICON_MEMORY_CACHE, "move_to_end"):
            FAVICON_MEMORY_CACHE.move_to_end(domain)
        return cached
    if cached:
        FAVICON_MEMORY_CACHE.pop(domain, None)
    disk = favicon_disk_get(domain, now)
    if disk is not None and not disk.get("negative") and not disk.get("body"):
        return None
    if disk is not None:
        favicon_memory_set(domain, disk)
    return disk


def favicon_cache_set(domain: str, payload: dict[str, object]) -> None:
    negative = bool(payload.get("negative"))
    expires_at = time.time() + (FAVICON_FAILURE_TTL_SECONDS if negative else FAVICON_SUCCESS_TTL_SECONDS)
    cached = {**payload, "expires_at": expires_at}
    favicon_memory_set(domain, cached)
    favicon_disk_set(domain, cached)


def favicon_memory_set(domain: str, payload: dict[str, object]) -> None:
    FAVICON_MEMORY_CACHE[domain] = payload
    if hasattr(FAVICON_MEMORY_CACHE, "move_to_end"):
        FAVICON_MEMORY_CACHE.move_to_end(domain)
    while len(FAVICON_MEMORY_CACHE) > FAVICON_MEMORY_CACHE_MAX:
        if isinstance(FAVICON_MEMORY_CACHE, OrderedDict):
            FAVICON_MEMORY_CACHE.popitem(last=False)
        else:
            FAVICON_MEMORY_CACHE.pop(next(iter(FAVICON_MEMORY_CACHE)), None)


def favicon_disk_get(domain: str, now: float) -> dict[str, object] | None:
    meta_path, body_path = favicon_cache_paths(domain)
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if float(meta.get("expires_at", 0)) <= now:
            return None
        body = body_path.read_bytes() if body_path.exists() else DEFAULT_FAVICON
        return {"body": body, "content_type": str(meta.get("content_type") or "image/svg+xml"), "negative": bool(meta.get("negative")), "expires_at": float(meta.get("expires_at", 0))}
    except Exception:
        return None


def favicon_disk_set(domain: str, payload: dict[str, object]) -> None:
    meta_path, body_path = favicon_cache_paths(domain)
    try:
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        body = payload.get("body") if isinstance(payload.get("body"), bytes) else DEFAULT_FAVICON
        body_path.write_bytes(body)
        meta_path.write_text(
            json.dumps({"content_type": str(payload.get("content_type") or "image/svg+xml"), "negative": bool(payload.get("negative")), "expires_at": float(payload.get("expires_at", 0))}),
            encoding="utf-8",
        )
    except Exception:
        pass


def favicon_cache_paths(domain: str) -> tuple[Path, Path]:
    digest = hashlib.sha256(domain.encode("utf-8")).hexdigest()
    cache_dir = Path(os.getenv("READER_FAVICON_CACHE_DIR", "/tmp/reader-favicon-cache"))
    return cache_dir / f"{digest}.json", cache_dir / f"{digest}.bin"


def fetch_favicon_bytes(domain: str) -> dict[str, object] | None:
    base = f"https://{domain}"
    direct = fetch_icon_candidate(f"{base}/favicon.ico")
    if direct is not None:
        return direct
    html = fetch_limited(base, allow_non_image=True)
    if html is not None:
        final_url = str(html.get("final_url") or base)
        for href in favicon_links(html["body"].decode("utf-8", "ignore")):
            icon = fetch_icon_candidate(urljoin(final_url, href))
            if icon is not None:
                return icon
    google = fetch_icon_candidate(
        f"https://www.google.com/s2/favicons?domain={domain}&sz=32"
    )
    if google is not None:
        return google
    return None


def fetch_icon_candidate(url: str) -> dict[str, object] | None:
    payload = fetch_limited(url)
    if payload is None:
        return None
    if not payload["body"]:
        return None
    content_type = payload["content_type"]
    if not str(content_type).startswith("image/") or favicon_is_svg(payload):
        return None
    return payload


def favicon_is_svg(payload: dict[str, object]) -> bool:
    content_type = str(payload.get("content_type") or "").lower()
    body = payload.get("body")
    prefix = body[:512].lstrip().lower() if isinstance(body, bytes) else b""
    return "svg" in content_type or prefix.startswith(b"<svg") or (
        prefix.startswith(b"<?xml") and b"<svg" in prefix
    )


def safe_favicon_payload(payload: dict[str, object]) -> dict[str, object]:
    if not favicon_is_svg(payload) or payload.get("negative"):
        return payload
    return {
        "body": DEFAULT_FAVICON,
        "content_type": "image/svg+xml",
        "negative": True,
    }


def fetch_limited(url: str, *, allow_non_image: bool = False) -> dict[str, object] | None:
    fetched = fetch_public_bytes(
        url,
        max_bytes=FAVICON_MAX_BYTES,
        timeout_seconds=5.0,
        headers={
            "User-Agent": "Reader favicon cache",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
        },
    )
    if not fetched.succeeded:
        return None
    content_type = fetched.content_type or "image/x-icon"
    if not allow_non_image and not content_type.startswith("image/"):
        return None
    return {
        "body": fetched.body,
        "content_type": content_type,
        "final_url": fetched.final_url,
    }


def favicon_links(html: str) -> list[str]:
    links: list[str] = []
    for match in re.finditer(r"<link\b[^>]*>", html, flags=re.IGNORECASE):
        tag = match.group(0)
        rel = html_attr(tag, "rel").lower()
        href = html_attr(tag, "href")
        if href and "icon" in rel:
            links.append(href)
    return links[:5]


def html_attr(tag: str, name: str) -> str:
    match = re.search(rf"\b{name}\s*=\s*(['\"])(.*?)\1", tag, flags=re.IGNORECASE)
    return match.group(2).strip() if match else ""


def check_db_health() -> dict[str, object]:
    with SessionLocal() as session:
        session.execute(text("SELECT 1"))
    return {"ok": True, "detail": "SELECT 1"}


def check_redis_health() -> dict[str, object]:
    connection = Redis.from_url(settings.redis_url, socket_connect_timeout=2, socket_timeout=2)
    connection.ping()
    return {"ok": True, "detail": "PING"}


def get_bulk_read_redis() -> Redis:
    return Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


def check_http_endpoint_health(label: str, endpoint: str) -> dict[str, object]:
    try:
        with httpx.Client(timeout=2.0) as client:
            response = client.get(endpoint)
        if 200 <= response.status_code < 300:
            return {"ok": True, "detail": f"HTTP {response.status_code}"}
        return {"ok": False, "detail": f"HTTP {response.status_code}"}
    except Exception:
        return {"ok": False, "detail": f"{label} 不可达"}


def reader_version() -> str:
    value = os.getenv("READER_VERSION", "").strip()
    if value:
        return value
    version_file = find_project_file("VERSIONS.md")
    if version_file is None:
        return "dev"
    text_value = version_file.read_text(encoding="utf-8", errors="ignore")
    current = re.search(r"当前版本：`([^`]+)`", text_value)
    if current:
        return current.group(1)
    heading = re.search(r"^##\s+(v[0-9][^\s]+)", text_value, flags=re.MULTILINE)
    return heading.group(1) if heading else "dev"


def reader_commit() -> str:
    value = os.getenv("READER_COMMIT", "").strip()
    if value:
        return value
    root = project_root()
    if root is None:
        return ""
    try:
        return subprocess.check_output(["git", "-C", str(root), "rev-parse", "--short", "HEAD"], text=True, timeout=1).strip()
    except Exception:
        return ""


def find_project_file(name: str) -> Path | None:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / name
        if candidate.exists():
            return candidate
    return None


def project_root() -> Path | None:
    versions = find_project_file("VERSIONS.md")
    if versions is not None:
        return versions.parent
    for parent in Path(__file__).resolve().parents:
        if (parent / ".git").exists():
            return parent
    return None


def local_llm(ai_settings: RuntimeAISettings) -> LocalChatProvider:
    return LocalChatProvider(ai_settings.llm_base_url, ai_settings.timeout_seconds)


def requested_generation_provider(
    payload: SynthesisGenerateIn | None, ai_settings: RuntimeAISettings
) -> str:
    provider = (
        payload.provider
        if payload is not None and payload.provider is not None
        else ai_settings.task_provider
    )
    if provider not in {"local", "openai_compatible"}:
        raise HTTPException(status_code=400, detail="生成服务选择无效")
    return provider


def producer_generation_request(
    session: Session,
    *,
    prepared: PreparedGeneration,
    provider: str,
    ai_settings: RuntimeAISettings,
) -> GenerationRequest:
    model = (
        ai_settings.synthesis_remote_model
        if provider == "openai_compatible"
        else ai_settings.llm_model
    )
    source_policies: list[Source] = []
    source_policy_fingerprint: str | None = None
    privacy_status = "local"
    if provider == "openai_compatible":
        source_policies, source_policy_fingerprint, privacy_reason = (
            external_generation_policy(
                session, prepared.source_ids, lock_sources=True
            )
        )
        if privacy_reason:
            get_or_create_generation_request(
                session,
                task_type=prepared.task_type,
                reason="explicit-user-request",
                target_type=prepared.target_type,
                target_id=prepared.target_id,
                target_uid=prepared.target_uid,
                provider=provider,
                model=model,
                prompt_version=prepared.prompt_version,
                schema_version=prepared.schema_version,
                input_fingerprint=prepared.input_fingerprint,
                payload=None,
                privacy_status="blocked",
                privacy_reason=privacy_reason,
                source_policy_fingerprint=source_policy_fingerprint,
                source_policies=source_policies,
            )
            session.commit()
            raise HTTPException(status_code=403, detail=privacy_reason)
        privacy_status = "eligible"
    request, _created = get_or_create_generation_request(
        session,
        task_type=prepared.task_type,
        reason="explicit-user-request",
        target_type=prepared.target_type,
        target_id=prepared.target_id,
        target_uid=prepared.target_uid,
        provider=provider,
        model=model,
        prompt_version=prepared.prompt_version,
        schema_version=prepared.schema_version,
        input_fingerprint=prepared.input_fingerprint,
        payload=prepared.payload,
        privacy_status=privacy_status,
        source_policy_fingerprint=source_policy_fingerprint,
        source_policies=source_policies,
    )
    return request


def strict_producer_output(
    request: GenerationRequest, provider_result: dict[str, object]
) -> dict[str, object]:
    fields = (
        {"summary"}
        if request.task_type == ITEM_SUMMARY_TASK_TYPE
        else {"title", "summary", "content"}
    )
    transport_payload = {
        key: value for key, value in provider_result.items() if key != "usage"
    }
    if fields & transport_payload.keys():
        return transport_payload
    try:
        parsed = json.loads(unfence_json(llm_text(provider_result)))
    except json.JSONDecodeError:
        raise GenerationProducerValidationError("模型输出不是合法 JSON") from None
    if not isinstance(parsed, dict):
        raise GenerationProducerValidationError("模型输出结构无效")
    return parsed


def execute_synchronous_producer(
    session: Session,
    *,
    request: GenerationRequest,
    prepared: PreparedGeneration,
    provider: str,
    ai_settings: RuntimeAISettings,
) -> GenerationRequest:
    existing = generation_result_for_request(session, request.id)
    if existing is not None:
        if existing[1].status != "applied":
            return reapply_generation_result(session, request=request)
        session.commit()
        return request
    attempt = latest_attempt(session, request.id)
    if attempt is not None and attempt.status in {"pending", "running"}:
        session.commit()
        return request
    approve_generation_request(session, request, allow_consumed_reissue=True)
    attempt = start_admitted_generation_attempt(session, request)
    if attempt is None:
        session.commit()
        return request
    attempt_id = attempt.id
    session.commit()
    for transport_attempt in range(2):
        input_tokens: int | None = None
        output_tokens: int | None = None
        try:
            if provider == "openai_compatible":
                assert_generation_request_eligible(
                    session, request, lock_sources=True
                )
                llm_provider = synthesis_remote_provider(ai_settings)
            else:
                llm_provider = local_llm(ai_settings)
            provider_result = llm_provider.chat(
                request.model,
                str(prepared.payload["system_prompt"]),
                str(prepared.payload["input"]),
            )
            input_tokens, output_tokens = token_usage(provider_result)
            normalized = strict_producer_output(request, provider_result)
            normalized = validate_producer_result(
                request, normalized, prepared.payload
            )
            break
        except GenerationProducerValidationError as exc:
            attempt = session.scalar(
                select(GenerationAttempt)
                .where(GenerationAttempt.id == attempt_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            assert attempt is not None
            fail_generation_attempt(
                session,
                attempt,
                str(exc),
                failure_class="validation",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
            session.commit()
            raise HTTPException(
                status_code=502,
                detail="模型返回的生成内容无法使用，请重试",
            ) from None
        except RuntimeError:
            error = (
                "云端合成服务不可用，请检查地址、模型和密钥"
                if provider == "openai_compatible"
                else "本地模型服务未连接，请检查 LM Studio"
            )
            attempt = session.scalar(
                select(GenerationAttempt)
                .where(GenerationAttempt.id == attempt_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            assert attempt is not None
            fail_generation_attempt(
                session,
                attempt,
                error,
                failure_class="transport",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
            if (
                transport_attempt == 0
                and schedule_automatic_generation_retry(session, request)
            ):
                approve_generation_request(
                    session, request, allow_consumed_reissue=True
                )
                retry_attempt = start_admitted_generation_attempt(session, request)
                if retry_attempt is not None:
                    attempt_id = retry_attempt.id
                    session.commit()
                    continue
            session.commit()
            raise HTTPException(status_code=502, detail=error) from None
    attempt = session.scalar(
        select(GenerationAttempt)
        .where(GenerationAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    assert attempt is not None
    if provider == "openai_compatible":
        try:
            assert_generation_request_eligible(
                session, request, lock_sources=True
            )
        except GenerationPrivacyError as exc:
            fail_generation_attempt(
                session,
                attempt,
                str(exc),
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
            session.commit()
            raise HTTPException(status_code=409, detail=str(exc)) from None
    complete_generation_attempt(
        session,
        attempt=attempt,
        payload=normalized,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    request_id = request.id
    session.commit()
    request = session.get(GenerationRequest, request_id)
    assert request is not None
    return reapply_generation_result(session, request=request, pending_only=True)


@app.get("/folders", response_model=list[FolderOut])
def list_folders(session: Session = Depends(get_session)) -> list[Folder]:
    return sorted(session.scalars(select(Folder)).all(), key=lambda folder: natural_name_key(folder.name))


@app.post("/folders", response_model=FolderOut)
def create_folder(payload: FolderCreate, session: Session = Depends(get_session)) -> Folder:
    name = normalize_folder_name(payload.name, payload.media_type)
    if not name:
        raise HTTPException(status_code=400, detail="文件夹名称不能为空")
    existing = session.scalar(
        select(Folder).where(
            Folder.name == name,
            Folder.media_type == payload.media_type,
        )
    )
    if existing:
        return existing
    folder = Folder(name=name, media_type=payload.media_type)
    session.add(folder)
    session.commit()
    session.refresh(folder)
    return folder


@app.patch("/folders/{folder_id}", response_model=FolderOut)
def update_folder(folder_id: int, payload: FolderPatch, session: Session = Depends(get_session)) -> Folder:
    folder = session.get(Folder, folder_id)
    if folder is None:
        raise HTTPException(status_code=404, detail="文件夹不存在")
    if payload.name is not None:
        name = normalize_folder_name(payload.name, folder.media_type)
        if not name:
            raise HTTPException(status_code=400, detail="文件夹名称不能为空")
        existing = session.scalar(
            select(Folder).where(
                Folder.name == name,
                Folder.media_type == folder.media_type,
                Folder.id != folder_id,
            )
        )
        if existing is not None:
            raise HTTPException(status_code=400, detail="文件夹名称已存在")
        folder.name = name
    session.commit()
    session.refresh(folder)
    return folder


@app.delete("/folders/{folder_id}", status_code=204)
def delete_folder(folder_id: int, session: Session = Depends(get_session)) -> Response:
    folder = session.get(Folder, folder_id)
    if folder is None:
        raise HTTPException(status_code=404, detail="文件夹不存在")
    if session.scalar(select(Source.id).where(Source.folder_id == folder_id).limit(1)) is not None:
        raise HTTPException(status_code=409, detail="请先移动文件夹内的订阅源")
    session.delete(folder)
    session.commit()
    return Response(status_code=204)


def cluster_effective_state_expressions(session: Session):
    current_event_state = cluster_current_event_state_projection(session)
    effective_read_status = case(
        (
            current_event_state.c.material_update_revision_uid.is_not(None),
            "unread",
        ),
        else_=func.coalesce(current_event_state.c.read_status, "unread"),
    )
    effective_read_later = func.coalesce(current_event_state.c.read_later, false())
    effective_starred = func.coalesce(current_event_state.c.starred, false())
    return (
        current_event_state,
        effective_read_status,
        effective_read_later,
        effective_starred,
    )


@app.get("/sources", response_model=list[SourceOut])
def list_sources(
    include_metrics: bool = True,
    session: Session = Depends(get_session),
) -> list[SourceOut]:
    (
        current_event_state,
        effective_read_status,
        _effective_read_later,
        _effective_starred,
    ) = cluster_effective_state_expressions(session)
    unread_cluster_clause = effective_read_status == "unread"
    unread_membership = (
        select(
            ClusterItem.cluster_id.label("cluster_id"),
            ContentItem.source_id.label("source_id"),
            Source.folder_id.label("folder_id"),
            Source.media_type.label("media_type"),
        )
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(current_event_state, current_event_state.c.cluster_id == ClusterItem.cluster_id, isouter=True)
        .where(
            Source.status == "active",
            unread_cluster_clause,
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .distinct()
        .cte("unread_membership")
    )
    article_unread_source_counts = (
        select(
            ContentItem.source_id.label("source_id"),
            func.count(func.distinct(ClusterItem.cluster_id)).label("unread_count"),
        )
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(
            current_event_state,
            current_event_state.c.cluster_id == ClusterItem.cluster_id,
            isouter=True,
        )
        .where(
            Source.status == "active",
            Source.media_type == "article",
            unread_cluster_clause,
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            ordinary_content_clause(session, ContentItem.id),
        )
        .group_by(ContentItem.source_id)
    )
    media_unread_source_counts = (
        select(
            ContentItem.source_id.label("source_id"),
            func.count(ContentItem.id).label("unread_count"),
        )
        .join(Source, Source.id == ContentItem.source_id)
        .join(
            UserState,
            (UserState.object_type == "item")
            & (UserState.object_id == ContentItem.id),
            isouter=True,
        )
        .where(
            Source.status == "active",
            Source.media_type != "article",
            or_(UserState.id.is_(None), UserState.read_status == "unread"),
            ordinary_content_clause(session, ContentItem.id),
        )
        .group_by(ContentItem.source_id)
    )
    unread_source_counts = (
        article_unread_source_counts.union_all(media_unread_source_counts)
        .subquery()
    )
    folder_key = func.coalesce(unread_membership.c.folder_id, literal(-1)).label("folder_key")
    unread_folder_counts = (
        select(folder_key, func.count(func.distinct(unread_membership.c.cluster_id)).label("folder_unread_count"))
        .where(unread_membership.c.media_type == "article")
        .group_by(folder_key)
        .subquery()
    )
    all_unread_count = func.coalesce(
        select(func.count(func.distinct(unread_membership.c.cluster_id)))
        .where(unread_membership.c.media_type == "article")
        .scalar_subquery(),
        0,
    )
    if include_metrics:
        cluster_counts = (
            select(ContentItem.source_id.label("source_id"), func.count(func.distinct(ClusterItem.cluster_id)).label("cluster_count"))
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .group_by(ContentItem.source_id)
            .subquery()
        )
        cluster_sizes = select(ClusterItem.cluster_id.label("cluster_id"), func.count(ClusterItem.id).label("item_count")).group_by(ClusterItem.cluster_id).subquery()
        duplicate_counts = (
            select(ContentItem.source_id.label("source_id"), func.count(ClusterItem.id).label("duplicate_count"))
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .join(cluster_sizes, cluster_sizes.c.cluster_id == ClusterItem.cluster_id)
            .where(cluster_sizes.c.item_count > 1)
            .group_by(ContentItem.source_id)
            .subquery()
        )
        recent_entry_counts = (
            select(RawEntry.source_id.label("source_id"), func.count(RawEntry.id).label("recent_entry_count_30d"))
            .where(RawEntry.fetched_at >= now_utc() - timedelta(days=30))
            .group_by(RawEntry.source_id)
            .subquery()
        )
        cluster_count = func.coalesce(cluster_counts.c.cluster_count, 0)
        duplicate_count = func.coalesce(duplicate_counts.c.duplicate_count, 0)
        recent_entry_count = func.coalesce(recent_entry_counts.c.recent_entry_count_30d, 0)
    else:
        cluster_count = literal(0)
        duplicate_count = literal(0)
        recent_entry_count = literal(0)
    stmt = (
        select(
            Source,
            func.coalesce(unread_source_counts.c.unread_count, 0),
            FeedMetric,
            cluster_count,
            duplicate_count,
            func.coalesce(unread_folder_counts.c.folder_unread_count, 0),
            all_unread_count,
            recent_entry_count,
        )
        .join(FeedMetric, FeedMetric.source_id == Source.id, isouter=True)
        .join(unread_source_counts, unread_source_counts.c.source_id == Source.id, isouter=True)
        .join(unread_folder_counts, func.coalesce(Source.folder_id, literal(-1)) == unread_folder_counts.c.folder_key, isouter=True)
        .where(Source.status != DELETED_SOURCE_STATUS)
    )
    if include_metrics:
        stmt = (
            stmt.join(cluster_counts, cluster_counts.c.source_id == Source.id, isouter=True)
            .join(duplicate_counts, duplicate_counts.c.source_id == Source.id, isouter=True)
            .join(recent_entry_counts, recent_entry_counts.c.source_id == Source.id, isouter=True)
        )
    rows = [
        source_out(source, count, metric, cluster_count, duplicate_count, folder_count, total_count, recent_entry_count, include_metrics=include_metrics)
        for source, count, metric, cluster_count, duplicate_count, folder_count, total_count, recent_entry_count in session.execute(stmt).all()
    ]
    return sorted(rows, key=lambda source: natural_name_key(source.name))


@app.get("/sources/navigation", response_model=list[SourceNavigationOut])
def source_navigation(
    session: Session = Depends(get_session),
) -> list[SourceNavigationOut]:
    return [
        SourceNavigationOut(**source.model_dump())
        for source in list_sources(include_metrics=False, session=session)
    ]


@app.get(
    "/sources/{source_id}/article-preview",
    response_model=ArticlePreviewOptionsOut,
)
def article_preview_options(
    source_id: int,
    session: Session = Depends(get_session),
) -> ArticlePreviewOptionsOut:
    source = session.get(Source, source_id)
    if source is None or source.status == DELETED_SOURCE_STATUS:
        raise HTTPException(status_code=404, detail="订阅源不存在")
    entries = latest_article_preview_entries(session, source_id)
    active_rules = active_public_rules(session)
    return ArticlePreviewOptionsOut(
        entries=[
            ArticlePreviewEntryOut(
                raw_entry_id=entry.id,
                title=entry.title,
                published_at=entry.published_at,
            )
            for entry in entries
        ],
        public_rules=public_rules_status_out(active_rules),
    )


@app.post("/article-rules/check", response_model=PublicRulesCheckOut)
def check_article_rules(
    session: Session = Depends(get_session),
) -> PublicRulesCheckOut:
    try:
        check = check_public_rule_update(
            session,
            preview_public_rule_candidate,
        )
    except PublicRuleUpdateError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return PublicRulesCheckOut(
        current_version=check.current.rules.version,
        current_commit=check.current.commit,
        candidate_version=check.candidate.rules.version,
        candidate_commit=check.candidate.commit,
        rules_count=check.rules_count,
        skipped_count=check.skipped_count,
        subscribed_domains=check.subscribed_domains,
        covered_subscribed_domains=check.covered_subscribed_domains,
        changed_subscribed_domains=check.changed_subscribed_domains,
        tested_subscribed_domains=check.tested_subscribed_domains,
        invalid_subscribed_domains=list(check.invalid_subscribed_domains),
        failed_subscribed_domains=list(check.failed_subscribed_domains),
        preview=(
            PublicRuleExtractionPreviewOut(
                hostname=check.preview.hostname,
                title=check.preview.title,
                reading_html=check.preview.reading_html,
                rss_characters=check.preview.rss_characters,
                webpage_characters=check.preview.webpage_characters,
                method=check.preview.method,
                version=check.preview.version,
                adopted_webpage=check.preview.adopted_webpage,
                matched_elements=check.preview.matched_elements,
                removed_elements=check.preview.removed_elements,
                diagnostics=list(check.preview.diagnostics),
                fallback_reason=article_preview_fallback_reason(
                    check.preview.diagnostics
                ),
                passed=check.preview.passed,
            )
            if check.preview is not None
            else None
        ),
        passed=check.passed,
        can_activate=check.can_activate,
    )


@app.post("/article-rules/activate", response_model=PublicRulesStatusOut)
def activate_article_rules(
    payload: PublicRulesActivateIn,
    session: Session = Depends(get_session),
) -> PublicRulesStatusOut:
    try:
        active = activate_public_rule_update(
            session,
            payload.commit,
            preview_public_rule_candidate,
        )
    except PublicRuleUpdateError as exc:
        session.rollback()
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return public_rules_status_out(active)


def public_rules_status_out(active: ActivePublicRules) -> PublicRulesStatusOut:
    return PublicRulesStatusOut(
        version=active.rules.version,
        commit=active.commit,
        activated_at=active.activated_at,
        bundled=active.bundled,
    )


@app.post(
    "/sources/{source_id}/article-preview",
    response_model=ArticlePreviewOut,
)
def preview_article(
    source_id: int,
    payload: ArticlePreviewIn,
    session: Session = Depends(get_session),
) -> ArticlePreviewOut:
    source = session.get(Source, source_id)
    if source is None or source.status == DELETED_SOURCE_STATUS:
        raise HTTPException(status_code=404, detail="订阅源不存在")
    entries = latest_article_preview_entries(session, source_id)
    entry = next(
        (candidate for candidate in entries if candidate.id == payload.raw_entry_id),
        None,
    )
    if entry is None:
        raise HTTPException(status_code=404, detail="预览文章不在最近 6 篇内")
    rss_html = entry.raw_content or entry.raw_summary
    prepared = prepare_entry_reading_body(
        rss_html,
        strip_html(rss_html, entry.url),
        entry.url,
        fetch_web=payload.fetch_full_content,
        article_selector=payload.article_selector,
        remove_selector=payload.remove_selector,
        rules=active_public_rules(session).rules,
    )
    return ArticlePreviewOut(
        raw_entry_id=entry.id,
        title=entry.title,
        reading_html=prepared.reading_html,
        rss_characters=prepared.rss_characters,
        webpage_characters=prepared.webpage_characters,
        method=prepared.method,
        version=prepared.version,
        body_source=prepared.body_source,
        web_fetch_status=prepared.web_fetch_status,
        adopted_webpage=prepared.body_source == "webpage",
        matched_elements=prepared.matched_elements,
        removed_elements=prepared.removed_elements,
        diagnostics=list(prepared.diagnostics),
        fallback_reason=article_preview_fallback_reason(prepared.diagnostics),
    )


def preview_public_rule_candidate(
    entry: RawEntry,
    hostname: str,
    rules: FiveFiltersRules,
) -> PublicRuleExtractionPreview:
    rss_html = entry.raw_content or entry.raw_summary
    prepared = prepare_entry_reading_body(
        rss_html,
        strip_html(rss_html, entry.url),
        entry.url,
        fetch_web=True,
        article_selector=None,
        remove_selector=entry.source.remove_selector,
        rules=rules,
    )
    return PublicRuleExtractionPreview(
        hostname=hostname,
        title=entry.title,
        reading_html=prepared.reading_html,
        rss_characters=prepared.rss_characters,
        webpage_characters=prepared.webpage_characters,
        method=prepared.method,
        version=prepared.version,
        adopted_webpage=prepared.body_source == "webpage",
        matched_elements=prepared.matched_elements,
        removed_elements=prepared.removed_elements,
        diagnostics=prepared.diagnostics,
        passed=(
            prepared.body_source == "webpage"
            and (
                rules.match(hostname) is None
                or prepared.method == "fivefilters"
            )
        ),
    )


def article_preview_fallback_reason(diagnostics: tuple[str, ...]) -> str:
    if not diagnostics:
        return ""
    reasons = {
        "manual_no_match": "手工正文规则未匹配",
        "manual_quality_rejected": "手工正文未通过质量检查",
        "fivefilters_hard_dependency": "公共规则需要不支持的登录、请求头或分页能力",
        "fivefilters_no_match": "公共规则未匹配",
        "fivefilters_quality_rejected": "公共规则正文未通过质量检查",
        "trafilatura_no_match": "通用提取未找到正文",
        "quality_rejected": "网页正文未通过质量检查",
        "ssrf_blocked": "文章地址不是公共网络地址",
        "time_budget_exhausted": "网页抓取超时",
    }
    return reasons.get(diagnostics[-1], "网页正文不可用，已回退 RSS")


def latest_article_preview_entries(
    session: Session,
    source_id: int,
) -> list[RawEntry]:
    return list(
        session.scalars(
            select(RawEntry)
            .join(
                SourceEntryIdentity,
                SourceEntryIdentity.id == RawEntry.source_entry_id,
            )
            .where(
                RawEntry.source_id == source_id,
                RawEntry.revision_no
                == SourceEntryIdentity.current_revision_no,
            )
            .order_by(
                RawEntry.published_at.is_(None),
                RawEntry.published_at.desc(),
                RawEntry.fetched_at.desc(),
                RawEntry.id.desc(),
            )
            .limit(6)
        ).all()
    )


@app.get("/filter-rules", response_model=list[FilterRuleOut])
def list_filter_rules(session: Session = Depends(get_session)) -> list[FilterRuleOut]:
    match_counts = (
        select(
            FilterMatch.rule_id.label("rule_id"),
            func.count(FilterMatch.content_item_id).label("match_count"),
        )
        .join(ContentItem, ContentItem.id == FilterMatch.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(Source.status != DELETED_SOURCE_STATUS)
        .group_by(FilterMatch.rule_id)
        .subquery()
    )
    rows = session.execute(
        select(
            FilterRule,
            Source.name,
            func.coalesce(match_counts.c.match_count, 0),
        )
        .join(Source, Source.id == FilterRule.source_id, isouter=True)
        .join(match_counts, match_counts.c.rule_id == FilterRule.id, isouter=True)
        .order_by(FilterRule.created_at.desc(), FilterRule.id.desc())
    ).all()
    return [
        filter_rule_out(rule, source_name or "", int(match_count or 0))
        for rule, source_name, match_count in rows
    ]


@app.post("/filter-rules/preview", response_model=FilterRulePreviewOut)
def preview_content_filter_rule(
    payload: FilterRuleSpec, session: Session = Depends(get_session)
) -> FilterRulePreviewOut:
    ensure_filter_source(session, payload.source_id)
    try:
        preview = preview_filter_rule(
            session,
            source_id=payload.source_id,
            match_type=payload.match_type,
            pattern=payload.pattern,
        )
    except (FilterRuleError, FilterRuleExecutionError) as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from None
    return FilterRulePreviewOut(
        count=preview.count,
        items=filter_item_outputs(session, list(preview.items)),
    )


@app.post("/filter-rules", response_model=FilterRuleOut)
def create_filter_rule(
    payload: FilterRuleCreate, session: Session = Depends(get_session)
) -> FilterRuleOut:
    ensure_filter_source(session, payload.source_id)
    try:
        pattern = validate_filter_rule(payload.match_type, payload.pattern)
        with clustering_run_execution_lock(session):
            defer_clustering_run_lock_until_transaction_end(session)
            rule = FilterRule(
                source_id=payload.source_id,
                match_type=payload.match_type,
                pattern=pattern,
                enabled=True,
            )
            session.add(rule)
            session.flush()
            match_count = rebuild_filter_rule_matches(session, rule)
            session.commit()
    except (FilterRuleError, FilterRuleExecutionError) as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from None
    session.refresh(rule)
    source_name = (
        session.scalar(select(Source.name).where(Source.id == rule.source_id)) or ""
        if rule.source_id is not None
        else ""
    )
    return filter_rule_out(rule, source_name, match_count)


@app.patch("/filter-rules/{rule_id}", response_model=FilterRuleOut)
def update_filter_rule(
    rule_id: int,
    payload: FilterRulePatch,
    session: Session = Depends(get_session),
) -> FilterRuleOut:
    if not payload.model_fields_set:
        raise HTTPException(status_code=400, detail="过滤规则更新内容不能为空")
    for required_field in ("match_type", "pattern", "enabled"):
        if required_field in payload.model_fields_set and getattr(payload, required_field) is None:
            raise HTTPException(status_code=400, detail="过滤规则字段不能为空")
    try:
        with clustering_run_execution_lock(session):
            defer_clustering_run_lock_until_transaction_end(session)
            rule = session.get(
                FilterRule,
                rule_id,
                populate_existing=True,
                with_for_update=True,
            )
            if rule is None:
                raise HTTPException(status_code=404, detail="过滤规则不存在")
            source_id = (
                payload.source_id
                if "source_id" in payload.model_fields_set
                else rule.source_id
            )
            ensure_filter_source(session, source_id)
            match_type = payload.match_type or rule.match_type
            pattern = validate_filter_rule(match_type, payload.pattern or rule.pattern)
            config_changed = (
                source_id != rule.source_id
                or match_type != rule.match_type
                or pattern != rule.pattern
            )
            enabling = payload.enabled is True and not rule.enabled
            rule.source_id = source_id
            rule.match_type = match_type
            rule.pattern = pattern
            if payload.enabled is not None:
                rule.enabled = payload.enabled
            rule.updated_at = now_utc()
            session.flush()
            if config_changed or enabling:
                rebuild_filter_rule_matches(session, rule)
            match_count = int(
                session.scalar(
                    select(func.count()).select_from(FilterMatch).where(
                        FilterMatch.rule_id == rule.id
                    )
                )
                or 0
            )
            session.commit()
    except (FilterRuleError, FilterRuleExecutionError) as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from None
    session.refresh(rule)
    source_name = (
        session.scalar(select(Source.name).where(Source.id == rule.source_id)) or ""
        if rule.source_id is not None
        else ""
    )
    return filter_rule_out(rule, source_name, match_count)


@app.delete("/filter-rules/{rule_id}", status_code=204)
def delete_filter_rule(
    rule_id: int, session: Session = Depends(get_session)
) -> Response:
    with clustering_run_execution_lock(session):
        defer_clustering_run_lock_until_transaction_end(session)
        rule = session.get(FilterRule, rule_id, with_for_update=True)
        if rule is None:
            raise HTTPException(status_code=404, detail="过滤规则不存在")
        session.delete(rule)
        session.commit()
    return Response(status_code=204)


def ensure_filter_source(session: Session, source_id: int | None) -> None:
    if source_id is not None:
        source = session.get(Source, source_id)
        if source is None or source.status == DELETED_SOURCE_STATUS:
            raise HTTPException(status_code=400, detail="过滤规则来源不存在")


def filter_rule_out(
    rule: FilterRule, source_name: str, match_count: int
) -> FilterRuleOut:
    return FilterRuleOut(
        id=rule.id,
        source_id=rule.source_id,
        source_name=source_name,
        match_type=rule.match_type,
        pattern=rule.pattern,
        enabled=rule.enabled,
        match_count=match_count,
        created_at=rule.created_at,
        updated_at=rule.updated_at,
    )


@app.get("/pipeline/status")
def pipeline_status(session: Session = Depends(get_session)) -> dict[str, datetime | None]:
    embedded_item_at = session.scalar(
        select(func.max(ContentItem.created_at))
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            Source.status == "active",
            Source.enabled.is_(True),
            Source.media_type == "article",
            ContentItem.embedding_model != "",
        )
    )
    embedding_cache_at = session.scalar(
        select(func.max(ContentEmbedding.created_at))
        .join(ContentItem, ContentItem.id == ContentEmbedding.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            Source.status == "active",
            Source.enabled.is_(True),
            Source.media_type == "article",
        )
    )
    fetched_at = session.scalar(
        select(func.max(Source.last_fetched_at)).where(
            Source.status == "active",
            Source.enabled.is_(True),
            Source.media_type == "article",
        )
    )
    completed_at = max((value for value in (embedded_item_at, embedding_cache_at) if value is not None), default=None)
    return {"completed_at": completed_at or fetched_at}


@app.get("/pipeline/overview")
def pipeline_overview(session: Session = Depends(get_session)) -> dict[str, object]:
    ai_settings = runtime_ai_settings(session)
    fetch_queue = queue_overview(FETCH_QUEUE_NAME)
    llm_queue = queue_overview(LLM_QUEUE_NAME)
    source_count = int(
        session.scalar(
            select(func.count(Source.id)).where(
                Source.status == "active",
                Source.enabled.is_(True),
                Source.media_type == "article",
            )
        )
        or 0
    )
    failed_source_count = int(
        session.scalar(
            select(func.count(Source.id)).where(
                Source.status == "active",
                Source.enabled.is_(True),
                Source.media_type == "article",
                Source.last_error != "",
            )
        )
        or 0
    )
    last_completed_at = session.scalar(
        select(func.max(Source.last_fetched_at)).where(
            Source.status == "active",
            Source.enabled.is_(True),
            Source.media_type == "article",
        )
    )
    pending_embeddings = pending_embedding_count(session, ai_settings.embedding_model)
    translation_cutoff = now_utc() - timedelta(hours=24)
    cached_translations = int(
        session.scalar(
            select(func.count(LLMTask.id)).where(
                LLMTask.task_type == translation_utils.TRANSLATION_TASK_TYPE,
                LLMTask.status == "complete",
                LLMTask.created_at >= translation_cutoff,
            )
        )
        or 0
    )
    generation_counts, latest_failed_error = generation_task_overview(session)
    return {
        "rss": {
            "last_completed_at": last_completed_at,
            "interval_seconds": settings.rss_fetch_interval_seconds,
            "source_count": source_count,
            "failed_source_count": failed_source_count,
            "queue": fetch_queue,
        },
        "embedding": {
            "pending_items": pending_embeddings,
            "queue": llm_queue,
        },
        "translation": {
            "cached_24h": cached_translations,
        },
        "generation": {
            "pending": generation_counts.get("pending", 0),
            "running": generation_counts.get("running", 0),
            "failed": generation_counts.get("failed", 0),
            "complete": generation_counts.get("complete", 0),
            "latest_failed_error": latest_failed_error,
        },
    }


@app.get("/generation/tasks", response_model=list[GenerationTaskOut])
def generation_tasks(
    limit: int = 20,
    offset: int = 0,
    before_request_uid: str | None = None,
    session: Session = Depends(get_session),
) -> list[GenerationTaskOut]:
    before = None
    if before_request_uid:
        before = session.scalar(
            select(GenerationRequest).where(
                GenerationRequest.uid == before_request_uid
            )
        )
        if before is None:
            raise HTTPException(status_code=400, detail="生成任务分页游标无效")
    return list_generation_tasks(
        session,
        min(max(limit, 1), 100),
        max(offset, 0),
        before=before,
    )


@app.get(
    "/generation/requests/{request_uid}",
    response_model=GenerationTaskOut,
)
def generation_request_detail(
    request_uid: str,
    session: Session = Depends(get_session),
) -> GenerationTaskOut:
    request = session.scalar(
        select(GenerationRequest).where(GenerationRequest.uid == request_uid)
    )
    if request is None:
        raise HTTPException(status_code=404, detail="生成任务不存在")
    return generation_task_out(session, request)


@app.get("/generation/retention", response_model=GenerationRetentionOut)
def generation_retention(
    session: Session = Depends(get_session),
) -> GenerationRetentionOut:
    return GenerationRetentionOut(**generation_retention_status(session))


@app.get("/generation/control", response_model=GenerationControlOut)
def generation_control(session: Session = Depends(get_session)) -> GenerationControlOut:
    return generation_control_out(session)


@app.patch("/generation/control", response_model=GenerationControlOut)
def update_generation_control(
    payload: GenerationControlPatch,
    session: Session = Depends(get_session),
) -> GenerationControlOut:
    try:
        return save_generation_control(
            session, payload.model_dump(exclude_unset=True)
        )
    except GenerationControlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


@app.post(
    "/generation/requests/{request_uid}/approve",
    response_model=GenerationTaskOut,
)
def approve_generation_request_endpoint(
    request_uid: str,
    session: Session = Depends(get_session),
) -> GenerationTaskOut:
    request = generation_request_or_404(session, request_uid)
    try:
        approve_generation_request(session, request)
    except (
        GenerationApprovalConsumedError,
        GenerationPrivacyError,
        GenerationRequestHasResultError,
        GenerationRequestNotRetryableError,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    session.commit()
    return generation_task_out(session, request)


def generation_request_or_404(
    session: Session, request_uid: str
) -> GenerationRequest:
    request = session.scalar(
        select(GenerationRequest).where(GenerationRequest.uid == request_uid)
    )
    if request is None:
        raise HTTPException(status_code=404, detail="生成任务不存在")
    return request


@app.post(
    "/generation/requests/{request_uid}/reapply",
    response_model=GenerationTaskOut,
)
def reapply_generation_request_endpoint(
    request_uid: str,
    session: Session = Depends(get_session),
) -> GenerationTaskOut:
    request = generation_request_or_404(session, request_uid)
    try:
        request = (
            reapply_report_result(session, request=request)
            if request.task_type.startswith("report:")
            else reapply_generation_result(session, request=request)
        )
    except (GenerationResultReplayConflict, GenerationPrivacyError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except GenerationResultNotCurrentError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except SynthesisValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except GenerationProducerValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except ReportValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    except UnsupportedGenerationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except GenerationApplicationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    return generation_task_out(session, request)


def queue_overview(queue_name: str) -> dict[str, object]:
    try:
        connection = Redis.from_url(settings.redis_url)
        queue = Queue(queue_name, connection=connection)
        started_ids = {job_id.split(":", 1)[0] for job_id in StartedJobRegistry(queue.name, connection=connection).get_job_ids()}
        running_ids = started_ids | set(wip_job_ids(queue, connection))
        return {"available": True, "queued": queue_job_count(queue), "running": len(running_ids), "error": ""}
    except Exception:
        return {"available": False, "queued": 0, "running": 0, "error": "Redis 未连接"}


def queue_job_count(queue: Queue) -> int:
    count_attr = getattr(queue, "count", None)
    if isinstance(count_attr, int):
        return count_attr
    if callable(count_attr):
        try:
            return int(count_attr())
        except TypeError:
            pass
    return len(getattr(queue, "jobs", []) or [])


def pending_embedding_count(session: Session, model: str) -> int:
    return int(
        session.scalar(
            select(func.count(ContentItem.id))
            .join(Source, Source.id == ContentItem.source_id)
            .where(embedding_pending_clause(session, model))
            .where(
                Source.status == "active",
                Source.media_type == "article",
            )
        )
        or 0
    )


def generation_task_overview(session: Session) -> tuple[dict[str, int], str]:
    counts: dict[str, int] = {}
    latest_failed_error = ""
    # ponytail: O(n) personal-reader scan; add a status projection only if the
    # pipeline overview becomes measurably slow.
    requests = session.scalars(
        select(GenerationRequest).order_by(
            GenerationRequest.created_at.desc(), GenerationRequest.id.desc()
        )
    ).all()
    for task in generation_tasks_out(session, list(requests)):
        status = "failed" if task.status == "apply_failed" else task.status
        counts[status] = counts.get(status, 0) + 1
        if status == "failed" and not latest_failed_error:
            latest_failed_error = "生成任务失败，请在任务分配中查看详情"
    return counts, latest_failed_error


@app.post("/sources", response_model=SourceOut)
def create_source(payload: SourceCreate, session: Session = Depends(get_session)) -> Source:
    if payload.media_type not in SOURCE_MEDIA_TYPES:
        raise HTTPException(status_code=400, detail="不支持的媒体类型")
    if payload.status not in SOURCE_STATUSES:
        raise HTTPException(status_code=400, detail="不支持的订阅源状态")
    if payload.external_generation_allowed and payload.privacy_class != "public":
        raise HTTPException(status_code=400, detail="只有公开来源才能允许发送给外部生成服务")
    try:
        url = clean_source_url(payload.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    with clustering_run_execution_lock(session):
        defer_clustering_run_lock_until_transaction_end(session)
        ensure_folder(session, payload.folder_id, payload.media_type)
        existing = source_by_url(session, url)
        if existing:
            if existing.status == "archived":
                reactivation: dict[str, object] = {
                    "name": payload.name,
                    "folder_id": payload.folder_id,
                    "media_type": payload.media_type,
                    "status": payload.status,
                    "enabled": payload.status != "archived",
                    "fetch_full_content": payload.fetch_full_content,
                }
                if "article_selector" in payload.model_fields_set:
                    reactivation["article_selector"] = payload.article_selector
                if "remove_selector" in payload.model_fields_set:
                    reactivation["remove_selector"] = payload.remove_selector
                if "privacy_class" in payload.model_fields_set:
                    reactivation["privacy_class"] = payload.privacy_class
                if "external_generation_allowed" in payload.model_fields_set:
                    reactivation["external_generation_allowed"] = (
                        payload.external_generation_allowed
                    )
                apply_source_update(
                    session,
                    existing,
                    SourcePatch(**reactivation),
                )
            session.commit()
            session.refresh(existing)
            return existing
        source = Source(
            name=payload.name.strip() or url,
            url=url,
            folder_id=payload.folder_id,
            media_type=payload.media_type,
            status=payload.status,
            enabled=payload.status != "archived",
            fetch_full_content=payload.fetch_full_content,
            article_selector=payload.article_selector,
            remove_selector=payload.remove_selector,
            privacy_class=payload.privacy_class,
            external_generation_allowed=payload.external_generation_allowed,
        )
        session.add(source)
        session.commit()
        session.refresh(source)
        return source


@app.patch("/sources/{source_id}", response_model=SourceOut)
def update_source(source_id: int, payload: SourcePatch, session: Session = Depends(get_session)) -> Source:
    source = session.get(Source, source_id)
    if source is None or source.status == DELETED_SOURCE_STATUS:
        raise HTTPException(status_code=404, detail="订阅源不存在")
    apply_source_update(session, source, payload)
    session.commit()
    session.refresh(source)
    return source


@app.post("/sources/bulk", response_model=SourceBulkOut)
def bulk_update_sources(payload: SourceBulkPatch, session: Session = Depends(get_session)) -> SourceBulkOut:
    ids = list(dict.fromkeys(payload.ids))
    if not ids:
        return SourceBulkOut(updated=0)
    if not payload.changes.model_fields_set:
        raise HTTPException(status_code=400, detail="批量更新内容不能为空")
    changes_topology = source_update_changes_topology(payload.changes)
    update_lock = (
        clustering_run_execution_lock(session) if changes_topology else nullcontext()
    )
    with update_lock:
        if changes_topology:
            defer_clustering_run_lock_until_transaction_end(session)
        sources = session.scalars(
            select(Source).where(
                Source.id.in_(ids),
                Source.status != DELETED_SOURCE_STATUS,
            )
        ).all()
        if len(sources) != len(ids):
            raise HTTPException(status_code=404, detail="部分订阅源不存在")
        for source in sources:
            apply_source_update(session, source, payload.changes)
        session.commit()
    return SourceBulkOut(updated=len(sources))


@app.post("/sources/discover", response_model=SourceDiscoverOut)
def discover_source(payload: SourceDiscoverIn) -> SourceDiscoverOut:
    try:
        discovered = discover_feed_candidates(payload.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SourceDiscoverOut(
        candidates=[{"title": candidate.title, "url": candidate.url} for candidate in discovered.candidates],
        entries=[
            {
                "title": entry.title,
                "summary": entry.summary,
                "image_url": entry.image_url,
                "media_url": entry.media_url,
                "media_kind": entry.media_kind,
                "media_duration": entry.media_duration,
                "url": entry.url,
                "published_at": entry.published_at,
            }
            for entry in discovered.entries
        ],
        site_url=discovered.site_url,
        title=discovered.title,
    )


def apply_source_update(session: Session, source: Source, payload: SourcePatch | SourceBulkSet) -> None:
    changes_topology = source_update_changes_topology(payload)
    update_lock = (
        clustering_run_execution_lock(session) if changes_topology else nullcontext()
    )
    with update_lock:
        if changes_topology:
            defer_clustering_run_lock_until_transaction_end(session)
        locked_source = session.get(
            Source,
            source.id,
            populate_existing=True,
            with_for_update=True,
        )
        if locked_source is None or locked_source.status == DELETED_SOURCE_STATUS:
            raise HTTPException(status_code=404, detail="订阅源不存在")
        _apply_source_update_locked(session, locked_source, payload)


def source_update_changes_topology(payload: SourcePatch | SourceBulkSet) -> bool:
    return bool(payload.model_fields_set & {"media_type", "status"})


def _apply_source_update_locked(
    session: Session,
    source: Source,
    payload: SourcePatch | SourceBulkSet,
) -> None:
    previous_status = source.status
    fields = payload.model_fields_set
    name = getattr(payload, "name", None)
    url_value = getattr(payload, "url", None)
    media_type = getattr(payload, "media_type", None)
    status = getattr(payload, "status", None)
    enabled = getattr(payload, "enabled", None)
    fetch_full_content = getattr(payload, "fetch_full_content", None)
    article_selector = getattr(payload, "article_selector", None)
    remove_selector = getattr(payload, "remove_selector", None)
    privacy_class = getattr(payload, "privacy_class", None)
    external_generation_allowed = getattr(
        payload, "external_generation_allowed", None
    )
    if media_type is not None and media_type not in SOURCE_MEDIA_TYPES:
        raise HTTPException(status_code=400, detail="不支持的媒体类型")
    if status is not None and status not in SOURCE_STATUSES:
        raise HTTPException(status_code=400, detail="不支持的订阅源状态")
    if name is not None:
        source.name = name.strip() or source.name
    if url_value is not None:
        try:
            url = clean_source_url(url_value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        existing = source_by_url(session, url, exclude_id=source.id)
        if existing is not None:
            raise HTTPException(status_code=400, detail="RSS URL 已存在")
        if source.url != url:
            source.fetch_etag = None
            source.fetch_last_modified = None
            source.last_successful_payload_hash = None
        source.url = url
    next_media_type = media_type or source.media_type
    media_type_changed = source.media_type != next_media_type
    next_folder_id = (
        payload.folder_id
        if "folder_id" in fields
        else (None if media_type_changed else source.folder_id)
    )
    ensure_folder(session, next_folder_id, next_media_type)
    if media_type is not None:
        source.media_type = next_media_type
    if "folder_id" in fields or media_type is not None:
        source.folder_id = next_folder_id
    status_changed = status is not None and source.status != status
    if status is not None:
        if status_changed:
            source.status = status
            source.status_changed_at = now_utc()
        if source.status == "archived":
            source.enabled = False
        elif previous_status == "archived" and enabled is None:
            source.enabled = True
    if enabled is not None:
        source.enabled = enabled
    if fetch_full_content is not None:
        source.fetch_full_content = fetch_full_content
    if "article_selector" in fields:
        source.article_selector = article_selector
    if "remove_selector" in fields:
        source.remove_selector = remove_selector
    next_privacy_class = privacy_class or source.privacy_class
    next_external_generation_allowed = source.external_generation_allowed
    if privacy_class is not None and privacy_class != "public":
        next_external_generation_allowed = False
    if external_generation_allowed is not None:
        next_external_generation_allowed = external_generation_allowed
    if next_external_generation_allowed and next_privacy_class != "public":
        raise HTTPException(
            status_code=400,
            detail="只有公开来源才能允许发送给外部生成服务",
        )
    if (
        source.privacy_class != next_privacy_class
        or source.external_generation_allowed != next_external_generation_allowed
    ):
        source.privacy_class = next_privacy_class
        source.external_generation_allowed = next_external_generation_allowed
        source.generation_policy_version += 1
    if source.status == "archived":
        source.enabled = False
    if media_type_changed or status_changed:
        if source.status == "active" and source.media_type == "article":
            cluster_source_items(session, source.id)
        else:
            decluster_source_items(session, source.id)


def ensure_folder(session: Session, folder_id: int | None, media_type: str) -> None:
    if folder_id is None:
        return
    folder = session.get(Folder, folder_id)
    if folder is None:
        raise HTTPException(status_code=400, detail="文件夹不存在")
    if folder.media_type != media_type:
        raise HTTPException(status_code=400, detail="文件夹类型与订阅源类型不一致")


def source_media_filter(media_type: str):
    return Source.media_type == media_type


@app.delete("/sources/{source_id}", status_code=204)
def delete_source(source_id: int, session: Session = Depends(get_session)) -> Response:
    if not tombstone_source(session, source_id):
        raise HTTPException(status_code=404, detail="订阅源不存在")
    return Response(status_code=204)


@app.post("/sources/{source_id}/fetch", response_model=JobOut)
def fetch_source_job(source_id: int, session: Session = Depends(get_session)) -> JobOut:
    source = session.get(Source, source_id)
    if source is None or source.status == DELETED_SOURCE_STATUS:
        raise HTTPException(status_code=404, detail="订阅源不存在")
    if not source_is_fetch_eligible(source):
        raise HTTPException(status_code=409, detail="请先启用该订阅源")
    try:
        job_id = enqueue_source_fetch_job(source_id)
    except Exception:
        raise HTTPException(status_code=503, detail="RSS 抓取队列不可用") from None
    return JobOut(mode="queued", job_id=job_id)


@app.post("/imports/opml")
async def upload_opml(file: UploadFile = File(...), session: Session = Depends(get_session)) -> dict[str, int]:
    data = await file.read(MAX_OPML_BYTES + 1)
    if len(data) > MAX_OPML_BYTES:
        raise HTTPException(status_code=413, detail="OPML 文件超过 2MB 限制")
    try:
        return {"imported": import_opml(session, data.decode("utf-8"))}
    except (UnicodeDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc) or "OPML 格式无效") from exc


@app.get("/exports/opml")
def download_opml(session: Session = Depends(get_session)) -> Response:
    return Response(content=export_opml(session), media_type="text/xml", headers={"Content-Disposition": 'attachment; filename="reader.opml"'})


@app.post("/jobs/fetch", response_model=JobOut)
def fetch_job(session: Session = Depends(get_session)) -> JobOut:
    try:
        queued, job_id = begin_fetch_refresh(session)
    except Exception:
        raise HTTPException(status_code=503, detail="RSS 抓取队列不可用") from None
    return JobOut(mode="queued" if queued else "existing", job_id=job_id)


@app.get("/jobs/fetch/{job_id}", response_model=FetchJobStatusOut)
def fetch_job_status(
    job_id: str,
    session: Session = Depends(get_session),
) -> FetchJobStatusOut:
    if not job_id or len(job_id) > 128:
        raise HTTPException(status_code=404, detail="刷新任务不存在")
    try:
        status = fetch_refresh_status(session, job_id)
    except (NoSuchJobError, ValueError):
        raise HTTPException(status_code=404, detail="刷新任务不存在") from None
    except Exception:
        raise HTTPException(status_code=503, detail="刷新任务状态不可用") from None
    return FetchJobStatusOut(status=status)


@app.post("/jobs/embeddings", response_model=JobOut)
def embeddings_job(session: Session = Depends(get_session)) -> JobOut:
    try:
        queue = Queue(LLM_QUEUE_NAME, connection=Redis.from_url(settings.redis_url))
        job = queue.enqueue("reader_api.worker.embed_all", job_timeout=settings.rq_job_timeout_seconds)
        return JobOut(mode="queued", job_id=job.id)
    except Exception:
        ai_settings = translation_settings_for_source(
            runtime_ai_settings(session), None
        )
        embedding_provider = LocalEmbeddingProvider(ai_settings.embedding_base_url, ai_settings.timeout_seconds, settings.embedding_api_key)
        translation_provider = translation_chat_provider(ai_settings)
        embedded = embed_pending_items(
            session,
            embedding_provider,
            ai_settings.embedding_model,
            batch_limit=EMBEDDING_BATCH_LIMIT,
            max_batches=EMBEDDING_AUTO_MAX_BATCHES,
            translation_provider=translation_provider,
            translation_model=ai_settings.translation_model,
        )
        return JobOut(mode="inline", embedded=embedded, reclustered=0)


@app.get("/items", response_model=list[ItemOut])
def list_items(
    folder_id: int | None = None,
    source_id: int | None = None,
    media_type: str | None = None,
    q: str | None = None,
    read_status: str | None = None,
    read_later: bool | None = None,
    starred: bool | None = None,
    limit: int = 80,
    offset: int = 0,
    include_content: bool = True,
    filtered_only: bool = False,
    session: Session = Depends(get_session),
) -> list[ItemOut]:
    if filtered_only:
        return query_filtered_items(
            session,
            folder_id=folder_id,
            source_id=source_id,
            media_type=media_type,
            q=q,
            limit=limit,
            offset=offset,
            include_content=include_content,
        ).items
    return query_items(
        session,
        folder_id=folder_id,
        source_id=source_id,
        media_type=media_type,
        q=q,
        read_status=read_status,
        read_later=read_later,
        starred=starred,
        limit=limit,
        offset=offset,
        include_content=include_content,
    )


@app.get("/browse/summary", response_model=list[BrowseMediaSummaryOut])
def browse_summary(session: Session = Depends(get_session)) -> list[BrowseMediaSummaryOut]:
    return browse_media_summaries(session)


@app.get("/filtered-items", response_model=FilteredItemsOut)
def list_filtered_items(
    folder_id: int | None = None,
    source_id: int | None = None,
    media_type: str | None = None,
    q: str | None = None,
    limit: int = 80,
    offset: int = 0,
    include_content: bool = False,
    session: Session = Depends(get_session),
) -> FilteredItemsOut:
    return query_filtered_items(
        session,
        folder_id=folder_id,
        source_id=source_id,
        media_type=media_type,
        q=q,
        limit=limit,
        offset=offset,
        include_content=include_content,
    )


@app.get("/items/{item_id}", response_model=ItemOut)
def get_item(item_id: int, session: Session = Depends(get_session)) -> ItemOut:
    rows = query_items(session, item_id=item_id, limit=1)
    if not rows:
        raise HTTPException(status_code=404, detail="条目不存在")
    return rows[0]


@app.get("/items/{item_id}/summary", response_model=AISummaryOut)
def get_item_summary(item_id: int, session: Session = Depends(get_session)) -> AISummaryOut:
    if not query_items(session, item_id=item_id, limit=1, include_content=False):
        raise HTTPException(status_code=404, detail="条目不存在")
    generated = item_summary_generation_snapshot(session, item_id)
    if generated is not None and generated.status == "ready":
        return generated
    legacy = latest_item_summary_task(session, item_id, statuses={"complete"})
    if legacy is not None:
        return item_summary_snapshot(item_id, legacy)
    if generated is not None:
        return generated
    return item_summary_snapshot(item_id, None)


@app.post("/items/{item_id}/summarize", response_model=AISummaryOut)
def summarize_item(
    item_id: int,
    payload: SynthesisGenerateIn | None = None,
    session: Session = Depends(get_session),
) -> AISummaryOut:
    ai_settings = runtime_ai_settings(session)
    provider = requested_generation_provider(payload, ai_settings)
    if provider == "openai_compatible":
        try:
            validate_synthesis_remote_settings(ai_settings)
        except SynthesisSettingsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        prepared = prepare_item_summary(session, item_id)
    except GenerationProducerValidationError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    request = producer_generation_request(
        session,
        prepared=prepared,
        provider=provider,
        ai_settings=ai_settings,
    )
    request = execute_synchronous_producer(
        session,
        request=request,
        prepared=prepared,
        provider=provider,
        ai_settings=ai_settings,
    )
    snapshot = item_summary_generation_snapshot(session, item_id)
    assert snapshot is not None
    return snapshot


@app.get("/search", response_model=list[ItemOut])
def search(
    q: str,
    folder_id: int | None = None,
    source_id: int | None = None,
    media_type: str | None = None,
    read_status: str | None = None,
    read_later: bool | None = None,
    starred: bool | None = None,
    limit: int = 80,
    offset: int = 0,
    include_content: bool = True,
    session: Session = Depends(get_session),
) -> list[ItemOut]:
    return query_items(
        session,
        folder_id=folder_id,
        source_id=source_id,
        media_type=media_type,
        q=q,
        read_status=read_status,
        read_later=read_later,
        starred=starred,
        limit=limit,
        offset=offset,
        include_content=include_content,
    )


@app.patch("/user-state/{object_type}/{object_id}", response_model=UserStateOut)
def patch_user_state(object_type: str, object_id: int, payload: UserStatePatch, session: Session = Depends(get_session)) -> UserStateOut:
    if object_type not in OBJECT_STATE_TYPES:
        raise HTTPException(status_code=400, detail="不支持的状态对象类型")
    if payload.read_status is not None and payload.read_status not in READ_STATUSES:
        raise HTTPException(status_code=400, detail="不支持的阅读状态")
    try:
        result = apply_object_user_state_mutation(
            session, object_type, object_id, payload
        )
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise


@app.post(
    "/event-user-state",
    response_model=EventUserStateMutationOut,
    response_model_exclude_unset=True,
)
def post_event_user_state(
    payload: EventUserStateMutationIn,
    session: Session = Depends(get_session),
) -> EventUserStateMutationOut:
    try:
        result = apply_event_user_state_mutation(session, payload)
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise


@app.post("/uninterested", response_model=UninterestedMutationOut)
def post_uninterested(
    payload: UninterestedMutationIn,
    session: Session = Depends(get_session),
) -> UninterestedMutationOut:
    try:
        result = apply_uninterested_mutation(session, payload)
        session.commit()
        return result
    except Exception:
        session.rollback()
        raise


@app.get("/uninterested-targets", response_model=UninterestedTargetsOut)
def get_uninterested_targets(
    q: str | None = None,
    reason: str | None = None,
    source_id: int | None = None,
    folder_id: int | None = None,
    limit: int = 80,
    offset: int = 0,
    session: Session = Depends(get_session),
) -> UninterestedTargetsOut:
    return list_uninterested_targets(
        session,
        q=q,
        reason=reason,
        source_id=source_id,
        folder_id=folder_id,
        limit=limit,
        offset=offset,
    )


@app.post(
    "/user-state/bulk-read/prepare",
    response_model=BulkReadPrepared,
    response_model_exclude_none=True,
)
def prepare_bulk_mark_read(
    payload: BulkReadPrepareIn,
    session: Session = Depends(get_session),
    connection: Redis = Depends(get_bulk_read_redis),
) -> BulkReadPrepared:
    ids = bulk_unread_ids(session, payload)
    return store_bulk_read_manifest(
        connection,
        prepare_bulk_read_manifest(
            session,
            object_type=payload.object_type,
            object_ids=ids,
        ),
    )


@app.post("/user-state/bulk-read")
def bulk_mark_read(
    payload: BulkReadConfirmIn,
    session: Session = Depends(get_session),
    connection: Redis = Depends(get_bulk_read_redis),
) -> dict[str, int]:
    try:
        updated = confirm_bulk_read_batch(
            session,
            connection,
            str(payload.batch_id),
        )
        session.commit()
        return {"updated": updated}
    except Exception:
        session.rollback()
        raise


@app.get("/interactions", response_model=list[InteractionAuditOut])
def list_interactions(
    event_uid: str | None = None,
    object_type: str | None = None,
    object_id: int | None = None,
    session: Session = Depends(get_session),
) -> list[InteractionAuditOut]:
    has_event_target = event_uid is not None
    has_object_target = object_type is not None or object_id is not None
    if has_event_target == has_object_target:
        raise HTTPException(status_code=400, detail="必须且只能指定一个审计目标")
    if has_object_target and (
        object_type not in OBJECT_STATE_TYPES
        or object_id is None
        or object_id < 1
    ):
        raise HTTPException(status_code=400, detail="对象审计目标不完整")

    event = aliased(Event)
    revision = aliased(EventRevision)
    statement = (
        select(InteractionEvent, event.uid, revision.uid)
        .outerjoin(event, event.id == InteractionEvent.event_id)
        .outerjoin(revision, revision.id == InteractionEvent.observed_revision_id)
        .order_by(
            InteractionEvent.occurred_at,
            InteractionEvent.recorded_at,
            InteractionEvent.id,
        )
    )
    if event_uid is not None:
        event_id = session.scalar(select(Event.id).where(Event.uid == event_uid))
        if event_id is None:
            raise HTTPException(status_code=404, detail="Event 不存在")
        statement = statement.where(InteractionEvent.event_id == event_id)
    else:
        statement = statement.where(
            InteractionEvent.object_type == object_type,
            InteractionEvent.object_id == object_id,
        )
    return [
        InteractionAuditOut(
            interaction_id=interaction.id,
            operation_id=interaction.operation_id,
            target_kind=(
                "event" if interaction.target_kind == "event" else "object"
            ),
            event_uid=resolved_event_uid,
            observed_revision_uid=observed_revision_uid,
            object_type=interaction.object_type,
            object_id=interaction.object_id,
            action=interaction.action,
            set_value=interaction.set_value,
            occurred_at=interaction.occurred_at,
            recorded_at=interaction.recorded_at,
        )
        for interaction, resolved_event_uid, observed_revision_uid in session.execute(
            statement
        )
    ]


@app.get("/events/{event_uid}", response_model=EventReadOut)
def get_event(
    event_uid: str,
    session: Session = Depends(get_session),
) -> EventReadOut:
    event = session.scalar(select(Event).where(Event.uid == event_uid))
    if event is None:
        raise HTTPException(status_code=404, detail="Event 不存在")
    return event_read_out(session, event)


@app.post(
    "/events/{event_uid}/readability-source",
    response_model=EventReadabilitySourceOut,
)
def get_event_readability_source(
    event_uid: str,
    request: EventReadabilitySourceIn,
    session: Session = Depends(get_session),
) -> EventReadabilitySourceOut:
    event = session.scalar(select(Event).where(Event.uid == event_uid))
    if event is None:
        raise HTTPException(status_code=404, detail="Event 不存在")
    revision = session.scalar(
        select(EventRevision).where(
            EventRevision.uid == request.observed_revision_uid
        )
    )
    if revision is None:
        raise HTTPException(status_code=404, detail="Event Revision 不存在")
    if revision.event_id != event.id:
        raise HTTPException(
            status_code=409,
            detail="observed revision 不属于目标 Event",
        )
    version = session.scalar(
        select(EventEvidenceVersion)
        .join(
            EventRevisionEvidence,
            EventRevisionEvidence.evidence_version_id
            == EventEvidenceVersion.id,
        )
        .where(
            EventRevisionEvidence.revision_id == revision.id,
            EventEvidenceVersion.uid == request.evidence_version_uid,
        )
    )
    if (
        version is None
        or version.source_id != request.source_id
        or version.legacy_content_item_id_snapshot != request.item_id
        or version.url_snapshot != request.url
    ):
        raise HTTPException(
            status_code=409,
            detail="Readability 来源不属于 observed revision 或精确 Evidence",
        )
    return EventReadabilitySourceOut(
        event_uid=event.uid,
        observed_revision_uid=revision.uid,
        evidence_version_uid=version.uid,
        source_id=version.source_id,
        item_id=version.legacy_content_item_id_snapshot,
        url=version.url_snapshot,
        title=version.title_snapshot,
    )


def raise_event_generation_privacy_block(
    session: Session,
    *,
    event: Event,
    task_type: str,
    provider: str,
    model: str,
    input_fingerprint: str,
    privacy_reason: str,
    source_policy_fingerprint: str | None,
    source_policies: list[Source],
) -> None:
    prompt_version, schema_version = (
        (SYNTHESIS_PROMPT_VERSION, SYNTHESIS_SCHEMA_VERSION)
        if task_type == SYNTHESIS_TASK_TYPE
        else (EVIDENCE_REVIEW_PROMPT_VERSION, EVIDENCE_REVIEW_SCHEMA_VERSION)
    )
    get_or_create_generation_request(
        session,
        task_type=task_type,
        reason="explicit-user-request",
        target_type="event",
        target_id=event.id,
        target_uid=event.uid,
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        schema_version=schema_version,
        input_fingerprint=input_fingerprint,
        payload=None,
        privacy_status="blocked",
        privacy_reason=privacy_reason,
        source_policy_fingerprint=source_policy_fingerprint,
        source_policies=source_policies,
    )
    session.commit()
    raise HTTPException(status_code=403, detail=privacy_reason)


def review_unreviewed_event_synthesis(
    session: Session,
    event: Event,
    revision: EventRevision,
    provider: str,
    ai_settings: RuntimeAISettings,
) -> EventSynthesisStateOut:
    current = session.get(SynthesisVersion, event.current_synthesis_version_id)
    if current is None:
        raise HTTPException(status_code=409, detail="当前合成稿不可用，请刷新后重试")
    latest_review = latest_ordinary_review_for_synthesis(session, event, current)
    baseline_snapshot = session.get(
        EvidenceSnapshot,
        latest_review.target_snapshot_id
        if latest_review is not None
        else current.snapshot_id,
    )
    if baseline_snapshot is None:
        raise HTTPException(status_code=409, detail="当前合成稿的证据快照不可用")
    target_source_ids = synthesis_source_ids_for_revision(session, revision)
    baseline_source_ids = [
        int(row["source_id"])
        for row in synthesis_snapshot_evidence(session, baseline_snapshot)
    ]
    source_ids = sorted({*target_source_ids, *baseline_source_ids})
    source_policies: list[Source] = []
    source_policy_fingerprint: str | None = None
    if provider == "openai_compatible":
        source_policies, source_policy_fingerprint, privacy_reason = (
            external_generation_policy(session, source_ids, lock_sources=True)
        )
        if privacy_reason:
            raise_event_generation_privacy_block(
                session,
                event=event,
                task_type=EVIDENCE_REVIEW_TASK_TYPE,
                provider=provider,
                model=ai_settings.synthesis_remote_model,
                input_fingerprint=stable_hash(
                    {
                        "event_uid": event.uid,
                        "baseline_snapshot_uid": baseline_snapshot.uid,
                        "target_revision_uid": revision.uid,
                    }
                ),
                privacy_reason=privacy_reason,
                source_policy_fingerprint=source_policy_fingerprint,
                source_policies=source_policies,
            )
    target_snapshot, _evidence, _source_count, _generation_fingerprint = (
        create_evidence_snapshot(session, event, revision)
    )
    input_data, _new_source_count = evidence_review_input_data(
        session, event, baseline_snapshot, target_snapshot
    )
    comparison_fingerprint = str(input_data["comparison_fingerprint"])
    completed = session.scalar(
        select(EvidenceReview).where(
            EvidenceReview.event_id == event.id,
            EvidenceReview.comparison_fingerprint == comparison_fingerprint,
        )
    )
    if completed is not None:
        if completed.result == "ordinary":
            advance_review_pointer(session, event, completed)
        session.commit()
        return event_synthesis_state(session, event, revision)
    active_request = latest_generation_request(
        session,
        task_type=EVIDENCE_REVIEW_TASK_TYPE,
        target_type="event",
        target_id=event.id,
        input_fingerprint=comparison_fingerprint,
    )
    if active_request is not None:
        reused = reuse_active_event_generation(
            session, event=event, revision=revision, request=active_request
        )
        if reused is not None:
            return reused
    if provider == "openai_compatible":
        try:
            validate_synthesis_remote_settings(ai_settings)
        except SynthesisSettingsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
    request = evidence_review_model_request(input_data)
    input_text = str(request["input"])
    model = (
        ai_settings.synthesis_remote_model
        if provider == "openai_compatible"
        else ai_settings.llm_model
    )
    llm_provider = (
        synthesis_remote_provider(ai_settings)
        if provider == "openai_compatible"
        else local_llm(ai_settings)
    )
    generation_request, _created = get_or_create_generation_request(
        session,
        task_type=EVIDENCE_REVIEW_TASK_TYPE,
        reason="explicit-user-request",
        target_type="event",
        target_id=event.id,
        target_uid=event.uid,
        provider=provider,
        model=model,
        prompt_version=EVIDENCE_REVIEW_PROMPT_VERSION,
        schema_version=EVIDENCE_REVIEW_SCHEMA_VERSION,
        input_fingerprint=comparison_fingerprint,
        payload=request,
        privacy_status=("eligible" if provider == "openai_compatible" else "local"),
        source_policy_fingerprint=source_policy_fingerprint,
        source_policies=source_policies,
    )
    return execute_inline_event_generation(
        session,
        event=event,
        revision=revision,
        request=generation_request,
        llm_provider=llm_provider,
        system_prompt=EVIDENCE_REVIEW_SYSTEM_PROMPT,
        input_text=input_text,
        allowed_version_uids={
            str(row["evidence_version_uid"])
            for row in input_data["target_evidence"]
            if isinstance(row, dict)
        },
    )


def execute_inline_event_generation(
    session: Session,
    *,
    event: Event,
    revision: EventRevision,
    request: GenerationRequest,
    llm_provider: object,
    system_prompt: str,
    input_text: str,
    allowed_version_uids: set[str],
) -> EventSynthesisStateOut:
    existing = generation_result_for_request(session, request.id)
    if existing is not None:
        _result, application = existing
        if application.status != "applied":
            try:
                reapply_generation_result(session, request=request)
            except GenerationResultNotCurrentError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from None
            except GenerationPrivacyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from None
            except GenerationApplicationError as exc:
                raise HTTPException(status_code=503, detail=str(exc)) from None
        else:
            session.commit()
        latest_event = session.get(Event, event.id, populate_existing=True)
        latest_revision = session.get(
            EventRevision,
            latest_event.current_revision_id if latest_event is not None else None,
            populate_existing=True,
        )
        assert latest_event is not None and latest_revision is not None
        return event_synthesis_state(session, latest_event, latest_revision)
    attempt = latest_attempt(session, request.id)
    if attempt is not None and attempt.status in {"pending", "running"}:
        session.commit()
        return event_synthesis_state(session, event, revision)
    approve_generation_request(session, request, allow_consumed_reissue=True)
    attempt = start_admitted_generation_attempt(session, request)
    if attempt is None:
        session.commit()
        return event_synthesis_state(session, event, revision)
    session.commit()
    attempt_id = attempt.id
    input_tokens: int | None = None
    output_tokens: int | None = None
    try:
        provider_result = llm_provider.chat(
            request.model,
            system_prompt,
            input_text,
        )
        input_tokens, output_tokens = token_usage(provider_result)
        result_payload, material_rewrite_missing = (
            validated_event_generation_result(
                request.task_type, provider_result, allowed_version_uids
            )
        )
        if material_rewrite_missing:
            frozen = session.get(GenerationRequestPayload, request.id)
            assert frozen is not None
            reject_material_review_without_synthesis(
                session,
                event=event,
                request=frozen.payload_json,
                payload=provider_result,
                provider=request.provider,
                model=request.model,
            )
    except (SynthesisValidationError, RuntimeError) as exc:
        validation_failed = isinstance(exc, SynthesisValidationError)
        error = (
            str(exc)
            if validation_failed
            else "云端合成服务不可用，请检查地址、模型和密钥"
            if request.provider == "openai_compatible"
            else "本地模型服务未连接，请检查 LM Studio"
        )
        attempt = session.scalar(
            select(GenerationAttempt)
            .where(GenerationAttempt.id == attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert attempt is not None
        fail_generation_attempt(
            session,
            attempt,
            error,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        session.commit()
        if request.task_type == SYNTHESIS_TASK_TYPE:
            reusable = session.scalar(
                select(SynthesisVersion).where(
                    SynthesisVersion.event_id == event.id,
                    SynthesisVersion.generation_fingerprint
                    == request.input_fingerprint,
                )
            )
            if reusable is not None:
                latest_event = session.get(Event, event.id, populate_existing=True)
                assert latest_event is not None
                latest_revision = session.get(
                    EventRevision,
                    latest_event.current_revision_id,
                    populate_existing=True,
                )
                assert latest_revision is not None
                return event_synthesis_state(
                    session, latest_event, latest_revision
                )
        raise HTTPException(
            status_code=502,
            detail=(
                "模型返回的合成内容无法使用，请重试"
                if validation_failed
                else error
            ),
        ) from None
    attempt = session.scalar(
        select(GenerationAttempt)
        .where(GenerationAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    assert attempt is not None
    complete_generation_attempt(
        session,
        attempt=attempt,
        payload=result_payload,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    request_id = request.id
    session.commit()
    request = session.get(GenerationRequest, request_id)
    assert request is not None
    try:
        reapply_generation_result(session, request=request, pending_only=True)
    except GenerationResultNotCurrentError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except GenerationPrivacyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except GenerationApplicationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    latest_event = session.get(Event, event.id, populate_existing=True)
    latest_revision = session.get(
        EventRevision,
        latest_event.current_revision_id if latest_event is not None else None,
        populate_existing=True,
    )
    assert latest_event is not None and latest_revision is not None
    return event_synthesis_state(session, latest_event, latest_revision)


def reuse_active_event_generation(
    session: Session,
    *,
    event: Event,
    revision: EventRevision,
    request: GenerationRequest,
) -> EventSynthesisStateOut | None:
    frozen = session.get(GenerationRequestPayload, request.id)
    if (
        frozen is not None
        and frozen.payload_json is None
        and generation_result_for_request(session, request.id) is None
    ):
        return None
    status = generation_task_out(session, request).status
    if status not in {"pending", "running", "apply_failed", "complete"}:
        return None
    if status == "apply_failed":
        try:
            reapply_generation_result(session, request=request)
        except GenerationResultNotCurrentError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        except GenerationPrivacyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        except GenerationApplicationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from None
    else:
        session.commit()
    latest_event = session.get(Event, event.id, populate_existing=True)
    latest_revision = session.get(
        EventRevision,
        latest_event.current_revision_id if latest_event is not None else None,
        populate_existing=True,
    )
    assert latest_event is not None and latest_revision is not None
    return event_synthesis_state(session, latest_event, latest_revision)


@app.post("/events/{event_uid}/synthesis", response_model=EventSynthesisStateOut)
def generate_event_synthesis(
    event_uid: str,
    payload: SynthesisGenerateIn,
    session: Session = Depends(get_session),
) -> EventSynthesisStateOut:
    event = session.scalar(
        select(Event).where(Event.uid == event_uid).with_for_update()
    )
    if event is None:
        raise HTTPException(status_code=404, detail="事件不存在")
    if event.status != "active":
        raise HTTPException(status_code=409, detail="这条事件已被取代，不能生成新合成稿")
    revision = session.get(EventRevision, event.current_revision_id)
    if revision is None:
        raise HTTPException(status_code=409, detail="事件的最新内容不可用，请刷新后重试")
    ai_settings = runtime_ai_settings(session)
    provider = (payload.provider or ai_settings.synthesis_provider).strip().lower()
    if provider not in {"local", "openai_compatible"}:
        raise HTTPException(status_code=400, detail="合成稿服务选择无效")
    current_state = event_synthesis_state(session, event, revision)
    if current_state.status == "unreviewed":
        return review_unreviewed_event_synthesis(
            session, event, revision, provider, ai_settings
        )
    current = (
        session.get(SynthesisVersion, event.current_synthesis_version_id)
        if event.current_synthesis_version_id is not None
        else None
    )
    if matching_ordinary_review(session, event, current, revision) is not None:
        session.commit()
        return current_state
    try:
        source_count, generation_fingerprint = synthesis_generation_for_revision(
            session, revision
        )
    except SynthesisValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    completed = session.scalar(
        select(SynthesisVersion).where(
            SynthesisVersion.event_id == event.id,
            SynthesisVersion.generation_fingerprint == generation_fingerprint,
        )
    )
    if completed is not None:
        # The current Revision can legitimately recur to an older immutable
        # generation; this is an explicit current-target cache hit, not a late
        # callback trying to roll the pointer back.
        event.current_synthesis_version_id = completed.id
        advance_material_review_for_synthesis(session, event, completed)
        session.commit()
        return event_synthesis_state(session, event, revision)
    source_policies: list[Source] = []
    source_policy_fingerprint: str | None = None
    source_ids = synthesis_source_ids_for_revision(session, revision)
    if provider == "openai_compatible":
        source_policies, source_policy_fingerprint, privacy_reason = (
            external_generation_policy(session, source_ids, lock_sources=True)
        )
        if privacy_reason:
            raise_event_generation_privacy_block(
                session,
                event=event,
                task_type=SYNTHESIS_TASK_TYPE,
                provider=provider,
                model=ai_settings.synthesis_remote_model,
                input_fingerprint=generation_fingerprint,
                privacy_reason=privacy_reason,
                source_policy_fingerprint=source_policy_fingerprint,
                source_policies=source_policies,
            )
    active_request = latest_generation_request(
        session,
        task_type=SYNTHESIS_TASK_TYPE,
        target_type="event",
        target_id=event.id,
        input_fingerprint=generation_fingerprint,
    )
    if active_request is not None:
        reused = reuse_active_event_generation(
            session, event=event, revision=revision, request=active_request
        )
        if reused is not None:
            return reused
    if provider == "openai_compatible":
        try:
            validate_synthesis_remote_settings(ai_settings)
        except SynthesisSettingsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
    snapshot, evidence, snapshot_source_count, snapshot_fingerprint = (
        create_evidence_snapshot(session, event, revision)
    )
    assert (snapshot_source_count, snapshot_fingerprint) == (
        source_count,
        generation_fingerprint,
    )
    input_data = synthesis_input_data(
        event, revision, snapshot, evidence, generation_fingerprint
    )
    request = synthesis_model_request(input_data)
    input_text = str(request["input"])
    model = (
        ai_settings.synthesis_remote_model
        if provider == "openai_compatible"
        else ai_settings.llm_model
    )
    llm_provider = (
        synthesis_remote_provider(ai_settings)
        if provider == "openai_compatible"
        else local_llm(ai_settings)
    )
    generation_request, _created = get_or_create_generation_request(
        session,
        task_type=SYNTHESIS_TASK_TYPE,
        reason="explicit-user-request",
        target_type="event",
        target_id=event.id,
        target_uid=event.uid,
        provider=provider,
        model=model,
        prompt_version=SYNTHESIS_PROMPT_VERSION,
        schema_version=SYNTHESIS_SCHEMA_VERSION,
        input_fingerprint=generation_fingerprint,
        payload=request,
        privacy_status=("eligible" if provider == "openai_compatible" else "local"),
        source_policy_fingerprint=source_policy_fingerprint,
        source_policies=source_policies,
    )
    return execute_inline_event_generation(
        session,
        event=event,
        revision=revision,
        request=generation_request,
        llm_provider=llm_provider,
        system_prompt=SYNTHESIS_SYSTEM_PROMPT,
        input_text=input_text,
        allowed_version_uids={
            str(row["evidence_version_uid"]) for row in evidence
        },
    )


@app.get(
    "/events/{event_uid}/revisions/{revision_uid}",
    response_model=EventRevisionDetailOut,
)
def get_event_revision(
    event_uid: str,
    revision_uid: str,
    session: Session = Depends(get_session),
) -> EventRevisionDetailOut:
    row = session.execute(
        select(Event, EventRevision)
        .join(EventRevision, EventRevision.event_id == Event.id)
        .where(Event.uid == event_uid, EventRevision.uid == revision_uid)
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Event Revision 不存在")
    _event, revision = row
    evidence_rows = session.execute(
        select(
            EventRevisionEvidence,
            EventEvidenceVersion,
            EventEvidence,
            Source,
        )
        .join(
            EventEvidenceVersion,
            EventEvidenceVersion.id
            == EventRevisionEvidence.evidence_version_id,
        )
        .join(EventEvidence, EventEvidence.id == EventEvidenceVersion.evidence_id)
        .join(Source, Source.id == EventEvidenceVersion.source_id)
        .where(EventRevisionEvidence.revision_id == revision.id)
        .order_by(EventRevisionEvidence.id)
    ).all()
    reading_documents = event_evidence_reading_documents(
        session, [version for _link, version, _evidence, _source in evidence_rows]
    )
    return EventRevisionDetailOut(
        **event_revision_summary(revision).model_dump(),
        evidence=[
            EventEvidenceSnapshotOut(
                evidence_uid=evidence.uid,
                identity_fingerprint=evidence.identity_fingerprint,
                version_uid=version.uid,
                version_fingerprint=version.version_fingerprint,
                evidence_type=link.evidence_type,
                role=link.role,
                source=EventEvidenceSourceOut(
                    source_id=source.id,
                    name=source.name,
                    feed_url=source.url,
                    site_url=source.site_url,
                    media_type=source.media_type,
                ),
                source_entry_id=version.source_entry_id,
                raw_entry_id=version.raw_entry_id,
                raw_revision_no=version.raw_revision_no,
                legacy_content_item_id=version.legacy_content_item_id,
                legacy_content_item_id_snapshot=(
                    version.legacy_content_item_id_snapshot
                ),
                fragment_fingerprint=version.fragment_fingerprint,
                title=version.title_snapshot,
                url=version.url_snapshot,
                author=version.author_snapshot,
                published_at=version.published_at_snapshot,
                content=version.content_snapshot,
                reading_html=(
                    reading_documents[version.id].reading_html
                    if reading_documents[version.id] is not None
                    else None
                ),
                body_source=(
                    reading_documents[version.id].body_source
                    if reading_documents[version.id] is not None
                    else None
                ),
                web_fetch_status=(
                    reading_documents[version.id].web_fetch_status
                    if reading_documents[version.id] is not None
                    else None
                ),
            )
            for link, version, evidence, source in evidence_rows
        ],
    )


def event_evidence_reading_documents(
    session: Session,
    versions: list[EventEvidenceVersion],
) -> dict[int, Document | None]:
    item_ids = {
        version.legacy_content_item_id
        for version in versions
        if version.legacy_content_item_id is not None
    }
    documents = {
        item_id: document
        for item_id, document in session.execute(
            select(ContentItem.id, Document)
            .join(Document, Document.id == ContentItem.document_id)
            .options(
                undefer(Document.reading_html),
                undefer(Document.body_source),
                undefer(Document.web_fetch_status),
            )
            .where(ContentItem.id.in_(item_ids))
        )
    }
    matches: dict[int, Document | None] = {}
    for version in versions:
        document = documents.get(version.legacy_content_item_id)
        matches[version.id] = (
            document
            if (
                document is not None
                and document.raw_entry_id == version.raw_entry_id
                and document.content_text == version.content_snapshot
            )
            else None
        )
    return matches


def event_revision_summary(revision: EventRevision) -> EventRevisionSummaryOut:
    return EventRevisionSummaryOut(
        revision_uid=revision.uid,
        revision_no=revision.revision_no,
        title=revision.title_snapshot,
        event_time=revision.event_time_snapshot,
        evidence_fingerprint=revision.evidence_fingerprint,
        created_at=revision.created_at,
    )


def event_read_out(session: Session, event: Event) -> EventReadOut:
    revisions = list(
        session.scalars(
            select(EventRevision)
            .where(EventRevision.event_id == event.id)
            .order_by(EventRevision.revision_no.desc())
        )
    )
    revision_by_id = {revision.id: revision for revision in revisions}
    current_revision = revision_by_id.get(event.current_revision_id)
    if current_revision is None:
        raise HTTPException(status_code=409, detail="Event 当前 Revision 缺失")
    state = session.scalar(
        select(EventUserState).where(EventUserState.event_id == event.id)
    )
    seen_revision = (
        revision_by_id.get(state.seen_revision_id)
        if state is not None and state.seen_revision_id is not None
        else None
    )
    projection = None
    if event.status == "active":
        projection = session.scalar(
            select(ClusterEventProjection)
            .where(ClusterEventProjection.event_id == event.id)
            .order_by(ClusterEventProjection.id.desc())
            .limit(1)
        )
    if projection is not None:
        latest_projection_id = session.scalar(
            select(ClusterEventProjection.id)
            .where(
                ClusterEventProjection.cluster_id_snapshot
                == projection.cluster_id_snapshot
            )
            .order_by(ClusterEventProjection.id.desc())
            .limit(1)
        )
        if projection.id != latest_projection_id:
            projection = None
    material_update_revision_uid = event_material_updates_for(
        session, [event.id]
    ).get(event.id)

    return EventReadOut(
        event_uid=event.uid,
        status=event.status,
        created_at=event.created_at,
        superseded_at=event.superseded_at,
        current_revision=event_revision_summary(current_revision),
        seen_revision=(
            event_revision_summary(seen_revision) if seen_revision else None
        ),
        current_revision_differs_from_seen=(
            seen_revision is None or current_revision.id != seen_revision.id
        ),
        has_material_update=material_update_revision_uid is not None,
        material_update_revision_uid=material_update_revision_uid,
        user_state=EventUserStateReadOut(
            read_status=state.read_status if state else "unread",
            read_later=bool(state and state.read_later),
            starred=bool(state and state.starred),
            uninterested=bool(state and state.uninterested),
            uninterested_reason=state.uninterested_reason if state else None,
            uninterested_note=state.uninterested_note if state else None,
            uninterested_at=state.uninterested_at if state else None,
            updated_at=state.updated_at if state else None,
        ),
        current_projection=(
            EventClusterProjectionOut(
                cluster_id=projection.cluster_id,
                cluster_id_snapshot=projection.cluster_id_snapshot,
                clustering_run_id=projection.clustering_run_id,
                revision_uid=revision_by_id[projection.event_revision_id].uid,
                reconciliation_kind=projection.reconciliation_kind,
                rule_version=projection.reconciliation_rule_version,
                before_evidence_fingerprint=projection.before_evidence_fingerprint,
                after_evidence_fingerprint=projection.after_evidence_fingerprint,
                projected_at=projection.projected_at,
            )
            if projection is not None
            else None
        ),
        revisions=[event_revision_summary(revision) for revision in revisions],
        lineage=event_lineage_out(session, event),
        successors=event_successors_out(session, event),
        synthesis=event_synthesis_state(session, event, current_revision),
    )


def event_lineage_out(session: Session, event: Event) -> list[EventLineageOut]:
    predecessor = aliased(ClusterEventProjection)
    predecessor_revision = aliased(EventRevision)
    target_revision = aliased(EventRevision)
    continued = [
        EventLineageOut(
            lineage_uid=None,
            relation_type="continued",
            direction="self",
            source_event_uid=event.uid,
            target_event_uid=event.uid,
            source_revision_uid=source_revision_uid,
            target_revision_uid=target_revision_uid,
            clustering_run_id=projection.clustering_run_id,
            rule_version=projection.reconciliation_rule_version,
            before_evidence_fingerprint=projection.before_evidence_fingerprint,
            after_evidence_fingerprint=projection.after_evidence_fingerprint,
            decision_reason=None,
            recorded_at=projection.projected_at,
        )
        for projection, source_revision_uid, target_revision_uid in session.execute(
            select(
                ClusterEventProjection,
                predecessor_revision.uid,
                target_revision.uid,
            )
            .join(
                predecessor,
                predecessor.id
                == ClusterEventProjection.predecessor_projection_id,
            )
            .join(
                predecessor_revision,
                predecessor_revision.id == predecessor.event_revision_id,
            )
            .join(
                target_revision,
                target_revision.id == ClusterEventProjection.event_revision_id,
            )
            .where(
                ClusterEventProjection.event_id == event.id,
                ClusterEventProjection.reconciliation_kind == "continued",
            )
            .order_by(ClusterEventProjection.projected_at, ClusterEventProjection.id)
        )
    ]

    source_event = aliased(Event)
    target_event = aliased(Event)
    explicit = [
        EventLineageOut(
            lineage_uid=lineage.uid,
            relation_type=lineage.relation_type,
            direction="outgoing" if lineage.source_event_id == event.id else "incoming",
            source_event_uid=source_uid,
            target_event_uid=target_uid,
            clustering_run_id=lineage.clustering_run_id,
            rule_version=lineage.rule_version,
            before_evidence_fingerprint=lineage.before_evidence_fingerprint,
            after_evidence_fingerprint=lineage.after_evidence_fingerprint,
            decision_reason=lineage.decision_reason,
            recorded_at=lineage.created_at,
        )
        for lineage, source_uid, target_uid in session.execute(
            select(EventLineage, source_event.uid, target_event.uid)
            .join(source_event, source_event.id == EventLineage.source_event_id)
            .join(target_event, target_event.id == EventLineage.target_event_id)
            .where(
                or_(
                    EventLineage.source_event_id == event.id,
                    EventLineage.target_event_id == event.id,
                )
            )
            .order_by(EventLineage.created_at, EventLineage.id)
        )
    ]
    return continued + explicit


def event_successors_out(
    session: Session,
    event: Event,
) -> list[EventSuccessorOut]:
    current_revision = aliased(EventRevision)
    return [
        EventSuccessorOut(
            event_uid=successor.uid,
            relation_type=lineage.relation_type,
            status=successor.status,
            current_revision_uid=revision.uid,
            title=revision.title_snapshot,
        )
        for lineage, successor, revision in session.execute(
            select(EventLineage, Event, current_revision)
            .join(Event, Event.id == EventLineage.target_event_id)
            .join(current_revision, current_revision.id == Event.current_revision_id)
            .where(EventLineage.source_event_id == event.id)
            .order_by(EventLineage.id)
        )
    ]


@app.get("/clusters", response_model=list[ClusterOut])
def list_clusters(
    folder_id: int | None = None,
    source_id: int | None = None,
    q: str | None = None,
    read_status: str | None = None,
    read_later: bool | None = None,
    starred: bool | None = None,
    limit: int = 80,
    offset: int = 0,
    cursor_id: int | None = None,
    order: str = "desc",
    session: Session = Depends(get_session),
) -> list[ClusterOut]:
    if order not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="未知排序方式")
    if cursor_id is not None and offset:
        raise HTTPException(status_code=400, detail="分页游标不能与偏移量同时使用")
    cursor = session.get(Cluster, cursor_id) if cursor_id is not None else None
    if cursor_id is not None and cursor is None:
        raise HTTPException(status_code=400, detail="分页游标不存在")
    (
        current_event_state,
        effective_read_status,
        effective_read_later,
        effective_starred,
    ) = cluster_effective_state_expressions(session)
    stmt = (
        select(Cluster, func.count(ClusterItem.id))
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(current_event_state, current_event_state.c.cluster_id == Cluster.id, isouter=True)
        .where(
            Source.status == "active",
            Source.media_type == "article",
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            ordinary_content_clause(session, ContentItem.id),
        )
    )
    active_lookup = source_id is not None or bool(q) or read_later is not None or starred is not None
    if not active_lookup:
        stmt = stmt.where(unfiltered_content_clause(ContentItem.id))
    if read_status:
        if read_status not in READ_STATUSES:
            raise HTTPException(status_code=400, detail="未知阅读状态")
        stmt = stmt.where(effective_read_status == read_status)
    elif read_later is None and starred is None:
        stmt = stmt.where(effective_read_status != "dismissed")
    if read_later is not None:
        stmt = stmt.where(effective_read_later.is_(read_later))
    if starred is not None:
        stmt = stmt.where(effective_starred.is_(starred))
    if folder_id is not None or source_id is not None or q:
        matching_clusters = (
            select(ClusterItem.cluster_id)
            .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
            .join(Source, Source.id == ContentItem.source_id)
            .join(Cluster, Cluster.id == ClusterItem.cluster_id)
            .where(Source.status == "active", Source.media_type == "article")
        )
        if folder_id is not None:
            matching_clusters = matching_clusters.where(Source.folder_id == folder_id)
        if source_id is not None:
            matching_clusters = matching_clusters.where(Source.id == source_id)
        if q and can_use_indexed_search(session, q):
            matching_clusters = indexed_cluster_search_query(
                session, q, folder_id, source_id
            )
        elif q:
            matching_clusters = matching_clusters.where(
                ordinary_content_clause(session, ContentItem.id),
                search_clause(session, q, include_cluster=True),
            )
        if not active_lookup:
            matching_clusters = matching_clusters.where(
                unfiltered_content_clause(ContentItem.id)
            )
        stmt = stmt.where(Cluster.id.in_(matching_clusters))
    if cursor is not None:
        id_after_cursor = Cluster.id > cursor.id if order == "asc" else Cluster.id < cursor.id
        if cursor.first_seen_at is None:
            cursor_clause = and_(Cluster.first_seen_at.is_(None), id_after_cursor)
        else:
            time_after_cursor = Cluster.first_seen_at > cursor.first_seen_at if order == "asc" else Cluster.first_seen_at < cursor.first_seen_at
            cursor_clause = or_(
                time_after_cursor,
                and_(Cluster.first_seen_at == cursor.first_seen_at, id_after_cursor),
                Cluster.first_seen_at.is_(None),
            )
        stmt = stmt.where(cursor_clause)
    order_by = (Cluster.first_seen_at.asc().nullslast(), Cluster.id.asc()) if order == "asc" else (Cluster.first_seen_at.desc().nullslast(), Cluster.id.desc())
    stmt = stmt.group_by(Cluster.id).order_by(*order_by).limit(min(max(limit, 1), 200)).offset(max(offset, 0))
    rows = session.execute(stmt).all()
    preview_items = cluster_preview_items_for(
        session,
        [cluster.id for cluster, _count in rows],
        include_filtered=active_lookup,
    )
    title_translations = translation_texts_for(
        session, [cluster.generated_title for cluster, _count in rows]
    )
    event_identities = cluster_event_identities_for(
        session, [cluster.id for cluster, _count in rows]
    )
    synthesis_freshness = event_synthesis_freshness_for(
        session,
        {
            identity.event_uid: identity.current_revision_uid
            for identity in event_identities.values()
        },
    )
    results = [
        cluster_out(
            session,
            cluster,
            count,
            items=preview_items.get(cluster.id, []),
            cached_translations=title_translations,
            event_identity=event_identities.get(cluster.id),
            synthesis_freshness=(
                synthesis_freshness.get(event_identities[cluster.id].event_uid)
                if cluster.id in event_identities
                else None
            ),
        )
        for cluster, count in rows
    ]
    session.commit()
    return results


@app.get("/clusters/count")
def count_clusters(
    folder_id: int | None = None,
    source_id: int | None = None,
    q: str | None = None,
    read_status: str | None = None,
    read_later: bool | None = None,
    starred: bool | None = None,
    session: Session = Depends(get_session),
) -> dict[str, int]:
    (
        current_event_state,
        effective_read_status,
        effective_read_later,
        effective_starred,
    ) = cluster_effective_state_expressions(session)
    stmt = (
        select(func.count(func.distinct(Cluster.id)))
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(current_event_state, current_event_state.c.cluster_id == Cluster.id, isouter=True)
        .where(
            Source.status == "active",
            Source.media_type == "article",
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            ordinary_content_clause(session, ContentItem.id),
        )
    )
    active_lookup = source_id is not None or bool(q) or read_later is not None or starred is not None
    if not active_lookup:
        stmt = stmt.where(unfiltered_content_clause(ContentItem.id))
    if read_status:
        if read_status not in READ_STATUSES:
            raise HTTPException(status_code=400, detail="未知阅读状态")
        stmt = stmt.where(effective_read_status == read_status)
    elif read_later is None and starred is None:
        stmt = stmt.where(effective_read_status != "dismissed")
    if read_later is not None:
        stmt = stmt.where(effective_read_later.is_(read_later))
    if starred is not None:
        stmt = stmt.where(effective_starred.is_(starred))
    if folder_id is not None or source_id is not None or q:
        matching_clusters = (
            select(ClusterItem.cluster_id)
            .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
            .join(Source, Source.id == ContentItem.source_id)
            .join(Cluster, Cluster.id == ClusterItem.cluster_id)
            .where(Source.status == "active", Source.media_type == "article")
        )
        if folder_id is not None:
            matching_clusters = matching_clusters.where(Source.folder_id == folder_id)
        if source_id is not None:
            matching_clusters = matching_clusters.where(Source.id == source_id)
        if q and can_use_indexed_search(session, q):
            matching_clusters = indexed_cluster_search_query(
                session, q, folder_id, source_id
            )
        elif q:
            matching_clusters = matching_clusters.where(
                ordinary_content_clause(session, ContentItem.id),
                search_clause(session, q, include_cluster=True),
            )
        if not active_lookup:
            matching_clusters = matching_clusters.where(
                unfiltered_content_clause(ContentItem.id)
            )
        stmt = stmt.where(Cluster.id.in_(matching_clusters))
    return {"count": int(session.scalar(stmt) or 0)}


@app.get("/clusters/{cluster_id}", response_model=ClusterDetailOut)
def get_cluster(cluster_id: int, session: Session = Depends(get_session)) -> ClusterDetailOut:
    return cluster_detail_out(session, cluster_id)


def cluster_detail_out(
    session: Session,
    cluster_id: int,
) -> ClusterDetailOut:
    row = session.execute(
        select(Cluster, func.count(ClusterItem.id))
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(Cluster.id == cluster_id, Source.status == "active", Source.media_type == "article")
        .group_by(Cluster.id)
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="事件聚类不存在")
    event_identity = cluster_event_identities_for(session, [row[0].id]).get(row[0].id)
    cluster = cluster_out(
        session,
        row[0],
        row[1],
        event_identity=event_identity,
    )
    data = cluster.model_dump()
    if event_identity is not None:
        event_revision = session.execute(
            select(Event, EventRevision)
            .join(EventRevision, EventRevision.event_id == Event.id)
            .where(
                Event.uid == event_identity.event_uid,
                EventRevision.uid == event_identity.current_revision_uid,
            )
        ).one_or_none()
        if event_revision is None:
            raise SynthesisValidationError("Event synthesis Revision 归属不匹配")
        event, revision = event_revision
        source_view_rows = session.execute(
            select(EventEvidenceVersion, Source, UserState)
            .join(
                EventRevisionEvidence,
                EventRevisionEvidence.evidence_version_id
                == EventEvidenceVersion.id,
            )
            .join(Source, Source.id == EventEvidenceVersion.source_id)
            .outerjoin(
                UserState,
                (UserState.object_type == "item")
                & (
                    UserState.object_id
                    == EventEvidenceVersion.legacy_content_item_id_snapshot
                ),
            )
            .where(EventRevisionEvidence.revision_id == revision.id)
            .order_by(
                EventEvidenceVersion.published_at_snapshot.asc().nullslast(),
                EventEvidenceVersion.uid.asc(),
            )
        ).all()
        if not event_identity.uninterested:
            source_view_rows = [
                row for row in source_view_rows
                if row[2] is None or not row[2].uninterested
            ]
        synthesis = event_synthesis_state(session, event, revision)
        data["synthesis_freshness"] = EventSynthesisFreshnessOut(
            **synthesis.model_dump()
        ).model_dump()
        data["synthesis"] = synthesis.model_dump()
        data["source_view_evidence"] = [
            EventSourceViewEvidenceOut(
                evidence_version_uid=version.uid,
                source_id=version.source_id,
                legacy_content_item_id_snapshot=(
                    version.legacy_content_item_id_snapshot
                ),
            ).model_dump()
            for version, _source, _state in source_view_rows
        ]
        data["item_count"] = len(source_view_rows)
        source_view_filter_labels = active_filter_labels_for_items(
            session,
            [
                version.legacy_content_item_id_snapshot
                for version, _source, _state in source_view_rows
            ],
        )
        reading_documents = event_evidence_reading_documents(
            session, [version for version, _source, _state in source_view_rows]
        )
        data["items"] = [
            event_source_item_out(
                session,
                version,
                source,
                state,
                reading_document=reading_documents[version.id],
                filter_rules=source_view_filter_labels.get(
                    version.legacy_content_item_id_snapshot, []
                ),
            )
            for version, source, state in source_view_rows
        ]
        return ClusterDetailOut(**data)
    item_rows = session.execute(
        select(ContentItem, Source, UserState)
        .options(
            *content_item_document_options(include_reading_body=True)
        )
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(UserState, (UserState.object_type == "item") & (UserState.object_id == ContentItem.id), isouter=True)
        .where(ClusterItem.cluster_id == row[0].id, Source.status == "active", Source.media_type == "article")
        .order_by(ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
    ).all()
    item_filter_labels = active_filter_labels_for_items(
        session, [item.id for item, _source, _state in item_rows]
    )
    data["items"] = [
        item_out(
            session,
            item,
            source,
            state,
            filter_rules=item_filter_labels.get(item.id, []),
        )
        for item, source, state in item_rows
    ]
    return ClusterDetailOut(**data)


@app.post("/clusters/{cluster_id}/synthesize", response_model=ClusterDetailOut)
def synthesize_cluster(
    cluster_id: int,
    payload: SynthesisGenerateIn | None = None,
    session: Session = Depends(get_session),
) -> ClusterDetailOut:
    cluster = session.get(Cluster, cluster_id)
    if cluster is None:
        raise HTTPException(status_code=404, detail="事件聚类不存在")

    ai_settings = runtime_ai_settings(session)
    provider = requested_generation_provider(payload, ai_settings)
    if provider == "openai_compatible":
        try:
            validate_synthesis_remote_settings(ai_settings)
        except SynthesisSettingsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        prepared = prepare_cluster_synthesis(session, cluster_id)
    except GenerationProducerValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    request = producer_generation_request(
        session,
        prepared=prepared,
        provider=provider,
        ai_settings=ai_settings,
    )
    execute_synchronous_producer(
        session,
        request=request,
        prepared=prepared,
        provider=provider,
        ai_settings=ai_settings,
    )
    return cluster_detail_out(session, cluster_id)


@app.get("/topics", response_model=list[TopicGroupOut])
def list_topics(session: Session = Depends(get_session)) -> list[TopicGroupOut]:
    # ponytail: per-topic scan is enough for personal use; materialize links if topic count grows.
    return [
        topic_out(topic, topic_clusters(session, topic.query), topic_state(session, topic.id))
        for topic in session.scalars(select(TopicGroup).order_by(TopicGroup.updated_at.desc(), TopicGroup.id.desc())).all()
    ]


@app.post("/topics", response_model=TopicGroupDetailOut)
def create_topic(payload: TopicGroupCreate, session: Session = Depends(get_session)) -> TopicGroupDetailOut:
    name = payload.name.strip()
    query = payload.query.strip()
    if not name or not query:
        raise HTTPException(status_code=400, detail="主题名称和关键词不能为空")
    topic = session.scalar(select(TopicGroup).where(TopicGroup.name == name))
    if topic is None:
        topic = TopicGroup(name=name, query=query, description=payload.description.strip())
        session.add(topic)
    else:
        topic.query = query
        topic.description = payload.description.strip()
        topic.updated_at = now_utc()
    session.commit()
    session.refresh(topic)
    clusters = topic_clusters(session, topic.query)
    return TopicGroupDetailOut(**topic_out(topic, clusters, topic_state(session, topic.id)).model_dump(), clusters=clusters)


@app.get("/topics/{topic_id}", response_model=TopicGroupDetailOut)
def get_topic(topic_id: int, session: Session = Depends(get_session)) -> TopicGroupDetailOut:
    topic = session.get(TopicGroup, topic_id)
    if topic is None:
        raise HTTPException(status_code=404, detail="议题组不存在")
    clusters = topic_clusters(session, topic.query)
    return TopicGroupDetailOut(**topic_out(topic, clusters, topic_state(session, topic.id)).model_dump(), clusters=clusters)


@app.patch("/topics/{topic_id}", response_model=TopicGroupDetailOut)
def update_topic(topic_id: int, payload: TopicGroupPatch, session: Session = Depends(get_session)) -> TopicGroupDetailOut:
    topic = session.get(TopicGroup, topic_id)
    if topic is None:
        raise HTTPException(status_code=404, detail="议题组不存在")
    if payload.name is not None:
        name = payload.name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="主题名称不能为空")
        existing = session.scalar(select(TopicGroup).where(TopicGroup.name == name, TopicGroup.id != topic_id))
        if existing is not None:
            raise HTTPException(status_code=400, detail="主题名称已存在")
        topic.name = name
    if payload.query is not None:
        query = payload.query.strip()
        if not query:
            raise HTTPException(status_code=400, detail="关键词不能为空")
        topic.query = query
    if payload.description is not None:
        topic.description = payload.description.strip()
    topic.updated_at = now_utc()
    session.commit()
    return get_topic(topic_id, session)


@app.delete("/topics/{topic_id}", status_code=204)
def delete_topic(topic_id: int, session: Session = Depends(get_session)) -> Response:
    topic = session.get(TopicGroup, topic_id)
    if topic is None:
        raise HTTPException(status_code=404, detail="议题组不存在")
    session.delete(topic)
    session.commit()
    return Response(status_code=204)


@app.get("/reports", response_model=ReportOut)
def get_report(period: str = "day", date: str | None = None, session: Session = Depends(get_session)) -> dict[str, object]:
    start, end = report_bounds(period, date)
    state = report_user_state(session, period, start)
    generated = report_generation_view(session, period, start, end, state)
    if generated is not None:
        return generated
    task = latest_report_task(session, period, report_key(start), statuses={"complete"})
    if task is None:
        return report_empty(period, start, end, state)
    return report_snapshot(session, period, start, end, task, state)


@app.post("/reports/generate", response_model=ReportOut)
def generate_report(period: str = "day", date: str | None = None, session: Session = Depends(get_session)) -> dict[str, object]:
    start, end = report_bounds(period, date)
    clusters = report_clusters(session, start, end)
    if not clusters:
        raise HTTPException(status_code=400, detail="当前时间段没有可生成报告的事件聚类")
    ai_settings = runtime_ai_settings(session)
    provider = ai_settings.synthesis_provider
    if provider not in {"local", "openai_compatible"}:
        raise HTTPException(status_code=400, detail="不支持的报告生成服务")
    if provider == "openai_compatible":
        try:
            validate_synthesis_remote_settings(ai_settings)
        except SynthesisSettingsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
    model = (
        ai_settings.synthesis_remote_model
        if provider == "openai_compatible"
        else ai_settings.llm_model
    )
    report_source_ids, report_provenance_complete = report_generation_sources(
        session, clusters
    )
    source_policies: list[Source] = []
    source_policy_fingerprint: str | None = None
    if provider == "openai_compatible":
        source_policies, source_policy_fingerprint, privacy_reason = (
            external_generation_policy(
                session,
                report_source_ids,
                lock_sources=True,
                provenance_complete=report_provenance_complete,
            )
        )
        if privacy_reason:
            session.rollback()
            source_policies, source_policy_fingerprint, current_reason = (
                external_generation_policy(
                    session,
                    report_source_ids,
                    provenance_complete=report_provenance_complete,
                )
            )
            privacy_reason = current_reason or privacy_reason
            get_or_create_generation_request(
                session,
                task_type=f"report:{period}",
                reason="explicit-user-request",
                target_type="report",
                target_id=report_key(start),
                target_uid=f"report:{period}:{report_key(start)}",
                provider=provider,
                model=model,
                prompt_version=REPORT_PROMPT_VERSION,
                schema_version=REPORT_SCHEMA_VERSION,
                input_fingerprint=stable_hash(
                    {
                        "period": period,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "cluster_ids": [cluster.id for cluster in clusters],
                    }
                ),
                payload=None,
                privacy_status="blocked",
                privacy_reason=privacy_reason,
                source_policy_fingerprint=source_policy_fingerprint,
                source_policies=source_policies,
            )
            session.commit()
            raise HTTPException(status_code=403, detail=privacy_reason)
    try:
        input_data = freeze_report_input(
            session,
            period=period,
            start=start,
            end=end,
            clusters=clusters,
            source_ids=report_source_ids,
            provenance_complete=report_provenance_complete,
        )
    except ReportValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    request_payload = report_model_request(input_data)
    control = generation_control_out(session)
    unavailable_reason = (
        "生成任务已全局暂停"
        if control.global_pause
        else "每日 Token 预算尚未配置"
        if control.daily_budget_tokens is None
        else "今日 Token 预算不足"
        if control.remaining_tokens is not None
        and estimate_generation_payload(request_payload, control.input_estimator)
        + control.output_reserve_tokens
        > control.remaining_tokens
        else ""
    )
    if unavailable_reason:
        raise HTTPException(
            status_code=409,
            detail=unavailable_reason,
        )
    request, _created = get_or_create_generation_request(
        session,
        task_type=f"report:{period}",
        reason="explicit-user-request",
        target_type="report",
        target_id=report_key(start),
        target_uid=f"report:{period}:{report_key(start)}",
        provider=provider,
        model=model,
        prompt_version=REPORT_PROMPT_VERSION,
        schema_version=REPORT_SCHEMA_VERSION,
        input_fingerprint=str(input_data["input_fingerprint"]),
        payload=request_payload,
        privacy_status="eligible" if provider != "local" else "local",
        source_policy_fingerprint=source_policy_fingerprint,
        source_policies=source_policies,
    )
    existing = generation_result_for_request(session, request.id)
    if existing is not None:
        result, _application = existing
        try:
            request = reapply_report_result(session, request=request)
        except GenerationResultNotCurrentError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        except GenerationPrivacyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
        except GenerationApplicationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from None
        return report_generation_snapshot(
            session,
            period,
            start,
            end,
            request,
            result,
            report_user_state(session, period, start),
        )
    attempt = latest_attempt(session, request.id)
    if attempt is not None and attempt.status in {"pending", "running"}:
        session.commit()
        return report_generation_pending(
            period, start, end, request, report_user_state(session, period, start)
        )
    approve_generation_request(session, request, allow_consumed_reissue=True)
    attempt = start_admitted_generation_attempt(session, request)
    if attempt is None:
        session.commit()
        return report_generation_pending(
            period, start, end, request, report_user_state(session, period, start)
        )
    session.commit()
    attempt_id = attempt.id
    input_tokens: int | None = None
    output_tokens: int | None = None
    try:
        llm_provider = (
            synthesis_remote_provider(ai_settings)
            if provider == "openai_compatible"
            else local_llm(ai_settings)
        )
        provider_result = llm_provider.chat(
            model,
            str(request_payload["system_prompt"]),
            str(request_payload["input"]),
        )
        input_tokens, output_tokens = token_usage(provider_result)
        title, body = report_fields(provider_result)
        canonical_result = validate_report_output(
            {"title": title, "body": body}, input_data
        )
    except (ReportValidationError, RuntimeError) as exc:
        error = (
            str(exc)
            if isinstance(exc, ReportValidationError)
            else (
                "远端生成服务不可用，请检查设置"
                if provider == "openai_compatible"
                else "本地模型服务未连接，请检查 LM Studio"
            )
        )
        failed_attempt = session.scalar(
            select(GenerationAttempt)
            .where(GenerationAttempt.id == attempt_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert failed_attempt is not None
        fail_generation_attempt(
            session,
            failed_attempt,
            error,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        session.commit()
        raise HTTPException(status_code=502, detail=error) from None
    lock_generation_lifecycle(session)
    completed_attempt = session.scalar(
        select(GenerationAttempt)
        .where(GenerationAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    assert completed_attempt is not None
    if provider == "openai_compatible":
        assert source_policy_fingerprint is not None
        _sources, current_fingerprint, privacy_reason = external_generation_policy(
            session,
            report_source_ids,
            lock_sources=True,
            provenance_complete=report_provenance_complete,
        )
        if privacy_reason or current_fingerprint != source_policy_fingerprint:
            error = privacy_reason or "来源外部生成策略已变更，请重新批准此任务"
            fail_generation_attempt(
                session,
                completed_attempt,
                error,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
            session.commit()
            raise HTTPException(status_code=409, detail=error)
    result, _application = complete_generation_attempt(
        session,
        attempt=completed_attempt,
        payload=canonical_result,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    session.commit()
    request = session.get(GenerationRequest, request.id)
    assert request is not None
    try:
        request = reapply_report_result(
            session, request=request, pending_only=True
        )
    except GenerationResultNotCurrentError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except GenerationPrivacyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except GenerationApplicationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from None
    return report_generation_snapshot(
        session,
        period,
        start,
        end,
        request,
        result,
        report_user_state(session, period, start),
    )


@app.get("/ai/settings", response_model=AISettingsOut)
def ai_settings(session: Session = Depends(get_session)) -> AISettingsOut:
    ai_settings = runtime_ai_settings(session)
    return AISettingsOut(
        task_provider=ai_settings.task_provider,
        synthesis_provider=ai_settings.synthesis_provider,
        base_url=ai_settings.llm_base_url,
        translation_provider=ai_settings.translation_provider,
        translation_base_url=ai_settings.translation_base_url,
        translation_local_base_url=ai_settings.translation_local_base_url,
        translation_local_model=ai_settings.translation_local_model,
        translation_cloud_base_url=ai_settings.translation_cloud_base_url,
        translation_cloud_model=ai_settings.translation_cloud_model,
        translation_api_key_configured=bool(ai_settings.translation_api_key),
        embedding_base_url=ai_settings.embedding_base_url,
        endpoint=f"{ai_settings.llm_base_url}/api/v1/chat",
        translation_endpoint=translation_endpoint(ai_settings),
        embedding_endpoint=embedding_endpoint_for(ai_settings.embedding_base_url),
        llm_model=ai_settings.llm_model,
        translation_model=ai_settings.translation_model,
        embedding_model=ai_settings.embedding_model,
        timeout_seconds=ai_settings.timeout_seconds,
        synthesis_remote_base_url=ai_settings.synthesis_remote_base_url,
        synthesis_remote_model=ai_settings.synthesis_remote_model,
        synthesis_remote_api_key_configured=bool(ai_settings.synthesis_remote_api_key),
        synthesis_remote_endpoint=(
            openai_chat_endpoint_for(ai_settings.synthesis_remote_base_url)
            if ai_settings.synthesis_remote_base_url
            else ""
        ),
    )


@app.patch("/ai/settings", response_model=AISettingsOut)
def update_ai_settings(payload: AISettingsPatch, session: Session = Depends(get_session)) -> AISettingsOut:
    values = payload.model_dump(exclude_none=True)
    for field in (
        "llm_model",
        "translation_model",
        "embedding_model",
        "synthesis_remote_model",
    ):
        if field in values and len(str(values[field]).strip()) > AI_MODEL_NAME_MAX_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"{field} 不能超过 {AI_MODEL_NAME_MAX_LENGTH} 个字符",
            )
    clear_translation_api_key = bool(values.pop("clear_translation_api_key", False))
    clear_synthesis_remote_api_key = bool(
        values.pop("clear_synthesis_remote_api_key", False)
    )
    changes = {key: value for key, value in values.items() if str(value).strip()}
    if not changes:
        if not clear_translation_api_key and not clear_synthesis_remote_api_key:
            return ai_settings(session)
    if "task_provider" in changes:
        normalized_task_provider = str(changes["task_provider"]).strip().lower()
        if normalized_task_provider not in {"local", "openai_compatible"}:
            raise HTTPException(
                status_code=400,
                detail="生成任务只支持本地模型或远端兼容接口",
            )
        changes["task_provider"] = normalized_task_provider
    if "synthesis_provider" in changes:
        normalized_synthesis_provider = str(
            changes["synthesis_provider"]
        ).strip().lower()
        if normalized_synthesis_provider not in {
            "local",
            "openai_compatible",
        }:
            raise HTTPException(
                status_code=400,
                detail="合成稿服务只支持本地模型或远端兼容接口",
            )
        changes["synthesis_provider"] = normalized_synthesis_provider
    current = runtime_ai_settings(session)
    try:
        changes = prepare_translation_settings_changes(current, changes, clear_api_key=clear_translation_api_key)
    except TranslationSettingsError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    for key in ("base_url", "translation_base_url", "embedding_base_url"):
        if key in changes and not str(changes[key]).startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="模型地址必须以 http:// 或 https:// 开头")
        if key in changes and not valid_model_base_url(str(changes[key])):
            raise HTTPException(
                status_code=400,
                detail="模型地址必须是有效且不含用户名、密码、查询参数或片段的 URL",
            )
    if "timeout_seconds" in changes and float(changes["timeout_seconds"]) <= 0:
        raise HTTPException(status_code=400, detail="超时时间必须大于 0")
    if clear_synthesis_remote_api_key:
        changes["synthesis_remote_api_key"] = ""
    candidate = replace(
        current,
        task_provider=str(changes.get("task_provider", current.task_provider)),
        synthesis_provider=str(
            changes.get("synthesis_provider", current.synthesis_provider)
        ),
        synthesis_remote_base_url=str(
            changes.get("synthesis_remote_base_url", current.synthesis_remote_base_url)
        ).rstrip("/"),
        synthesis_remote_model=str(
            changes.get("synthesis_remote_model", current.synthesis_remote_model)
        ),
        synthesis_remote_api_key=str(
            changes.get("synthesis_remote_api_key", current.synthesis_remote_api_key)
        ),
    )
    synthesis_fields_changed = any(
        (
            candidate.synthesis_remote_base_url
            != current.synthesis_remote_base_url,
            candidate.synthesis_remote_model != current.synthesis_remote_model,
            candidate.synthesis_remote_api_key
            != current.synthesis_remote_api_key,
        )
    )
    if (
        candidate.synthesis_provider == "openai_compatible"
        or synthesis_fields_changed
    ):
        try:
            validate_synthesis_remote_settings(
                candidate,
                require_api_key=(
                    candidate.synthesis_provider == "openai_compatible"
                    and not clear_synthesis_remote_api_key
                ),
            )
        except SynthesisSettingsError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
    save_ai_settings(session, changes)
    session.commit()
    return ai_settings(session)


@app.get("/assistant", response_model=AssistantOut)
def assistant(
    q: str,
    folder_id: int | None = None,
    source_id: int | None = None,
    item_id: int | None = None,
    cluster_id: int | None = None,
    operation_id: str | None = None,
    session: Session = Depends(get_session),
) -> AssistantOut:
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="问题不能为空")
    if item_id is not None and cluster_id is not None:
        raise HTTPException(status_code=400, detail="只能选择一个 Assistant 上下文")
    if item_id is not None:
        if not operation_id:
            raise HTTPException(
                status_code=400,
                detail="条目 Assistant 操作必须提交 operation_id",
            )
        items = query_items(session, item_id=item_id, limit=1)
        if not items:
            raise HTTPException(status_code=404, detail="条目不存在")
    elif cluster_id is not None:
        if session.get(Cluster, cluster_id) is None:
            raise HTTPException(status_code=404, detail="事件聚类不存在")
        items = query_cluster_items(session, cluster_id=cluster_id, limit=8)
    else:
        items = query_items(session, folder_id=folder_id, source_id=source_id, q=query, limit=8)
    citations = [assistant_citation(item) for item in items]
    ai_settings = runtime_ai_settings(session)
    if not items:
        return AssistantOut(query=query, answer="没有找到可引用的相关文章。", model=ai_settings.llm_model, citations=[])
    context = "\n\n".join(
        f"[{index}] 来源：{item.source_name}\n标题：{item.title}\n时间：{item.published_at or '未知'}\nRSS正文：{(item.content_text or '').strip()[:1800]}"
        for index, item in enumerate(items, 1)
    )
    try:
        result = local_llm(ai_settings).chat(
            ai_settings.llm_model,
            "你是个人信息阅读器助手。只根据给定 RSS 条目回答，用中文，保留 [1] 这类引用编号。只输出 JSON：{\"answer\":\"回答\"}。",
            f"用户问题：{query}\n\n可引用条目：\n{context}",
        )
    except RuntimeError:
        raise HTTPException(
            status_code=502,
            detail="本地模型服务未连接，请检查 LM Studio",
        ) from None
    answer = assistant_answer(result)
    if not answer:
        raise HTTPException(status_code=502, detail="LLM 未返回可用回答")
    if item_id is not None:
        assert operation_id is not None
        try:
            apply_object_user_state_mutation(
                session,
                "item",
                item_id,
                UserStatePatch(
                    operation_id=operation_id,
                    read_status="summary_seen",
                ),
            )
            session.commit()
        except Exception:
            session.rollback()
            raise
    return AssistantOut(query=query, answer=answer, model=ai_settings.llm_model, citations=citations)


@app.post("/ai/chat", response_model=AIChatOut)
def ai_chat(payload: AIChatIn, session: Session = Depends(get_session)) -> AIChatOut:
    if payload.model_type not in {"llm", "embedding", "translation"}:
        raise HTTPException(status_code=400, detail="模型类型只支持 llm、embedding 或 translation")
    ai_settings = runtime_ai_settings(session)
    model = ai_settings.embedding_model if payload.model_type == "embedding" else ai_settings.translation_model if payload.model_type == "translation" else ai_settings.llm_model
    provider_name = "local"
    endpoint = f"{ai_settings.llm_base_url}/api/v1/chat"
    try:
        if payload.model_type == "embedding":
            endpoint = embedding_endpoint_for(ai_settings.embedding_base_url)
            result = LocalEmbeddingProvider(ai_settings.embedding_base_url, ai_settings.timeout_seconds, settings.embedding_api_key).embed(model, payload.input)
        elif payload.model_type == "translation":
            provider_name = ai_settings.translation_provider
            endpoint = translation_endpoint(ai_settings)
            result = translation_chat_provider(ai_settings).chat(model, payload.system_prompt, payload.input)
        else:
            result = local_llm(ai_settings).chat(model, payload.system_prompt, payload.input)
    except RuntimeError:
        detail = (
            "云端翻译服务不可用，请检查地址、模型和密钥"
            if payload.model_type == "translation"
            and ai_settings.translation_provider == "openai_compatible"
            else "本地模型服务未连接，请检查 LM Studio"
        )
        raise HTTPException(status_code=502, detail=detail) from None
    return AIChatOut(model=model, provider=provider_name, endpoint=endpoint, result=result)


@app.post("/translations", response_model=TranslationOut)
def translate_reading(payload: TranslationIn, session: Session = Depends(get_session)) -> TranslationOut:
    source = session.get(Source, payload.source_id) if payload.source_id else None
    if payload.source_id and (source is None or source.status == "deleted"):
        raise HTTPException(status_code=404, detail="来源不存在")
    ai_settings = translation_settings_for_source(runtime_ai_settings(session), source)
    if payload.blocks:
        blocks = [
            {
                "id": block.id,
                "text": translation_utils.strip_translation_urls(block.text),
            }
            for block in payload.blocks
        ]
        blocks = [block for block in blocks if block["text"]]
        if not blocks:
            return TranslationOut(status="empty")
        combined_text = "\n\n".join(block["text"] for block in blocks)
        if not translation_utils.needs_reading_translation(combined_text):
            return TranslationOut(
                status="skipped", model_version=ai_settings.translation_model
            )
        source_hash = translation_utils.block_translation_source_hash(blocks)
        expected_ids = [block["id"] for block in blocks]
        cached = translation_utils.latest_block_translation_task(
            session,
            ai_settings.translation_provider,
            ai_settings.translation_model,
            source_hash,
            expected_ids,
        )
        if cached is not None:
            return translation_snapshot(cached)
        task = translation_utils.ensure_block_translation(
            session,
            translation_chat_provider(ai_settings),
            ai_settings.translation_model,
            blocks,
        )
        if task is None:
            raise HTTPException(status_code=502, detail="翻译块映射无效")
        session.commit()
        session.refresh(task)
        return translation_snapshot(task)
    text = payload.text.strip()
    if not text:
        return TranslationOut(status="empty")
    if not translation_utils.needs_reading_translation(text):
        return TranslationOut(status="skipped", model_version=ai_settings.translation_model)
    source_hash = content_hash(text)
    cached = translation_utils.latest_translation_task(session, ai_settings.translation_provider, ai_settings.translation_model, source_hash)
    if cached is not None:
        return translation_snapshot(cached)
    translation = translation_utils.ensure_translation(
        session,
        translation_chat_provider(ai_settings),
        ai_settings.translation_model,
        text,
    )
    if not translation:
        raise HTTPException(status_code=502, detail="翻译模型未返回可用译文")
    session.commit()
    cached = translation_utils.latest_translation_task(session, ai_settings.translation_provider, ai_settings.translation_model, source_hash)
    if cached is None:
        raise HTTPException(status_code=502, detail="翻译缓存写入失败")
    return translation_snapshot(cached)


def filter_item_outputs(
    session: Session, items: list[ContentItem], *, include_content: bool = False
) -> list[ItemOut]:
    if not items:
        return []
    item_ids = [item.id for item in items]
    states = {
        state.object_id: state
        for state in session.scalars(
            select(UserState).where(
                UserState.object_type == "item", UserState.object_id.in_(item_ids)
            )
        ).all()
    }
    labels = active_filter_labels_for_items(session, item_ids)
    return [
        item_out(
            session,
            item,
            item.source,
            states.get(item.id),
            include_content=include_content,
            filter_rules=labels.get(item.id, []),
        )
        for item in items
    ]


def content_item_document_options(
    *, include_reading_body: bool
) -> tuple[Load, ...]:
    options = [
        joinedload(ContentItem.document).joinedload(Document.raw_entry)
    ]
    if include_reading_body:
        options.append(
            joinedload(ContentItem.document)
            .undefer(Document.reading_html)
            .undefer(Document.body_source)
            .undefer(Document.web_fetch_status)
        )
    return tuple(options)


def query_filtered_items(
    session: Session,
    folder_id: int | None = None,
    source_id: int | None = None,
    media_type: str | None = None,
    q: str | None = None,
    limit: int = 80,
    offset: int = 0,
    include_content: bool = False,
) -> FilteredItemsOut:
    statement = (
        select(ContentItem, Source, UserState, func.count().over().label("total_count"))
        .options(
            *content_item_document_options(
                include_reading_body=include_content
            )
        )
        .join(Source, Source.id == ContentItem.source_id)
        .join(Folder, Folder.id == Source.folder_id, isouter=True)
        .join(
            UserState,
            (UserState.object_type == "item")
            & (UserState.object_id == ContentItem.id),
            isouter=True,
        )
        .where(active_filter_match_exists(ContentItem.id))
    )
    if source_id is not None:
        statement = statement.where(
            Source.id == source_id,
            Source.status != "deleted",
        )
    else:
        statement = statement.where(Source.status == "active")
    if folder_id is not None:
        statement = statement.where(Source.folder_id == folder_id)
    if media_type:
        if media_type not in SOURCE_MEDIA_TYPES:
            raise HTTPException(status_code=400, detail="不支持的媒体类型")
        statement = statement.where(source_media_filter(media_type))
    if q:
        if can_use_indexed_search(session, q):
            statement = statement.where(ContentItem.id.in_(indexed_item_search_query(q)))
        else:
            statement = statement.where(search_clause(session, q))
    rows = session.execute(
        statement.order_by(
            ContentItem.published_at.desc().nullslast(), ContentItem.id.desc()
        )
        .limit(min(max(limit, 1), 200))
        .offset(max(offset, 0))
    ).all()
    if not rows:
        total = (
            int(
                session.scalar(
                    select(func.count()).select_from(
                        statement.with_only_columns(ContentItem.id)
                        .order_by(None)
                        .subquery()
                    )
                )
                or 0
            )
            if offset > 0
            else 0
        )
        return FilteredItemsOut(count=total, items=[])
    total = int(rows[0][3])
    item_ids = [item.id for item, _source, _state, _total in rows]
    labels = active_filter_labels_for_items(session, item_ids)
    feedback = uninterested_feedback_for_items(session, item_ids)
    # ponytail: filtered audit lists use source text; explicit detail loads cached translations.
    items = [
        item_out(
            session,
            item,
            source,
            state,
            include_content=include_content,
            cached_translations={},
            filter_rules=labels.get(item.id, []),
            uninterested_feedback=feedback.get(item.id),
        )
        for item, source, state, _total in rows
    ]
    return FilteredItemsOut(count=total, items=items)


def query_items(
    session: Session,
    folder_id: int | None = None,
    source_id: int | None = None,
    media_type: str | None = None,
    q: str | None = None,
    read_status: str | None = None,
    read_later: bool | None = None,
    starred: bool | None = None,
    item_id: int | None = None,
    limit: int = 80,
    offset: int = 0,
    include_content: bool = True,
    ensure_title_translation: bool = False,
    ensure_translations: bool = False,
    ensure_content_translation: bool = False,
) -> list[ItemOut]:
    stmt = (
        select(ContentItem, Source, UserState)
        .options(
            *content_item_document_options(
                include_reading_body=include_content
            )
        )
        .join(Source, Source.id == ContentItem.source_id)
        .join(Folder, Folder.id == Source.folder_id, isouter=True)
        .join(
            UserState,
            (UserState.object_type == "item") & (UserState.object_id == ContentItem.id),
            isouter=True,
        )
        .where(Source.status != DELETED_SOURCE_STATUS)
    )
    if item_id is not None:
        stmt = stmt.where(ContentItem.id == item_id)
    else:
        stmt = stmt.where(ordinary_content_clause(session, ContentItem.id))
    if folder_id is not None:
        stmt = stmt.where(Source.folder_id == folder_id)
    if source_id is not None:
        stmt = stmt.where(Source.id == source_id)
    elif item_id is None:
        stmt = stmt.where(Source.status == "active")
        if not q and read_later is None and starred is None:
            stmt = stmt.where(unfiltered_content_clause(ContentItem.id))
    if media_type:
        if media_type not in SOURCE_MEDIA_TYPES:
            raise HTTPException(status_code=400, detail="不支持的媒体类型")
        stmt = stmt.where(source_media_filter(media_type))
    if q:
        if can_use_indexed_search(session, q):
            stmt = stmt.where(ContentItem.id.in_(indexed_item_search_query(q)))
        else:
            stmt = stmt.where(search_clause(session, q))
    if read_status:
        if read_status not in READ_STATUSES:
            raise HTTPException(status_code=400, detail="不支持的阅读状态")
        if read_status == "unread":
            stmt = stmt.where(or_(UserState.id.is_(None), UserState.read_status == "unread"))
        else:
            stmt = stmt.where(UserState.read_status == read_status)
    elif read_later is None and starred is None and item_id is None:
        stmt = stmt.where(or_(UserState.id.is_(None), UserState.read_status != "dismissed"))
    if read_later is not None:
        stmt = stmt.where(UserState.read_later.is_(True) if read_later else or_(UserState.id.is_(None), UserState.read_later.is_(False)))
    if starred is not None:
        stmt = stmt.where(UserState.starred.is_(True) if starred else or_(UserState.id.is_(None), UserState.starred.is_(False)))
    stmt = stmt.order_by(ContentItem.published_at.desc().nullslast(), ContentItem.created_at.desc()).limit(min(max(limit, 1), 200)).offset(max(offset, 0))
    rows = session.execute(stmt).all()
    cached_translations = None
    if not ensure_title_translation and not ensure_translations:
        cached_translations = translation_texts_for(
            session,
            [
                text
                for item, _source, _state in rows
                for text in (
                    clean_title(item.title, clean_title(item.summary or item.content_text[:220])),
                    clean_preview(item.summary or item.content_text[:500], 500),
                    item.content_text if include_content else "",
                )
            ],
        )
    labels = (
        active_filter_labels_for_items(
            session, [item.id for item, _source, _state in rows]
        )
        if item_id is not None
        or source_id is not None
        or bool(q)
        or read_later is not None
        or starred is not None
        else {}
    )
    feedback = (
        uninterested_feedback_for_items(
            session, [item.id for item, _source, _state in rows]
        )
        if item_id is not None
        else {}
    )
    result = [
        item_out(
            session,
            item,
            source,
            state,
            include_content=include_content,
            ensure_title_translation=ensure_title_translation,
            ensure_translations=ensure_translations,
            ensure_content_translation=ensure_content_translation,
            cached_translations=cached_translations,
            filter_rules=labels.get(item.id, []),
            uninterested_feedback=feedback.get(item.id),
        )
        for item, source, state in rows
    ]
    if ensure_title_translation or ensure_translations:
        session.commit()
    return result

def query_cluster_items(session: Session, cluster_id: int, limit: int = 8) -> list[ItemOut]:
    if session.get(Cluster, cluster_id) is None:
        raise HTTPException(status_code=404, detail="事件聚类不存在")
    stmt = (
        select(ContentItem, Source, UserState)
        .options(
            *content_item_document_options(include_reading_body=True)
        )
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(UserState, (UserState.object_type == "item") & (UserState.object_id == ContentItem.id), isouter=True)
        .where(
            ClusterItem.cluster_id == cluster_id,
            Source.status == "active",
            Source.media_type == "article",
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .order_by(ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
        .limit(min(limit, 200))
    )
    rows = session.execute(stmt).all()
    labels = active_filter_labels_for_items(
        session, [item.id for item, _source, _state in rows]
    )
    return [
        item_out(
            session,
            item,
            source,
            state,
            filter_rules=labels.get(item.id, []),
        )
        for item, source, state in rows
    ]


def browse_media_summaries(session: Session) -> list[BrowseMediaSummaryOut]:
    source_rows = session.execute(
        select(Source, Folder.name)
        .join(Folder, Folder.id == Source.folder_id, isouter=True)
        .where(Source.status == "active")
        .order_by(Source.name.asc(), Source.id.asc())
    ).all()
    count_rows = session.execute(
        select(
            Source.id,
            func.count(ContentItem.id),
            func.sum(case((or_(UserState.id.is_(None), UserState.read_status == "unread"), 1), else_=0)),
        )
        .join(Folder, Folder.id == Source.folder_id, isouter=True)
        .join(ContentItem, ContentItem.source_id == Source.id)
        .join(UserState, (UserState.object_type == "item") & (UserState.object_id == ContentItem.id), isouter=True)
        .where(
            Source.status == "active",
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .group_by(Source.id)
    ).all()
    counts = {source_id: (int(total or 0), int(unread or 0)) for source_id, total, unread in count_rows}
    grouped: dict[str, list[BrowseSourceSummaryOut]] = {media_type: [] for media_type in SOURCE_MEDIA_TYPE_ORDER}
    for source, _folder_name in source_rows:
        media_type = source.media_type
        grouped[media_type].append(BrowseSourceSummaryOut(
            source_id=source.id,
            folder_id=source.folder_id,
            name=source.name,
            media_type=media_type,
            total_count=counts.get(source.id, (0, 0))[0],
            unread_count=counts.get(source.id, (0, 0))[1],
        ))
    return [
        BrowseMediaSummaryOut(
            media_type=media_type,
            total_count=sum(source.total_count for source in grouped[media_type]),
            unread_count=sum(source.unread_count for source in grouped[media_type]),
            sources=grouped[media_type],
        )
        for media_type in SOURCE_MEDIA_TYPE_ORDER
    ]


def bulk_unread_ids(session: Session, payload: BulkReadPrepareIn) -> list[int]:
    if payload.object_type == "item":
        stmt = select(ContentItem.id).join(Source, Source.id == ContentItem.source_id).join(Folder, Folder.id == Source.folder_id, isouter=True).join(
            UserState,
            (UserState.object_type == "item") & (UserState.object_id == ContentItem.id),
            isouter=True,
        )
        stmt = stmt.where(
            Source.status == "active",
            or_(UserState.id.is_(None), UserState.read_status == "unread"),
            ordinary_content_clause(session, ContentItem.id),
        )
        if payload.source_id is None and not payload.q:
            stmt = stmt.where(unfiltered_content_clause(ContentItem.id))
        if payload.folder_id is not None:
            stmt = stmt.where(Source.folder_id == payload.folder_id)
        if payload.source_id is not None:
            stmt = stmt.where(Source.id == payload.source_id)
        if payload.media_type:
            if payload.media_type not in SOURCE_MEDIA_TYPES:
                raise HTTPException(status_code=400, detail="不支持的媒体类型")
            stmt = stmt.where(source_media_filter(payload.media_type))
        if payload.q:
            stmt = stmt.where(search_clause(session, payload.q))
        return list(session.scalars(stmt).all())

    current_event_state, effective_read_status, _, _ = (
        cluster_effective_state_expressions(session)
    )
    matching = (
        select(current_event_state.c.event_id)
        .select_from(Cluster)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(current_event_state, current_event_state.c.cluster_id == Cluster.id)
        .where(
            Source.status == "active",
            Source.media_type == "article",
            effective_read_status == "unread",
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
            ordinary_content_clause(session, ContentItem.id),
        )
    )
    if payload.source_id is None and not payload.q:
        matching = matching.where(unfiltered_content_clause(ContentItem.id))
    if payload.folder_id is not None:
        matching = matching.where(Source.folder_id == payload.folder_id)
    if payload.source_id is not None:
        matching = matching.where(Source.id == payload.source_id)
    if payload.q:
        matching = matching.where(search_clause(session, payload.q, include_cluster=True))
    return list(session.scalars(matching.distinct()).all())


def item_out(
    session: Session,
    item: ContentItem,
    source: Source,
    state: UserState | None,
    include_content: bool = True,
    ensure_title_translation: bool = False,
    ensure_translations: bool = False,
    ensure_content_translation: bool = False,
    cached_translations: dict[str, str] | None = None,
    filter_rules: list[str] | None = None,
    uninterested_feedback: UninterestedFeedback | None = None,
) -> ItemOut:
    title = clean_title(item.title, clean_title(item.summary or item.content_text[:220]))
    summary = clean_preview(item.summary or item.content_text[:500], 500)
    content = item.content_text if include_content else ""
    if ensure_translations:
        values = [title, summary]
        if ensure_content_translation:
            values.append(content)
        ensure_reading_translations(session, values, source)
    elif ensure_title_translation:
        ensure_reading_translations(session, [title], source)
    return ItemOut(
        id=item.id,
        source_id=source.id,
        source_name=source.name,
        source_site_url=source.site_url or "",
        title=title,
        title_translation=(cached_translations.get(title, "") if cached_translations is not None else translation_text_for(session, title, source)),
        summary=summary,
        summary_translation=(cached_translations.get(summary, "") if cached_translations is not None else translation_text_for(session, summary, source)),
        image_url=first_markdown_image(content) or item_image_url(item, source),
        media_url=item.media_url or "",
        media_kind=item.media_kind or "",
        media_duration=item.media_duration or 0,
        content_text=content,
        reading_html=item.document.reading_html if include_content else None,
        body_source=item.document.body_source if include_content else None,
        web_fetch_status=(
            item.document.web_fetch_status if include_content else None
        ),
        content_translation=(cached_translations.get(content, "") if cached_translations is not None else translation_text_for(session, content, source)) if content else "",
        reading_translation_needed=translation_utils.needs_reading_translation(content or summary or title),
        url=item.url,
        published_at=item.published_at,
        read_status=state.read_status if state else "unread",
        read_later=state.read_later if state else False,
        starred=state.starred if state else False,
        filtered=bool(filter_rules),
        filter_rules=filter_rules or [],
        uninterested=uninterested_feedback is not None,
        uninterested_reason=(
            uninterested_feedback.reason if uninterested_feedback else None
        ),
        uninterested_note=(
            uninterested_feedback.note if uninterested_feedback else None
        ),
        uninterested_at=(
            uninterested_feedback.marked_at if uninterested_feedback else None
        ),
    )


def event_source_item_out(
    session: Session,
    version: EventEvidenceVersion,
    source: Source,
    state: UserState | None,
    *,
    reading_document: Document | None,
    ensure_translations: bool = False,
    ensure_content_translation: bool = False,
    filter_rules: list[str] | None = None,
) -> ItemOut:
    title = clean_title(version.title_snapshot, version.content_snapshot[:220])
    summary = clean_preview(version.content_snapshot[:500], 500)
    content = version.content_snapshot
    if ensure_translations:
        values = [title, summary]
        if ensure_content_translation:
            values.append(content)
        ensure_reading_translations(session, values, source)
    return ItemOut(
        id=version.legacy_content_item_id_snapshot,
        source_id=source.id,
        source_name=source.name,
        source_site_url=source.site_url or "",
        title=title,
        title_translation=translation_text_for(session, title, source),
        summary=summary,
        summary_translation=translation_text_for(session, summary, source),
        image_url=first_markdown_image(content),
        content_text=content,
        reading_html=(
            reading_document.reading_html
            if reading_document is not None
            else None
        ),
        body_source=(
            reading_document.body_source
            if reading_document is not None
            else None
        ),
        web_fetch_status=(
            reading_document.web_fetch_status
            if reading_document is not None
            else None
        ),
        content_translation=(
            translation_text_for(session, content, source) if content else ""
        ),
        reading_translation_needed=translation_utils.needs_reading_translation(content or summary or title),
        url=version.url_snapshot,
        published_at=version.published_at_snapshot,
        read_status=state.read_status if state else "unread",
        read_later=state.read_later if state else False,
        starred=state.starred if state else False,
        filtered=bool(filter_rules),
        filter_rules=filter_rules or [],
    )


def translation_text_for(
    session: Session, text: str, source: Source | None = None
) -> str:
    if not text or not translation_utils.needs_reading_translation(text):
        return ""
    ai_settings = translation_settings_for_source(runtime_ai_settings(session), source)
    return translation_utils.cached_translation_text(session, ai_settings.translation_model, text, ai_settings.translation_provider)


def translation_texts_for(session: Session, texts: list[str]) -> dict[str, str]:
    values = [text for text in texts if text and translation_utils.needs_reading_translation(text)]
    if not values:
        return {}
    ai_settings = translation_settings_for_source(runtime_ai_settings(session), None)
    return translation_utils.cached_translation_texts(
        session,
        ai_settings.translation_model,
        values,
        ai_settings.translation_provider,
    )


def ensure_reading_translations(
    session: Session, values: list[str], source: Source | None = None
) -> None:
    provider = None
    ai_settings = translation_settings_for_source(runtime_ai_settings(session), source)
    for value in values:
        text_value = value.strip()
        if not text_value or not translation_utils.needs_reading_translation(text_value):
            continue
        if translation_utils.cached_translation_text(session, ai_settings.translation_model, text_value, ai_settings.translation_provider):
            continue
        if provider is None:
            provider = translation_chat_provider(ai_settings)
        translation_utils.ensure_translation(session, provider, ai_settings.translation_model, text_value)


def short_item_raw_text(item: ContentItem) -> str:
    raw = item.document.raw_entry if item.document is not None else None
    return strip_html((raw.raw_content or raw.raw_summary) if raw is not None else "", item.url)


def item_image_url(item: ContentItem, source: Source) -> str:
    return first_markdown_image(item.content_text) or first_markdown_image(short_item_raw_text(item)) or ((item.media_url or "") if item.media_kind == "image" else "") or video_thumbnail_url(item, source)


def video_thumbnail_url(item: ContentItem, source: Source) -> str:
    if source.media_type != "video" and item.media_kind != "video":
        return ""
    for url in (item.url, item.media_url):
        watch_url = youtube_watch_url(url or "")
        if not watch_url:
            continue
        video_id = parse_qs(urlsplit(watch_url).query).get("v", [""])[0]
        if video_id:
            return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    return ""


def first_markdown_image(text_value: str) -> str:
    return first_markdown_image_url(text_value)


def source_out(
    source: Source,
    unread_count: int,
    metric: FeedMetric | None = None,
    cluster_count: int = 0,
    duplicate_count: int = 0,
    folder_unread_count: int = 0,
    all_unread_count: int = 0,
    recent_entry_count_30d: int = 0,
    *,
    include_metrics: bool = True,
) -> SourceOut:
    return SourceOut(
        id=source.id,
        folder_id=source.folder_id,
        name=source.name,
        url=source.url,
        site_url=source.site_url,
        media_type=source.media_type,
        status=source.status,
        enabled=source.enabled,
        fetch_full_content=source.fetch_full_content,
        article_selector=source.article_selector,
        remove_selector=source.remove_selector,
        privacy_class=source.privacy_class,
        external_generation_allowed=source.external_generation_allowed,
        unread_count=unread_count,
        folder_unread_count=folder_unread_count,
        all_unread_count=all_unread_count,
        feed_trust_score=feed_trust_score(metric, cluster_count, duplicate_count) if metric and include_metrics else source.feed_trust_score,
        fetched_count=metric.fetched_count if metric else 0,
        read_count=metric.read_count if metric else 0,
        opened_count=metric.opened_count if metric else 0,
        starred_count=metric.starred_count if metric else 0,
        read_later_count=metric.read_later_count if metric else 0,
        cluster_count=cluster_count,
        duplicate_count=duplicate_count,
        recent_entry_count_30d=recent_entry_count_30d,
        last_fetched_at=source.last_fetched_at,
        last_error=source.last_error,
        status_changed_at=source.status_changed_at,
    )


def cluster_out(
    session: Session,
    cluster: Cluster,
    item_count: int,
    items: list[ItemOut] | None = None,
    ensure_translations: bool = False,
    event_identity: ClusterEventIdentity | None = None,
    synthesis_freshness: EventSynthesisFreshnessOut | None = None,
    cached_translations: dict[str, str] | None = None,
) -> ClusterOut:
    if ensure_translations:
        ensure_reading_translations(session, [cluster.generated_title])
    read_status = event_identity.read_status if event_identity else "unread"
    read_later = event_identity.read_later if event_identity else False
    starred = event_identity.starred if event_identity else False
    return ClusterOut(
        id=cluster.id,
        event_uid=event_identity.event_uid if event_identity else None,
        current_revision_uid=(
            event_identity.current_revision_uid if event_identity else None
        ),
        seen_revision_uid=(
            event_identity.seen_revision_uid if event_identity else None
        ),
        current_revision_differs_from_seen=(
            event_identity.current_revision_differs_from_seen
            if event_identity
            else False
        ),
        has_material_update=(
            event_identity.has_material_update if event_identity else False
        ),
        material_update_revision_uid=(
            event_identity.material_update_revision_uid if event_identity else None
        ),
        title=clean_title(cluster.title),
        generated_title=cluster.generated_title,
        generated_title_translation=(cached_translations.get(cluster.generated_title, "") if cached_translations is not None else translation_text_for(session, cluster.generated_title)),
        generated_summary=cluster.generated_summary,
        generated_content=cluster.generated_content,
        citations=cluster.citations,
        model_version=cluster.model_version,
        prompt_version=cluster.prompt_version,
        first_seen_at=cluster.first_seen_at,
        last_seen_at=cluster.last_seen_at,
        item_count=item_count,
        read_status=read_status,
        read_later=read_later,
        starred=starred,
        uninterested=event_identity.uninterested if event_identity else False,
        uninterested_reason=(
            event_identity.uninterested_reason if event_identity else None
        ),
        uninterested_note=(
            event_identity.uninterested_note if event_identity else None
        ),
        uninterested_at=(
            event_identity.uninterested_at if event_identity else None
        ),
        items=items or [],
        synthesis_freshness=synthesis_freshness,
    )


def cluster_preview_items(session: Session, cluster_id: int) -> list[ItemOut]:
    return cluster_preview_items_for(session, [cluster_id]).get(cluster_id, [])


def cluster_preview_items_for(
    session: Session,
    cluster_ids: list[int],
    *,
    include_filtered: bool = False,
) -> dict[int, list[ItemOut]]:
    if not cluster_ids:
        return {}
    stmt = (
        select(ClusterItem.cluster_id, ContentItem, Source, UserState)
        .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(UserState, (UserState.object_type == "item") & (UserState.object_id == ContentItem.id), isouter=True)
        .options(
            *content_item_document_options(include_reading_body=False)
        )
        .where(ClusterItem.cluster_id.in_(cluster_ids), Source.status == "active", Source.media_type == "article")
        .order_by(ClusterItem.cluster_id.asc(), ContentItem.published_at.asc().nullslast(), ContentItem.id.asc())
    )
    stmt = stmt.where(ordinary_content_clause(session, ContentItem.id))
    if not include_filtered:
        stmt = stmt.where(unfiltered_content_clause(ContentItem.id))
    rows = session.execute(stmt).all()
    translations = translation_texts_for(
        session,
        [
            text
            for _cluster_id, item, _source, _state in rows
            for text in (
                clean_title(item.title, clean_title(item.summary or item.content_text[:220])),
                clean_preview(item.summary or item.content_text[:500], 500),
            )
        ],
    )
    labels = active_filter_labels_for_items(
        session, [item.id for _cluster_id, item, _source, _state in rows]
    )
    grouped: dict[int, list[ItemOut]] = {}
    for cluster_id, item, source, state in rows:
        grouped.setdefault(cluster_id, []).append(
            item_out(
                session,
                item,
                source,
                state,
                include_content=False,
                cached_translations=translations,
                filter_rules=labels.get(item.id, []),
            )
        )
    return grouped


def topic_out(topic: TopicGroup, clusters: list[ClusterOut], state: UserState | None = None) -> TopicGroupOut:
    last_seen = max((cluster.first_seen_at for cluster in clusters if cluster.first_seen_at), default=None)
    return TopicGroupOut(
        id=topic.id,
        name=topic.name,
        query=topic.query,
        description=topic.description,
        cluster_count=len(clusters),
        last_seen_at=last_seen,
        read_status=state.read_status if state else "unread",
        read_later=state.read_later if state else False,
        starred=state.starred if state else False,
    )


def topic_state(session: Session, topic_id: int) -> UserState | None:
    return session.scalar(select(UserState).where(UserState.object_type == "topic", UserState.object_id == topic_id))


def topic_clusters(session: Session, query: str) -> list[ClusterOut]:
    terms = [term for term in topic_terms(query) if has_search_term(term)]
    if not terms:
        return []
    matches = [
        or_(
            Cluster.title.ilike(like_pattern(term), escape="\\"),
            Cluster.generated_title.ilike(like_pattern(term), escape="\\"),
            Cluster.generated_summary.ilike(like_pattern(term), escape="\\"),
            ContentItem.title.ilike(like_pattern(term), escape="\\"),
            ContentItem.summary.ilike(like_pattern(term), escape="\\"),
            ContentItem.content_text.ilike(like_pattern(term), escape="\\"),
            Source.name.ilike(like_pattern(term), escape="\\"),
        )
        for term in terms
    ]
    matched_clusters = (
        select(Cluster.id)
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            Source.status == "active",
            Source.media_type == "article",
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
        )
        .where(or_(*matches))
        .distinct()
    )
    current_event_state, effective_read_status, _, _ = (
        cluster_effective_state_expressions(session)
    )
    stmt = (
        select(Cluster, func.count(ClusterItem.id))
        .join(ClusterItem, ClusterItem.cluster_id == Cluster.id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .join(current_event_state, current_event_state.c.cluster_id == Cluster.id, isouter=True)
        .where(
            Source.status == "active",
            Source.media_type == "article",
            unfiltered_content_clause(ContentItem.id),
            ordinary_content_clause(session, ContentItem.id),
            func.coalesce(current_event_state.c.uninterested, false()).is_(False),
        )
        .where(Cluster.id.in_(matched_clusters))
        .where(effective_read_status != "dismissed")
        .group_by(Cluster.id)
        .order_by(Cluster.first_seen_at.asc().nullslast(), Cluster.id.asc())
        .limit(80)
    )
    rows = session.execute(stmt).all()
    identities = cluster_event_identities_for(
        session, [cluster.id for cluster, _count in rows]
    )
    return [
        cluster_out(
            session,
            cluster,
            count,
            event_identity=identities.get(cluster.id),
        )
        for cluster, count in rows
    ]


def topic_terms(query: str) -> list[str]:
    value = query.strip()
    for delimiter in ("，", "、", ";", "；", "\n"):
        value = value.replace(delimiter, ",")
    return [part.strip() for part in value.split(",") if part.strip()]


def citation_for(item: ContentItem, source: Source) -> dict[str, object]:
    return {
        "source_name": source.name,
        "title": clean_title(item.title, clean_title(item.summary or item.content_text[:220])),
        "url": item.url,
        "published_at": item.published_at.isoformat() if item.published_at else None,
    }


def task_result_data(task: LLMTask) -> dict[str, object]:
    try:
        data = json.loads(task.result_json)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def task_request(task: LLMTask) -> dict[str, object]:
    data = task_result_data(task)
    request = data.get("request") if isinstance(data, dict) else {}
    return request if isinstance(request, dict) else {}


def task_execution_contract(task: LLMTask) -> dict[str, object]:
    execution = task_result_data(task).get("execution")
    return execution if isinstance(execution, dict) else {}


def report_bounds(period: str, date_value: str | None = None) -> tuple[datetime, datetime]:
    if period not in REPORT_PERIODS:
        raise HTTPException(status_code=400, detail="不支持的报告周期")
    if date_value:
        try:
            parsed = datetime.fromisoformat(date_value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="报告日期格式无效") from exc
        anchor = parsed.replace(tzinfo=APP_TIMEZONE) if parsed.tzinfo is None else parsed.astimezone(APP_TIMEZONE)
    else:
        anchor = now_utc().astimezone(APP_TIMEZONE)
    start = anchor.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        start = start - timedelta(days=start.weekday())
    if period == "month":
        start = start.replace(day=1)
    if period == "day":
        end = start + timedelta(days=1)
    elif period == "week":
        end = start + timedelta(days=7)
    else:
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def report_key(start: datetime) -> int:
    return int(start.astimezone(APP_TIMEZONE).strftime("%Y%m%d"))


def report_state_key(period: str, start: datetime) -> int:
    return REPORT_STATE_PREFIX[period] * 100000000 + report_key(start)


def latest_report_task(session: Session, period: str, object_id: int, statuses: set[str] | None = None) -> LLMTask | None:
    selected_statuses = statuses or {"complete"}
    return session.scalar(
        select(LLMTask)
        .where(
            LLMTask.task_type == f"report:{period}",
            LLMTask.object_type == "report",
            LLMTask.object_id == object_id,
            LLMTask.status.in_(list(selected_statuses)),
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
    )


def report_user_state(session: Session, period: str, start: datetime) -> UserState | None:
    return session.scalar(select(UserState).where(UserState.object_type == "report", UserState.object_id == report_state_key(period, start)))


def report_state_fields(period: str, start: datetime, state: UserState | None) -> dict[str, object]:
    return {
        "object_id": report_state_key(period, start),
        "read_status": state.read_status if state else "unread",
        "read_later": state.read_later if state else False,
        "starred": state.starred if state else False,
    }


def report_empty(period: str, start: datetime, end: datetime, state: UserState | None = None) -> dict[str, object]:
    return {
        "period": period,
        "title": REPORT_PERIODS[period],
        "status": "empty",
        "body": "",
        "items": [],
        "citations": [],
        "start": start.isoformat(),
        "end": end.isoformat(),
        **report_state_fields(period, start, state),
    }


def report_citations_with_event_identity(
    session: Session,
    value: object,
    *,
    generated_at: datetime | None,
    request: GenerationRequest | None = None,
) -> list[dict[str, object]]:
    citations = (
        [dict(row) for row in value if isinstance(row, dict)]
        if isinstance(value, list)
        else []
    )
    if not citations:
        return []
    identities: dict[int, tuple[str, str]] = {}
    if request is not None:
        payload = session.get(GenerationRequestPayload, request.id)
        frozen_items: object = None
        if payload is not None and payload.payload_json is not None:
            try:
                frozen_items = frozen_report_input(
                    request, payload.payload_json
                ).get("items")
            except ReportValidationError:
                frozen_items = None
        elif payload is not None and isinstance(payload.application_context_json, dict):
            frozen_items = payload.application_context_json.get("citations")
        if isinstance(frozen_items, list):
            for item in frozen_items:
                if not isinstance(item, dict):
                    continue
                cluster_id = item.get("cluster_id")
                event_uid = item.get("event_uid")
                revision_uid = item.get("event_revision_uid")
                if (
                    isinstance(cluster_id, int)
                    and not isinstance(cluster_id, bool)
                    and isinstance(event_uid, str)
                    and event_uid
                    and isinstance(revision_uid, str)
                    and revision_uid
                ):
                    identities[cluster_id] = (event_uid, revision_uid)
    missing_ids = sorted(
        {
            cluster_id
            for citation in citations
            if isinstance((cluster_id := citation.get("cluster_id")), int)
            and not isinstance(cluster_id, bool)
            and cluster_id not in identities
        }
    )
    if missing_ids:
        statement = (
            select(
                ClusterEventProjection.cluster_id_snapshot,
                Event.uid,
                EventRevision.uid,
            )
            .join(Event, Event.id == ClusterEventProjection.event_id)
            .join(
                EventRevision,
                EventRevision.id == ClusterEventProjection.event_revision_id,
            )
            .where(ClusterEventProjection.cluster_id_snapshot.in_(missing_ids))
            .order_by(
                ClusterEventProjection.cluster_id_snapshot,
                ClusterEventProjection.projected_at.desc(),
                ClusterEventProjection.id.desc(),
            )
        )
        if generated_at is not None:
            statement = statement.where(
                ClusterEventProjection.projected_at <= generated_at
            )
        for cluster_id, event_uid, revision_uid in session.execute(statement):
            identities.setdefault(cluster_id, (event_uid, revision_uid))
    for citation in citations:
        if isinstance(citation.get("event_uid"), str) and isinstance(
            citation.get("event_revision_uid"), str
        ):
            continue
        identity = identities.get(citation.get("cluster_id"))
        if identity is not None:
            citation["event_uid"], citation["event_revision_uid"] = identity
    return citations


def report_snapshot(
    session: Session,
    period: str,
    start: datetime,
    end: datetime,
    task: LLMTask,
    state: UserState | None = None,
) -> dict[str, object]:
    try:
        data = json.loads(task.result_json)
    except json.JSONDecodeError:
        data = {}
    return {
        "period": period,
        "title": data.get("title") or REPORT_PERIODS[period],
        "status": "ready",
        "body": data.get("body") or "",
        "items": data.get("cluster_ids") or [],
        "citations": report_citations_with_event_identity(
            session,
            data.get("citations"),
            generated_at=task.updated_at,
        ),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "model_version": task.model_version,
        "prompt_version": task.prompt_version,
        "updated_at": task.updated_at.isoformat() if task.updated_at else None,
        **report_state_fields(period, start, state),
    }


def report_generation_pending(
    period: str,
    start: datetime,
    end: datetime,
    request: GenerationRequest,
    state: UserState | None = None,
) -> dict[str, object]:
    return {
        "period": period,
        "title": REPORT_PERIODS[period],
        "status": "pending",
        "body": "",
        "items": [],
        "citations": [],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "model_version": request.model,
        "prompt_version": request.prompt_version,
        "updated_at": request.created_at.isoformat() if request.created_at else None,
        **report_state_fields(period, start, state),
    }


def report_generation_snapshot(
    session: Session,
    period: str,
    start: datetime,
    end: datetime,
    request: GenerationRequest,
    result: GenerationResult,
    state: UserState | None = None,
) -> dict[str, object]:
    data = result.payload_json
    return {
        "period": period,
        "title": data.get("title") or REPORT_PERIODS[period],
        "status": "ready",
        "body": data.get("body") or "",
        "items": data.get("cluster_ids") or [],
        "citations": report_citations_with_event_identity(
            session,
            data.get("citations"),
            generated_at=result.created_at,
            request=request,
        ),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "model_version": request.model,
        "prompt_version": request.prompt_version,
        "updated_at": result.created_at.isoformat() if result.created_at else None,
        **report_state_fields(period, start, state),
    }


def report_generation_view(
    session: Session,
    period: str,
    start: datetime,
    end: datetime,
    state: UserState | None = None,
) -> dict[str, object] | None:
    requests = session.scalars(
        select(GenerationRequest)
        .where(
            GenerationRequest.task_type == f"report:{period}",
            GenerationRequest.target_type == "report",
            GenerationRequest.target_id == report_key(start),
        )
        .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
    ).all()
    for request in requests:
        result_application = generation_result_for_request(session, request.id)
        if result_application is not None:
            result, application = result_application
            if application.status == "applied":
                return report_generation_snapshot(
                    session, period, start, end, request, result, state
                )
            continue
        attempt = latest_attempt(session, request.id)
        if attempt is None or attempt.status in {"pending", "running"}:
            return report_generation_pending(period, start, end, request, state)
    return None


def latest_item_summary_task(session: Session, item_id: int, statuses: set[str] | None = None) -> LLMTask | None:
    selected_statuses = statuses or {"complete"}
    return session.scalar(
        select(LLMTask)
        .where(
            LLMTask.task_type == "item-summary",
            LLMTask.object_type == "item",
            LLMTask.object_id == item_id,
            LLMTask.status.in_(list(selected_statuses)),
        )
        .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
    )


def item_summary_generation_snapshot(
    session: Session, item_id: int
) -> AISummaryOut | None:
    row = latest_item_summary_generation(session, item_id)
    if row is None:
        return None
    request, result, application = row
    if result is None or application is None or application.status != "applied":
        applied = latest_applied_item_summary_generation(session, item_id)
        if applied is not None:
            request, result, application = applied
    task = generation_task_out(session, request)
    if result is not None and application is not None and application.status == "applied":
        return AISummaryOut(
            object_type="item",
            object_id=item_id,
            status="ready",
            summary=str(result.payload_json.get("summary") or "").strip(),
            model_version=request.model,
            updated_at=task.finished_at,
        )
    return AISummaryOut(
        object_type="item",
        object_id=item_id,
        status=task.status,
        summary="",
        model_version=request.model,
        updated_at=task.finished_at or task.started_at or task.created_at,
    )


def item_summary_snapshot(item_id: int, task: LLMTask | None) -> AISummaryOut:
    if task is None:
        return AISummaryOut(object_type="item", object_id=item_id, status="empty", summary="")
    try:
        data = json.loads(task.result_json)
    except json.JSONDecodeError:
        data = {}
    return AISummaryOut(
        object_type="item",
        object_id=item_id,
        status="ready",
        summary=str(data.get("summary") or ""),
        model_version=task.model_version,
        updated_at=task.updated_at,
    )


def assistant_citation(item: ItemOut) -> AssistantCitationOut:
    return AssistantCitationOut(
        id=item.id,
        title=item.title,
        source_name=item.source_name,
        published_at=item.published_at,
        url=item.url,
    )


def report_generation_sources(
    session: Session, clusters: list[Cluster]
) -> tuple[list[int], bool]:
    cluster_ids = [cluster.id for cluster in clusters]
    source_ids = set(
        session.scalars(
            select(ContentItem.source_id)
            .join(ClusterItem, ClusterItem.content_item_id == ContentItem.id)
            .where(ClusterItem.cluster_id.in_(cluster_ids))
            .distinct()
        ).all()
    )
    provenance_complete = bool(source_ids)
    for cluster in clusters:
        if not (cluster.generated_title or cluster.generated_summary):
            continue
        lifecycle_request = session.scalar(
            select(GenerationRequest)
            .join(
                GenerationApplication,
                GenerationApplication.request_id == GenerationRequest.id,
            )
            .where(
                GenerationRequest.task_type == CLUSTER_SYNTHESIS_TASK_TYPE,
                GenerationRequest.target_type == "cluster",
                GenerationRequest.target_id == cluster.id,
                GenerationRequest.prompt_version == cluster.prompt_version,
                GenerationRequest.model == cluster.model_version,
                GenerationApplication.status == "applied",
                GenerationApplication.artifact_type == "cluster-synthesis",
                GenerationApplication.artifact_id == cluster.id,
            )
            .order_by(GenerationRequest.created_at.desc(), GenerationRequest.id.desc())
            .limit(1)
        )
        if lifecycle_request is not None:
            frozen_sources = generation_request_sources(
                session, lifecycle_request.id
            )
            source_ids.update(source.source_id for source in frozen_sources)
            provenance_complete = provenance_complete and bool(frozen_sources)
            continue
        synthesis_task = session.scalar(
            select(LLMTask)
            .where(
                LLMTask.task_type == "cluster-synthesis",
                LLMTask.object_type == "cluster",
                LLMTask.object_id == cluster.id,
                LLMTask.status == "complete",
                LLMTask.prompt_version == cluster.prompt_version,
                LLMTask.model_version == cluster.model_version,
            )
            .order_by(LLMTask.updated_at.desc(), LLMTask.id.desc())
            .limit(1)
        )
        if synthesis_task is not None and synthesis_task.provider == "legacy":
            execution_source_ids = task_execution_contract(synthesis_task).get(
                "source_ids"
            )
            if (
                isinstance(execution_source_ids, list)
                and execution_source_ids
                and all(
                    isinstance(source_id, int) and source_id > 0
                    for source_id in execution_source_ids
                )
            ):
                source_ids.update(execution_source_ids)
                continue
            frozen_request_citations = task_request(synthesis_task).get("citations")
            resolved, complete = source_ids_from_citations(
                session, frozen_request_citations
            )
            source_ids.update(resolved)
            provenance_complete = provenance_complete and complete
            continue
        try:
            citations = json.loads(cluster.citations)
        except (TypeError, json.JSONDecodeError):
            citations = None
        resolved, complete = source_ids_from_citations(session, citations)
        source_ids.update(resolved)
        provenance_complete = (
            provenance_complete
            and complete
            and synthesis_task is not None
            and synthesis_task.provider == "local-chat"
        )
    return sorted(source_ids), provenance_complete


def source_ids_from_citations(
    session: Session, citations: object
) -> tuple[set[int], bool]:
    source_ids: set[int] = set()
    if not isinstance(citations, list) or not citations:
        return source_ids, False
    complete = True
    for citation in citations:
        if not isinstance(citation, dict):
            complete = False
            continue
        source_id = citation.get("source_id")
        if isinstance(source_id, int) and source_id > 0:
            source_ids.add(source_id)
            continue
        url = citation.get("url")
        if not isinstance(url, str) or not url.strip():
            complete = False
            continue
        matched_source_ids = set(
            session.scalars(
                select(ContentItem.source_id).where(
                    or_(ContentItem.url == url, ContentItem.canonical_url == url)
                )
            ).all()
        )
        if not matched_source_ids:
            complete = False
        source_ids.update(matched_source_ids)
    return source_ids, complete


def report_fields(result: dict[str, object]) -> tuple[str, str]:
    if isinstance(result.get("title"), str) or isinstance(result.get("body"), str) or isinstance(result.get("summary"), str):
        return str(result.get("title") or "").strip(), str(result.get("body") or result.get("summary") or result.get("content") or "").strip()
    text_value = llm_text(result)
    json_text = unfence_json(text_value)
    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        return "", text_value
    if isinstance(parsed, dict):
        return str(parsed.get("title") or "").strip(), str(parsed.get("body") or parsed.get("summary") or parsed.get("content") or "").strip()
    return "", text_value


def synthesis_fields(result: dict[str, object]) -> tuple[str, str, str]:
    if isinstance(result.get("title"), str) or isinstance(result.get("summary"), str) or isinstance(result.get("content"), str):
        return str(result.get("title") or "").strip(), str(result.get("summary") or "").strip(), str(result.get("content") or "").strip()
    text_value = llm_text(result)
    json_text = unfence_json(text_value)
    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        return "", "", text_value
    if isinstance(parsed, dict):
        return str(parsed.get("title") or "").strip(), str(parsed.get("summary") or "").strip(), str(parsed.get("content") or parsed.get("body") or "").strip()
    return "", "", text_value


def assistant_answer(result: dict[str, object]) -> str:
    if isinstance(result.get("answer"), str):
        return str(result.get("answer") or "").strip()
    text_value = llm_text(result)
    try:
        parsed = json.loads(unfence_json(text_value))
    except json.JSONDecodeError:
        return text_value
    if isinstance(parsed, dict):
        for key in ("answer", "body", "summary", "content"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return text_value


def translation_snapshot(task: LLMTask) -> TranslationOut:
    try:
        data = json.loads(task.result_json)
    except json.JSONDecodeError:
        data = {}
    return TranslationOut(
        status="ready",
        translation=str(data.get("translation") or ""),
        blocks=(
            data.get("blocks")
            if isinstance(data.get("blocks"), list)
            else []
        ),
        model_version=task.model_version,
        updated_at=task.updated_at,
    )


def llm_text(result: dict[str, object]) -> str:
    for key in ("output", "text", "reply", "response", "content"):
        value = result.get(key)
        if isinstance(value, str):
            return value.strip()
        if key == "output" and isinstance(value, list):
            for item in reversed(value):
                if isinstance(item, dict) and item.get("type") == "message" and isinstance(item.get("content"), str):
                    return item["content"].strip()
            for item in reversed(value):
                if isinstance(item, dict) and isinstance(item.get("content"), str):
                    return item["content"].strip()
                if isinstance(item, str):
                    return item.strip()
    choices = result.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        message = choices[0].get("message")
        if isinstance(message, dict) and isinstance(message.get("content"), str):
            return message["content"].strip()
        text = choices[0].get("text")
        if isinstance(text, str):
            return text.strip()
    return json.dumps(result, ensure_ascii=False)


def unfence_json(value: str) -> str:
    lines = value.strip().splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def natural_name_key(value: str) -> tuple[int, list[tuple[int, object]], str]:
    normalized = normalize_space(value).casefold()
    bucket = 0 if re.match(r"^[0-9a-z]", normalized) else 1
    parts: list[tuple[int, object]] = []
    for part in re.split(r"(\d+)", normalized):
        if part:
            parts.append((0, int(part)) if part.isdigit() else (1, part))
    return bucket, parts, normalized


def search_clause(session: Session, query: str, include_cluster: bool = False):
    term = query.strip()
    if not has_search_term(term):
        return false()
    fuzzy_fields = [ContentItem.title, ContentItem.summary, ContentItem.content_text, Source.name]
    if include_cluster:
        fuzzy_fields.extend([Cluster.title, Cluster.generated_title, Cluster.generated_summary, Cluster.generated_content])
    fuzzy_clause = and_(
        *(
            or_(
                *(field.ilike(like_pattern(part), escape="\\") for field in fuzzy_fields),
            )
            for part in search_terms(term)
        )
    )
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        if needs_fuzzy_search(term):
            return fuzzy_clause
        item_text = func.coalesce(ContentItem.title, "") + literal(" ") + func.coalesce(ContentItem.summary, "") + literal(" ") + func.coalesce(ContentItem.content_text, "")
        item_fts_clause = func.to_tsvector("simple", item_text).op("@@")(func.plainto_tsquery("simple", term))
        source_fts_clause = func.to_tsvector("simple", func.coalesce(Source.name, "")).op("@@")(func.plainto_tsquery("simple", term))
        clauses = [item_fts_clause, source_fts_clause]
        if include_cluster:
            cluster_text = (
                func.coalesce(Cluster.title, "")
                + literal(" ")
                + func.coalesce(Cluster.generated_title, "")
                + literal(" ")
                + func.coalesce(Cluster.generated_summary, "")
                + literal(" ")
                + func.coalesce(Cluster.generated_content, "")
            )
            cluster_fts_clause = func.to_tsvector("simple", cluster_text).op("@@")(func.plainto_tsquery("simple", term))
            clauses.append(cluster_fts_clause)
        return or_(*clauses)
    return fuzzy_clause


def search_terms(value: str) -> list[str]:
    return [part for part in re.split(r"[\s,，、;；]+", value.strip()) if has_search_term(part)]


def has_search_term(value: str) -> bool:
    return any(char.isalnum() for char in value)


def like_pattern(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def needs_fuzzy_search(value: str) -> bool:
    return any(not part.isascii() for part in search_terms(value))


def can_use_indexed_search(session: Session, value: str) -> bool:
    return bool(session.bind is not None and session.bind.dialect.name == "postgresql" and has_search_term(value) and not needs_fuzzy_search(value))


def indexed_item_search_query(term: str):
    item_text = func.coalesce(ContentItem.title, "") + literal(" ") + func.coalesce(ContentItem.summary, "") + literal(" ") + func.coalesce(ContentItem.content_text, "")
    source_text = func.coalesce(Source.name, "")
    tsquery = func.plainto_tsquery("simple", term)
    return select(ContentItem.id).where(func.to_tsvector("simple", item_text).op("@@")(tsquery)).union(
        select(ContentItem.id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(func.to_tsvector("simple", source_text).op("@@")(tsquery))
    )


def indexed_cluster_search_query(
    session: Session,
    term: str,
    folder_id: int | None,
    source_id: int | None,
):
    item_text = func.coalesce(ContentItem.title, "") + literal(" ") + func.coalesce(ContentItem.summary, "") + literal(" ") + func.coalesce(ContentItem.content_text, "")
    source_text = func.coalesce(Source.name, "")
    cluster_text = (
        func.coalesce(Cluster.title, "")
        + literal(" ")
        + func.coalesce(Cluster.generated_title, "")
        + literal(" ")
        + func.coalesce(Cluster.generated_summary, "")
        + literal(" ")
        + func.coalesce(Cluster.generated_content, "")
    )
    tsquery = func.plainto_tsquery("simple", term)
    return base_cluster_search_query(session, folder_id, source_id).where(func.to_tsvector("simple", item_text).op("@@")(tsquery)).union(
        base_cluster_search_query(session, folder_id, source_id).where(func.to_tsvector("simple", source_text).op("@@")(tsquery)),
        base_cluster_search_query(session, folder_id, source_id, include_cluster=True).where(func.to_tsvector("simple", cluster_text).op("@@")(tsquery)),
    )


def base_cluster_search_query(
    session: Session,
    folder_id: int | None,
    source_id: int | None,
    include_cluster: bool = False,
):
    stmt = (
        select(ClusterItem.cluster_id)
        .join(ContentItem, ContentItem.id == ClusterItem.content_item_id)
        .join(Source, Source.id == ContentItem.source_id)
        .where(
            Source.status == "active",
            Source.media_type == "article",
            ordinary_content_clause(session, ContentItem.id),
        )
    )
    if include_cluster:
        stmt = stmt.join(Cluster, Cluster.id == ClusterItem.cluster_id)
    if folder_id is not None:
        stmt = stmt.where(Source.folder_id == folder_id)
    if source_id is not None:
        stmt = stmt.where(Source.id == source_id)
    return stmt
