from datetime import datetime
from typing import Annotated, Literal, TypeAlias
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictBool, model_validator

from .article_selectors import validate_article_selector
GenerationTaskStatus: TypeAlias = Literal[
    "blocked",
    "pending",
    "running",
    "failed",
    "apply_pending",
    "apply_failed",
    "stale_result",
    "complete",
    "canceled",
]
EventGenerationTaskStatus: TypeAlias = Literal["idle"] | GenerationTaskStatus
UninterestedReason: TypeAlias = Literal[
    "promotion",
    "repetitive",
    "topic",
    "low_quality",
    "other",
]
ArticleSelector: TypeAlias = Annotated[str, AfterValidator(validate_article_selector)]


def validate_topic_query(value: str) -> str:
    normalized = value
    for delimiter in ("，", "、", ";", "；", "\n"):
        normalized = normalized.replace(delimiter, ",")
    if len([part for part in normalized.split(",") if part.strip()]) > 20:
        raise ValueError("议题关键词不能超过 20 个")
    return value


TopicQuery: TypeAlias = Annotated[
    str,
    Field(max_length=500),
    AfterValidator(validate_topic_query),
]


class FolderCreate(BaseModel):
    name: str = Field(max_length=240)
    media_type: Literal["article", "social", "image", "video", "podcast", "notification"]


class FolderPatch(BaseModel):
    name: str | None = Field(default=None, max_length=240)

    model_config = ConfigDict(extra="forbid")


class FolderOut(BaseModel):
    id: int
    name: str
    media_type: Literal["article", "social", "image", "video", "podcast", "notification"]

    model_config = ConfigDict(from_attributes=True)


class SourceCreate(BaseModel):
    name: str = Field(max_length=320)
    url: str
    folder_id: int | None = None
    media_type: str = "article"
    status: str = "active"
    fetch_full_content: bool = False
    article_selector: ArticleSelector | None = None
    remove_selector: ArticleSelector | None = None
    privacy_class: Literal["unclassified", "public", "private"] = "unclassified"
    external_generation_allowed: bool = False


class SourcePatch(BaseModel):
    name: str | None = Field(default=None, max_length=320)
    url: str | None = None
    folder_id: int | None = None
    media_type: str | None = None
    status: str | None = None
    enabled: bool | None = None
    fetch_full_content: bool | None = None
    article_selector: ArticleSelector | None = None
    remove_selector: ArticleSelector | None = None
    privacy_class: Literal["unclassified", "public", "private"] | None = None
    external_generation_allowed: bool | None = None


class SourceBulkSet(BaseModel):
    folder_id: int | None = None
    media_type: str | None = None
    status: str | None = None
    enabled: bool | None = None
    privacy_class: Literal["unclassified", "public", "private"] | None = None
    external_generation_allowed: bool | None = None


class SourceBulkPatch(BaseModel):
    ids: list[int] = Field(max_length=10_000)
    changes: SourceBulkSet = Field(alias="set")


class SourceBulkOut(BaseModel):
    updated: int


class SourceDiscoverIn(BaseModel):
    url: str


class SourceDiscoverCandidate(BaseModel):
    title: str
    url: str


class SourceDiscoverEntry(BaseModel):
    title: str = ""
    summary: str = ""
    image_url: str = ""
    media_url: str = ""
    media_kind: str = ""
    media_duration: int = 0
    url: str = ""
    published_at: datetime | None = None


class SourceDiscoverOut(BaseModel):
    candidates: list[SourceDiscoverCandidate] = Field(default_factory=list)
    entries: list[SourceDiscoverEntry] = Field(default_factory=list)
    site_url: str
    title: str


class ArticlePreviewEntryOut(BaseModel):
    raw_entry_id: int
    title: str
    published_at: datetime | None


class PublicRulesStatusOut(BaseModel):
    version: str
    commit: str
    activated_at: datetime | None
    bundled: bool


class PublicRulesActivateIn(BaseModel):
    commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    model_config = ConfigDict(extra="forbid")


class PublicRuleExtractionPreviewOut(BaseModel):
    hostname: str
    title: str
    reading_html: str
    rss_characters: int
    webpage_characters: int
    method: str
    version: str
    adopted_webpage: bool
    matched_elements: int
    removed_elements: int
    diagnostics: list[str]
    fallback_reason: str
    passed: bool


class PublicRulesCheckOut(BaseModel):
    current_version: str
    current_commit: str
    candidate_version: str
    candidate_commit: str
    rules_count: int
    skipped_count: int
    subscribed_domains: int
    covered_subscribed_domains: int
    changed_subscribed_domains: int
    tested_subscribed_domains: int
    invalid_subscribed_domains: list[str]
    failed_subscribed_domains: list[str]
    preview: PublicRuleExtractionPreviewOut | None
    passed: bool
    can_activate: bool


class ArticlePreviewOptionsOut(BaseModel):
    entries: list[ArticlePreviewEntryOut]
    public_rules: PublicRulesStatusOut


class ArticlePreviewIn(BaseModel):
    raw_entry_id: int
    fetch_full_content: bool
    article_selector: ArticleSelector | None = None
    remove_selector: ArticleSelector | None = None

    model_config = ConfigDict(extra="forbid")


class ArticlePreviewOut(BaseModel):
    raw_entry_id: int
    title: str
    reading_html: str
    rss_characters: int
    webpage_characters: int
    method: str
    version: str
    body_source: Literal["rss", "webpage"]
    web_fetch_status: Literal["not_requested", "failed", "succeeded"]
    adopted_webpage: bool
    matched_elements: int
    removed_elements: int
    diagnostics: list[str]
    fallback_reason: str


class SourceOut(BaseModel):
    id: int
    folder_id: int | None
    name: str
    url: str
    site_url: str
    media_type: str
    status: str
    enabled: bool
    fetch_full_content: bool
    article_selector: str | None
    remove_selector: str | None
    privacy_class: Literal["unclassified", "public", "private"]
    external_generation_allowed: bool
    unread_count: int = 0
    folder_unread_count: int = 0
    all_unread_count: int = 0
    feed_trust_score: float
    fetched_count: int = 0
    read_count: int = 0
    opened_count: int = 0
    starred_count: int = 0
    read_later_count: int = 0
    cluster_count: int = 0
    duplicate_count: int = 0
    recent_entry_count_30d: int = 0
    last_fetched_at: datetime | None
    last_error: str
    status_changed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class SourceNavigationOut(BaseModel):
    id: int
    folder_id: int | None
    name: str
    url: str
    site_url: str
    media_type: str
    status: str
    enabled: bool
    unread_count: int = 0
    folder_unread_count: int = 0
    all_unread_count: int = 0
    starred_count: int = 0
    last_fetched_at: datetime | None
    last_error: str


class FilterRuleSpec(BaseModel):
    source_id: int | None = None
    match_type: Literal["literal", "regex"] = "literal"
    pattern: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_pattern(self) -> "FilterRuleSpec":
        self.pattern = self.pattern.strip()
        if not self.pattern:
            raise ValueError("过滤表达式不能为空")
        return self


class FilterRuleCreate(FilterRuleSpec):
    pass


class FilterRulePatch(BaseModel):
    source_id: int | None = None
    match_type: Literal["literal", "regex"] | None = None
    pattern: str | None = Field(default=None, min_length=1, max_length=500)
    enabled: bool | None = None

    @model_validator(mode="after")
    def validate_pattern(self) -> "FilterRulePatch":
        if self.pattern is not None:
            self.pattern = self.pattern.strip()
            if not self.pattern:
                raise ValueError("过滤表达式不能为空")
        return self


class FilterRuleOut(BaseModel):
    id: int
    source_id: int | None
    source_name: str = ""
    match_type: Literal["literal", "regex"]
    pattern: str
    enabled: bool
    match_count: int = 0
    created_at: datetime
    updated_at: datetime


class UserStatePatch(BaseModel):
    operation_id: str | None = Field(default=None, min_length=1, max_length=120)
    read_status: str | None = None
    read_later: StrictBool | None = None
    starred: StrictBool | None = None


class EventUserStateMutationIn(BaseModel):
    event_uid: str = Field(min_length=1, max_length=36)
    observed_revision_uid: str = Field(min_length=1, max_length=36)
    operation_id: str = Field(min_length=1, max_length=120)
    action: str = Field(min_length=1, max_length=40)
    value: StrictBool | Literal["unread", "summary_seen", "original_opened"]
    source_id: int | None = Field(default=None, gt=0)
    evidence_version_uid: str | None = Field(default=None, min_length=1, max_length=36)


class EventUserStateMutationOut(BaseModel):
    operation_id: str
    event_uid: str
    observed_revision_uid: str
    action: str
    value: bool | str
    read_later: bool
    starred: bool
    updated_at: datetime
    source_id: int | None = None
    evidence_version_uid: str | None = None
    read_status: str | None = None
    seen_revision_uid: str | None = None
    current_revision_differs_from_seen: bool | None = None
    has_material_update: bool | None = None
    material_update_revision_uid: str | None = None


class UninterestedMutationIn(BaseModel):
    operation_id: str = Field(min_length=1, max_length=120)
    target_type: Literal["event", "item", "article"]
    event_uid: str | None = Field(default=None, min_length=1, max_length=36)
    observed_revision_uid: str | None = Field(
        default=None, min_length=1, max_length=36
    )
    item_id: int | None = Field(default=None, gt=0)
    value: StrictBool
    reason: UninterestedReason | None = None
    note: str | None = Field(default=None, max_length=240)

    @model_validator(mode="after")
    def validate_target_and_reason(self) -> "UninterestedMutationIn":
        is_event = self.target_type == "event"
        if is_event != bool(self.event_uid and self.observed_revision_uid):
            raise ValueError("Event 目标必须提交事件及其已观察版本")
        if is_event and self.item_id is not None:
            raise ValueError("Event 目标不能同时提交条目")
        if self.target_type in {"item", "article"} and (
            self.item_id is None
            or self.event_uid is not None
            or self.observed_revision_uid is not None
        ):
            raise ValueError("条目目标必须只提交 item_id")
        if not self.value and (self.reason is not None or self.note is not None):
            raise ValueError("恢复时不能提交原因")
        if self.note is not None:
            self.note = self.note.strip()
            if not self.note:
                self.note = None
        if self.reason == "other" and not self.note:
            raise ValueError("选择“其他”时请填写简短说明")
        if self.reason != "other" and self.note is not None:
            raise ValueError("只有“其他”原因可以填写说明")
        return self


class UninterestedMutationOut(BaseModel):
    operation_id: str
    target_kind: Literal["event", "item"]
    event_uid: str | None = None
    observed_revision_uid: str | None = None
    cluster_id: int | None = None
    item_id: int | None = None
    affected_item_ids: list[int] = Field(default_factory=list)
    uninterested: bool
    reason: UninterestedReason | None = None
    note: str | None = None
    marked_at: datetime | None = None
    updated_at: datetime


class BulkReadPrepareIn(BaseModel):
    object_type: Literal["item", "event"] = "item"
    folder_id: int | None = None
    source_id: int | None = None
    media_type: str | None = None
    q: str | None = None

    model_config = ConfigDict(extra="forbid")


class BulkReadPrepared(BaseModel):
    batch_id: UUID | None = None
    target_count: int = Field(ge=0)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_batch_shape(self) -> "BulkReadPrepared":
        if (self.target_count == 0) != (self.batch_id is None):
            raise ValueError("空批次不得包含 batch_id，非空批次必须包含 batch_id")
        return self


class BulkReadConfirmIn(BaseModel):
    batch_id: UUID

    model_config = ConfigDict(extra="forbid")


class BulkReadTarget(BaseModel):
    target_kind: Literal["event", "object"]
    operation_id: str = Field(min_length=1, max_length=120)
    event_uid: str | None = Field(default=None, min_length=1, max_length=36)
    observed_revision_uid: str | None = Field(
        default=None,
        min_length=1,
        max_length=36,
    )
    object_type: Literal["item"] | None = None
    object_id: int | None = Field(default=None, gt=0)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_target_shape(self) -> "BulkReadTarget":
        if self.target_kind == "event":
            if (
                self.event_uid is None
                or self.observed_revision_uid is None
                or self.object_type is not None
                or self.object_id is not None
            ):
                raise ValueError("Event 批量目标必须固定 Event 与 observed Revision")
            return self
        if (
            self.event_uid is not None
            or self.observed_revision_uid is not None
            or self.object_type != "item"
            or self.object_id is None
        ):
            raise ValueError("对象批量目标必须固定 item")
        return self


class BulkReadManifest(BaseModel):
    targets: list[BulkReadTarget] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_unique_targets(self) -> "BulkReadManifest":
        operation_ids = [target.operation_id for target in self.targets]
        if len(set(operation_ids)) != len(operation_ids):
            raise ValueError("批量清单中的 operation_id 不得重复")
        target_keys = [
            (
                target.target_kind,
                target.event_uid if target.target_kind == "event" else target.object_type,
                None if target.target_kind == "event" else target.object_id,
            )
            for target in self.targets
        ]
        if len(set(target_keys)) != len(target_keys):
            raise ValueError("批量清单中的目标不得重复")
        return self


class BrowseSourceSummaryOut(BaseModel):
    source_id: int
    folder_id: int | None
    name: str
    media_type: str
    unread_count: int = 0
    total_count: int = 0


class BrowseMediaSummaryOut(BaseModel):
    media_type: str
    unread_count: int = 0
    total_count: int = 0
    sources: list[BrowseSourceSummaryOut] = Field(default_factory=list)


class AISettingsOut(BaseModel):
    task_provider: str
    synthesis_provider: str
    base_url: str
    translation_provider: str
    translation_base_url: str
    translation_local_base_url: str
    translation_local_model: str
    translation_cloud_base_url: str
    translation_cloud_model: str
    translation_api_key_configured: bool
    embedding_base_url: str
    endpoint: str
    translation_endpoint: str
    embedding_endpoint: str
    llm_model: str
    translation_model: str
    embedding_model: str
    timeout_seconds: float
    synthesis_remote_base_url: str = ""
    synthesis_remote_model: str = ""
    synthesis_remote_api_key_configured: bool = False
    synthesis_remote_endpoint: str = ""


class AISettingsPatch(BaseModel):
    task_provider: str | None = None
    synthesis_provider: str | None = None
    base_url: str | None = None
    translation_provider: str | None = None
    translation_base_url: str | None = None
    translation_api_key: str | None = None
    clear_translation_api_key: bool = False
    embedding_base_url: str | None = None
    llm_model: str | None = None
    translation_model: str | None = None
    embedding_model: str | None = None
    timeout_seconds: float | None = None
    synthesis_remote_base_url: str | None = None
    synthesis_remote_model: str | None = None
    synthesis_remote_api_key: str | None = None
    clear_synthesis_remote_api_key: bool = False


class AIChatIn(BaseModel):
    input: str
    model_type: str = "llm"
    system_prompt: str = ""


class AIChatOut(BaseModel):
    model: str
    provider: str
    endpoint: str
    result: dict[str, object]


class TranslationBlock(BaseModel):
    id: str = Field(pattern=r"^block-[0-9a-f]{16}$")
    text: str = Field(min_length=1, max_length=10_000)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def normalize_text(self) -> "TranslationBlock":
        self.text = self.text.strip()
        if not self.text:
            raise ValueError("翻译块不能为空")
        return self


class TranslationIn(BaseModel):
    source_id: int | None = Field(default=None, gt=0)
    text: str = Field(default="", max_length=100_000)
    blocks: list[TranslationBlock] = Field(default_factory=list, max_length=256)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_shape(self) -> "TranslationIn":
        self.text = self.text.strip()
        if self.text and self.blocks:
            raise ValueError("翻译请求不能同时包含正文和块")
        ids = [block.id for block in self.blocks]
        if len(ids) != len(set(ids)):
            raise ValueError("翻译块 ID 不能重复")
        if sum(len(block.text) for block in self.blocks) > 100_000:
            raise ValueError("翻译块总长度超过限制")
        return self


class TranslationOut(BaseModel):
    status: str
    translation: str = ""
    blocks: list[TranslationBlock] = Field(default_factory=list)
    model_version: str = ""
    updated_at: datetime | None = None


class AISummaryOut(BaseModel):
    object_type: str
    object_id: int
    status: str
    summary: str
    model_version: str = ""
    updated_at: datetime | None = None


class AssistantCitationOut(BaseModel):
    id: int
    title: str
    source_name: str
    published_at: datetime | None
    url: str


class AssistantOut(BaseModel):
    query: str
    answer: str
    model: str
    citations: list[AssistantCitationOut]


class UserStateOut(BaseModel):
    object_type: str
    object_id: int
    read_status: str
    read_later: bool
    starred: bool

    model_config = ConfigDict(from_attributes=True)


class ItemOut(BaseModel):
    id: int
    source_id: int
    source_name: str
    source_site_url: str = ""
    title: str
    title_translation: str = ""
    summary: str
    summary_translation: str = ""
    image_url: str = ""
    media_url: str = ""
    media_kind: str = ""
    media_duration: int = 0
    content_text: str
    reading_html: str | None = None
    body_source: str | None = None
    web_fetch_status: str | None = None
    content_translation: str = ""
    reading_translation_needed: bool = False
    url: str
    published_at: datetime | None
    read_status: str = "unread"
    read_later: bool = False
    starred: bool = False
    filtered: bool = False
    filter_rules: list[str] = Field(default_factory=list)
    uninterested: bool = False
    uninterested_reason: UninterestedReason | None = None
    uninterested_note: str | None = None
    uninterested_at: datetime | None = None


class FilterRulePreviewOut(BaseModel):
    count: int
    items: list[ItemOut] = Field(default_factory=list)


class FilteredItemsOut(BaseModel):
    count: int
    items: list[ItemOut] = Field(default_factory=list)


class UninterestedTargetOut(BaseModel):
    target_kind: Literal["event", "item"]
    event_uid: str | None = None
    current_revision_uid: str | None = None
    cluster_id: int | None = None
    item_id: int | None = None
    item_ids: list[int] = Field(default_factory=list)
    title: str
    summary: str = ""
    source_ids: list[int] = Field(default_factory=list)
    source_names: list[str] = Field(default_factory=list)
    media_type: str = "article"
    item_count: int = 1
    read_status: str = "unread"
    read_later: bool = False
    starred: bool = False
    reason: UninterestedReason | None = None
    note: str | None = None
    marked_at: datetime


class UninterestedTargetsOut(BaseModel):
    count: int
    items: list[UninterestedTargetOut] = Field(default_factory=list)


class EventRevisionSummaryOut(BaseModel):
    revision_uid: str
    revision_no: int
    title: str
    event_time: datetime | None
    evidence_fingerprint: str
    created_at: datetime


class EventEvidenceSourceOut(BaseModel):
    source_id: int
    name: str
    feed_url: str
    site_url: str
    media_type: str


class EventEvidenceSnapshotOut(BaseModel):
    evidence_uid: str
    identity_fingerprint: str
    version_uid: str
    version_fingerprint: str
    evidence_type: str
    role: str
    source: EventEvidenceSourceOut
    source_entry_id: int
    raw_entry_id: int
    raw_revision_no: int
    legacy_content_item_id: int | None
    legacy_content_item_id_snapshot: int
    fragment_fingerprint: str
    title: str
    url: str
    author: str
    published_at: datetime | None
    content: str
    reading_html: str | None = None
    body_source: str | None = None
    web_fetch_status: str | None = None


class EventRevisionDetailOut(EventRevisionSummaryOut):
    evidence: list[EventEvidenceSnapshotOut]


class SynthesisGenerateIn(BaseModel):
    provider: Literal["local", "openai_compatible"] | None = None


class SynthesisCitationOut(BaseModel):
    evidence_version_uid: str
    evidence_type: str
    role: str
    side: str
    source: EventEvidenceSourceOut
    legacy_content_item_id_snapshot: int
    title: str
    url: str
    published_at: datetime | None


class SynthesisBlockOut(BaseModel):
    block_uid: str
    position: int
    kind: Literal["summary", "fact", "viewpoint", "disagreement", "uncertainty"]
    body: str
    attribution: str
    citations: list[SynthesisCitationOut]


class SynthesisVersionOut(BaseModel):
    version_uid: str
    snapshot_uid: str
    target_revision_uid: str
    source_count: int
    provider: str
    model: str
    prompt_version: str
    schema_version: str
    generation_fingerprint: str
    snapshot_created_at: datetime
    created_at: datetime
    blocks: list[SynthesisBlockOut]


class GenerationTaskSourceOut(BaseModel):
    source_id: int
    source_name: str
    privacy_class: Literal["unclassified", "public", "private"]
    external_generation_allowed: bool
    source_policy_version: int


class GenerationAttemptOut(BaseModel):
    attempt_uid: str
    attempt_no: int
    status: Literal["pending", "running", "failed", "complete", "expired", "canceled"]
    input_tokens: int | None
    output_tokens: int | None
    started_at: datetime | None
    finished_at: datetime | None
    error: str
    runner_events_retention: Literal["not_recorded", "retained", "purged"]
    runner_events_purged_at: datetime | None


class GenerationTaskOut(BaseModel):
    request_uid: str
    task_type: str
    reason: str
    target_type: str
    target_uid: str
    provider: str
    model: str
    payload_retention: Literal["not_stored", "retained", "purged"]
    payload_purged_at: datetime | None
    status: GenerationTaskStatus
    privacy_status: Literal["local", "eligible", "blocked"]
    privacy_reason: str
    source_policy_fingerprint: str | None
    sources: list[GenerationTaskSourceOut]
    approval_status: Literal["awaiting", "approved", "consumed"]
    admission_status: Literal[
        "awaiting",
        "blocked_paused",
        "blocked_budget_unconfigured",
        "blocked_budget",
        "blocked_concurrency",
        "admitted",
        "canceled",
    ]
    admission_reason: str
    input_tokens_estimated: int | None
    output_tokens_reserved: int | None
    application_status: Literal["not_started", "pending", "applied", "failed"]
    result_currency: Literal["none", "current", "stale", "unverified"]
    can_reapply: bool
    result_uid: str | None
    result_fingerprint: str | None
    result_schema_version: str | None
    apply_attempt_count: int
    last_apply_error: str
    artifact_type: str | None
    artifact_uid: str | None
    attempts: list[GenerationAttemptOut]
    input_tokens: int | None
    output_tokens: int | None
    retry_count: int
    failure_class: Literal["transport", "validation", "canceled"] | None
    cancel_requested: bool
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error: str


class GenerationRetentionOut(BaseModel):
    status: Literal["never", "running", "succeeded", "failed"]
    last_run_at: datetime | None
    finished_at: datetime | None
    scanned_count: int
    deleted_count: int
    failure_reason: str


class GenerationControlOut(BaseModel):
    global_pause: bool
    auto_run: bool
    daily_budget_tokens: int | None
    input_estimator: Literal["unicode-codepoints-v1", "utf8-bytes-v1"]
    output_reserve_tokens: int
    day_timezone: str
    used_tokens: int
    reserved_tokens: int
    remaining_tokens: int | None
    requires_usage_review: bool


class GenerationControlPatch(BaseModel):
    global_pause: bool | None = None
    auto_run: bool | None = None
    daily_budget_tokens: int | None = None
    input_estimator: Literal["unicode-codepoints-v1", "utf8-bytes-v1"] | None = None
    output_reserve_tokens: int | None = None
    day_timezone: str | None = None


class EventSynthesisFreshnessOut(BaseModel):
    status: Literal["missing", "current", "unreviewed", "stale"]
    current_revision_uid: str
    covered_revision_uid: str | None
    reviewed_revision_uid: str | None
    new_source_count: int
    unreviewed_evidence_count: int
    unreviewed_source_count: int


class EventSynthesisStateOut(EventSynthesisFreshnessOut):
    target_revision_uid: str
    source_view_revision_uid: str
    source_count: int
    can_generate: bool
    default_view: Literal["synthesis", "source"]
    task_status: EventGenerationTaskStatus
    task: GenerationTaskOut | None = None
    current: SynthesisVersionOut | None


class EventUserStateReadOut(BaseModel):
    read_status: str
    read_later: bool
    starred: bool
    uninterested: bool = False
    uninterested_reason: UninterestedReason | None = None
    uninterested_note: str | None = None
    uninterested_at: datetime | None = None
    updated_at: datetime | None


class EventClusterProjectionOut(BaseModel):
    cluster_id: int | None
    cluster_id_snapshot: int
    clustering_run_id: str
    revision_uid: str
    reconciliation_kind: str
    rule_version: str | None
    before_evidence_fingerprint: str | None
    after_evidence_fingerprint: str
    projected_at: datetime


class EventLineageOut(BaseModel):
    lineage_uid: str | None
    relation_type: Literal[
        "continued", "split_from", "merged_from", "ambiguous_from"
    ]
    direction: Literal["self", "incoming", "outgoing"]
    source_event_uid: str
    target_event_uid: str
    source_revision_uid: str | None = None
    target_revision_uid: str | None = None
    clustering_run_id: str
    rule_version: str | None
    before_evidence_fingerprint: str
    after_evidence_fingerprint: str
    decision_reason: str | None
    recorded_at: datetime


class EventSuccessorOut(BaseModel):
    event_uid: str
    relation_type: Literal["split_from", "merged_from", "ambiguous_from"]
    status: str
    current_revision_uid: str
    title: str


class EventReadOut(BaseModel):
    event_uid: str
    status: str
    created_at: datetime
    superseded_at: datetime | None
    current_revision: EventRevisionSummaryOut
    seen_revision: EventRevisionSummaryOut | None
    current_revision_differs_from_seen: bool
    has_material_update: bool = False
    material_update_revision_uid: str | None = None
    user_state: EventUserStateReadOut
    current_projection: EventClusterProjectionOut | None
    revisions: list[EventRevisionSummaryOut]
    lineage: list[EventLineageOut] = Field(default_factory=list)
    successors: list[EventSuccessorOut] = Field(default_factory=list)
    synthesis: EventSynthesisStateOut


class EventReadabilitySourceIn(BaseModel):
    observed_revision_uid: str
    evidence_version_uid: str
    source_id: int = Field(gt=0)
    item_id: int = Field(gt=0)
    url: str


class EventReadabilitySourceOut(EventReadabilitySourceIn):
    event_uid: str
    title: str


class InteractionAuditOut(BaseModel):
    interaction_id: str
    operation_id: str
    target_kind: Literal["event", "object"]
    event_uid: str | None
    observed_revision_uid: str | None
    object_type: str | None
    object_id: int | None
    action: str
    set_value: object
    occurred_at: datetime
    recorded_at: datetime


class ClusterOut(BaseModel):
    id: int
    event_uid: str | None = None
    current_revision_uid: str | None = None
    seen_revision_uid: str | None = None
    current_revision_differs_from_seen: bool = False
    has_material_update: bool = False
    material_update_revision_uid: str | None = None
    title: str
    generated_title: str
    generated_title_translation: str = ""
    generated_summary: str
    generated_content: str
    citations: str
    model_version: str = ""
    prompt_version: str = ""
    first_seen_at: datetime | None
    last_seen_at: datetime | None
    item_count: int
    read_status: str = "unread"
    read_later: bool = False
    starred: bool = False
    uninterested: bool = False
    uninterested_reason: UninterestedReason | None = None
    uninterested_note: str | None = None
    uninterested_at: datetime | None = None
    items: list[ItemOut] = Field(default_factory=list)
    synthesis_freshness: EventSynthesisFreshnessOut | None = None
    synthesis: EventSynthesisStateOut | None = None


class EventSourceViewEvidenceOut(BaseModel):
    evidence_version_uid: str
    source_id: int
    legacy_content_item_id_snapshot: int


class ClusterDetailOut(ClusterOut):
    items: list[ItemOut]
    source_view_evidence: list[EventSourceViewEvidenceOut] = Field(
        default_factory=list
    )


class TopicGroupCreate(BaseModel):
    name: str = Field(max_length=240)
    query: TopicQuery
    description: str = ""


class TopicGroupPatch(BaseModel):
    name: str | None = Field(default=None, max_length=240)
    query: TopicQuery | None = None
    description: str | None = None


class TopicGroupOut(BaseModel):
    id: int
    name: str
    query: str
    description: str
    cluster_count: int = 0
    last_seen_at: datetime | None = None
    read_status: str = "unread"
    read_later: bool = False
    starred: bool = False


class TopicGroupDetailOut(TopicGroupOut):
    clusters: list[ClusterOut]


class SourceCitationOut(BaseModel):
    source_name: str
    title: str
    url: str
    published_at: str | None = None


class ReportSourceOut(SourceCitationOut):
    pass


class ReportCitationOut(BaseModel):
    citation_no: int | None = None
    cluster_id: int
    event_uid: str | None = None
    event_revision_uid: str | None = None
    title: str
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    sources: list[ReportSourceOut] = Field(default_factory=list)


class ReportOut(BaseModel):
    period: str
    title: str
    status: str
    body: str
    items: list[int]
    citations: list[ReportCitationOut]
    start: str
    end: str
    object_id: int
    read_status: str = "unread"
    read_later: bool = False
    starred: bool = False
    model_version: str = ""
    prompt_version: str = ""
    updated_at: str | None = None


class JobOut(BaseModel):
    mode: str
    imported: int | None = None
    embedded: int | None = None
    reclustered: int | None = None
    job_id: str | None = None


class FetchJobStatusOut(BaseModel):
    status: Literal["running", "complete", "failed"]
