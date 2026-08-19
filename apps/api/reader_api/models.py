from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    FetchedValue,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    column,
    event,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import TypeDecorator, UserDefinedType

from .db import Base
from .generation_types import GenerationFailureClass, GenerationRetryKind
from .media_types import SOURCE_MEDIA_TYPES


EVENT_EVIDENCE_TYPES = ("article", "social", "notification")
EVENT_EVIDENCE_ROLES = (
    "primary_source",
    "corroboration",
    "challenge",
    "opinion",
    "social_reaction",
    "material",
)
SYNTHESIS_BLOCK_KINDS = (
    "summary",
    "fact",
    "viewpoint",
    "disagreement",
    "uncertainty",
)
SYNTHESIS_CITATION_SIDE_MAX_LENGTH = 40
EVIDENCE_REVIEW_RESULTS = ("ordinary", "material", "uncertain")
DELETED_SOURCE_STATUS = "deleted"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class HalfVec(UserDefinedType):
    cache_ok = True

    def __init__(self, dimensions: int = 2560) -> None:
        self.dimensions = dimensions

    def get_col_spec(self, **kw: object) -> str:
        return f"halfvec({self.dimensions})"


class EmbeddingVectorType(TypeDecorator[str]):
    impl = Text
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(HalfVec())
        return dialect.type_descriptor(Text())


class Folder(Base):
    __tablename__ = "folders"
    __table_args__ = (
        CheckConstraint(
            column("media_type", String()).in_(SOURCE_MEDIA_TYPES),
            name="ck_folder_media_type",
        ),
        UniqueConstraint("media_type", "name", name="uq_folder_media_type_name"),
        UniqueConstraint("id", "media_type", name="uq_folder_id_media_type"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(240), nullable=False)
    media_type: Mapped[str] = mapped_column(String(32), nullable=False, default="article")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    sources: Mapped[list["Source"]] = relationship(back_populates="folder")


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class MaintenanceRun(Base):
    __tablename__ = "maintenance_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    operation_type: Mapped[str] = mapped_column(String(80), nullable=False)
    start_status: Mapped[str] = mapped_column(String(40), nullable=False, default="started")
    end_status: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    scanned_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failure_info: Mapped[str] = mapped_column(Text, nullable=False, default="")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Source(Base):
    __tablename__ = "sources"
    __table_args__ = (
        ForeignKeyConstraint(
            ["folder_id", "media_type"],
            ["folders.id", "folders.media_type"],
            name="fk_source_folder_media_type",
        ),
        CheckConstraint(
            column("media_type", String()).in_(SOURCE_MEDIA_TYPES),
            name="ck_source_media_type",
        ),
        CheckConstraint(
            column("privacy_class", String()).in_(("unclassified", "public", "private")),
            name="ck_source_privacy_class",
        ),
        CheckConstraint(
            "external_generation_allowed = false OR privacy_class = 'public'",
            name="ck_source_external_generation_public",
        ),
        CheckConstraint(
            "generation_policy_version >= 1",
            name="ck_source_generation_policy_version_positive",
        ),
        Index(
            "uq_sources_live_url",
            "url",
            unique=True,
            postgresql_where=text("status <> 'deleted'"),
            sqlite_where=text("status <> 'deleted'"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    folder_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    name: Mapped[str] = mapped_column(String(320), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    site_url: Mapped[str] = mapped_column(Text, default="")
    media_type: Mapped[str] = mapped_column(String(32), default="article")
    status: Mapped[str] = mapped_column(String(20), default="active")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    fetch_full_content: Mapped[bool] = mapped_column(Boolean, default=False)
    article_selector: Mapped[str | None] = mapped_column(Text, nullable=True)
    remove_selector: Mapped[str | None] = mapped_column(Text, nullable=True)
    privacy_class: Mapped[str] = mapped_column(
        String(20), nullable=False, default="unclassified", server_default="unclassified"
    )
    external_generation_allowed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    generation_policy_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1, server_default=text("1")
    )
    feed_trust_score: Mapped[float] = mapped_column(Float, default=0.0)
    last_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str] = mapped_column(Text, default="")
    fetch_etag: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetch_last_modified: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_successful_payload_hash: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    status_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=now_utc)

    folder: Mapped[Folder | None] = relationship(back_populates="sources")
    raw_entries: Mapped[list["RawEntry"]] = relationship(back_populates="source", cascade="all, delete-orphan")
    source_entry_identities: Mapped[list["SourceEntryIdentity"]] = relationship(
        back_populates="source"
    )


class SourceEntryIdentity(Base):
    __tablename__ = "source_entry_identities"
    __table_args__ = (
        UniqueConstraint(
            "id",
            "source_id",
            name="uq_source_entry_identity_id_source",
        ),
        CheckConstraint(
            "current_revision_no >= 1",
            name="ck_source_entry_current_revision_positive",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    current_revision_no: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    projection_pending: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)

    source: Mapped[Source] = relationship(back_populates="source_entry_identities")
    keys: Mapped[list["SourceEntryKey"]] = relationship(back_populates="source_entry")
    raw_entries: Mapped[list["RawEntry"]] = relationship(
        viewonly=True,
    )


class SourceEntryKey(Base):
    __tablename__ = "source_entry_keys"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_entry_id", "source_id"],
            ["source_entry_identities.id", "source_entry_identities.source_id"],
            name="fk_source_entry_key_identity_source",
        ),
        UniqueConstraint(
            "source_id",
            "identity_kind",
            "identity_key",
            name="uq_source_entry_key_source_kind_value",
        ),
        UniqueConstraint(
            "source_entry_id",
            "identity_kind",
            name="uq_source_entry_key_identity_kind",
        ),
        CheckConstraint(
            column("identity_kind", String()).in_(
                ("legacy", "guid", "url", "fallback")
            ),
            name="ck_source_entry_key_kind",
        ),
        CheckConstraint(
            column("identity_key", String()).regexp_match(
                r"^(legacy|guid|url|fallback):[0-9a-f]{64}$"
            ),
            name="ck_source_entry_key_format",
        ),
        CheckConstraint(
            "substr(identity_key, 1, length(identity_kind) + 1) = "
            "identity_kind || ':'",
            name="ck_source_entry_key_kind_prefix",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_entry_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    identity_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    identity_key: Mapped[str] = mapped_column(String(80), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)

    source_entry: Mapped[SourceEntryIdentity] = relationship(back_populates="keys")


class SourceEntryRelation(Base):
    __tablename__ = "source_entry_relations"
    __table_args__ = (
        UniqueConstraint(
            "source_entry_id",
            "canonical_source_entry_id",
            "relation_type",
            "rule_version",
            name="uq_source_entry_relation_rule",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_entry_id: Mapped[int] = mapped_column(
        ForeignKey("source_entry_identities.id"), nullable=False
    )
    canonical_source_entry_id: Mapped[int] = mapped_column(
        ForeignKey("source_entry_identities.id"), nullable=False
    )
    relation_type: Mapped[str] = mapped_column(String(40), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)
    rule_version: Mapped[str] = mapped_column(String(80), nullable=False, default="")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    source_entry: Mapped[SourceEntryIdentity] = relationship(
        foreign_keys=[source_entry_id]
    )
    canonical_source_entry: Mapped[SourceEntryIdentity] = relationship(
        foreign_keys=[canonical_source_entry_id]
    )


class RawEntry(Base):
    __tablename__ = "raw_entries"
    __table_args__ = (
        ForeignKeyConstraint(
            ["source_entry_id", "source_id"],
            ["source_entry_identities.id", "source_entry_identities.source_id"],
            name="fk_raw_entry_identity_source",
        ),
        UniqueConstraint(
            "source_entry_id",
            "revision_no",
            name="uq_raw_source_entry_revision",
        ),
        UniqueConstraint(
            "source_entry_id",
            "payload_fingerprint",
            name="uq_raw_source_entry_fingerprint",
        ),
        UniqueConstraint(
            "id",
            "source_entry_id",
            "source_id",
            "revision_no",
            name="uq_raw_event_evidence_reference",
        ),
        CheckConstraint(
            column("payload_fingerprint").regexp_match(r"^[0-9a-f]{64}$"),
            name="ck_raw_payload_fingerprint_sha256",
        ),
        CheckConstraint(
            "revision_no >= 1",
            name="ck_raw_revision_positive",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    source_entry_id: Mapped[int] = mapped_column(Integer, nullable=False)
    revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, default="")
    author: Mapped[str] = mapped_column(Text, default="")
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    raw_summary: Mapped[str] = mapped_column(Text, default="")
    raw_content: Mapped[str] = mapped_column(Text, default="")
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    source: Mapped[Source] = relationship(back_populates="raw_entries")
    source_entry: Mapped[SourceEntryIdentity] = relationship(
        foreign_keys=[source_entry_id],
    )
    document: Mapped["Document | None"] = relationship(back_populates="raw_entry")


class Document(Base):
    __tablename__ = "documents"
    __mapper_args__ = {"eager_defaults": False}
    __table_args__ = (
        CheckConstraint(
            """
            (
                reading_html IS NULL
                AND body_source IS NULL
                AND web_fetch_status IS NULL
            )
            OR
            (
                reading_html IS NOT NULL
                AND body_source IS NOT NULL
                AND web_fetch_status IS NOT NULL
                AND (
                    (body_source = 'rss' AND web_fetch_status IN ('not_requested', 'failed'))
                    OR
                    (body_source = 'webpage' AND web_fetch_status = 'succeeded')
                )
            )
            """,
            name="ck_document_reading_body_state",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    raw_entry_id: Mapped[int] = mapped_column(ForeignKey("raw_entries.id"), unique=True, nullable=False)
    document_type: Mapped[str] = mapped_column(String(32), default="normal_article")
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="")
    content_text: Mapped[str] = mapped_column(Text, default="")
    reading_html: Mapped[str | None] = mapped_column(
        Text, nullable=True, deferred=True, server_default=FetchedValue()
    )
    body_source: Mapped[str | None] = mapped_column(
        String(20), nullable=True, deferred=True, server_default=FetchedValue()
    )
    web_fetch_status: Mapped[str | None] = mapped_column(
        String(20), nullable=True, deferred=True, server_default=FetchedValue()
    )
    digest_score: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    raw_entry: Mapped[RawEntry] = relationship(back_populates="document")
    content_items: Mapped[list["ContentItem"]] = relationship(back_populates="document", cascade="all, delete-orphan")


def validate_document_reading_body_state(document: Document) -> None:
    state = (document.body_source, document.web_fetch_status)
    if document.reading_html is None and state == (None, None):
        return
    if document.reading_html is None or state not in {
        ("rss", "not_requested"),
        ("rss", "failed"),
        ("webpage", "succeeded"),
    }:
        raise ValueError("Document 正文状态组合无效")


@event.listens_for(Document, "before_insert")
@event.listens_for(Document, "before_update")
def _validate_document_reading_body_state(
    _mapper: object, _connection: object, document: Document
) -> None:
    validate_document_reading_body_state(document)


class ContentItem(Base):
    __tablename__ = "content_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("documents.id"), nullable=False)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="")
    content_text: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(Text, default="")
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_url: Mapped[str] = mapped_column(Text, default="")
    normalized_title: Mapped[str] = mapped_column(Text, default="")
    lsh_signature: Mapped[str] = mapped_column(Text, default="")
    media_url: Mapped[str] = mapped_column(Text, default="")
    media_kind: Mapped[str] = mapped_column(String(32), default="")
    media_duration: Mapped[int] = mapped_column(Integer, default=0)
    embedding_vector: Mapped[str | None] = mapped_column(EmbeddingVectorType(), nullable=True, default=None)
    embedding_model: Mapped[str] = mapped_column(String(120), default="")
    cluster_score: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    document: Mapped[Document] = relationship(back_populates="content_items")
    source: Mapped[Source] = relationship()
    cluster_items: Mapped[list["ClusterItem"]] = relationship(back_populates="content_item", cascade="all, delete-orphan")
    content_embeddings: Mapped[list["ContentEmbedding"]] = relationship(back_populates="content_item", cascade="all, delete-orphan")


class FilterRule(Base):
    __tablename__ = "filter_rules"
    __table_args__ = (
        CheckConstraint(
            column("match_type", String()).in_(("literal", "regex")),
            name="ck_filter_rule_match_type",
        ),
        CheckConstraint("length(trim(pattern)) > 0", name="ck_filter_rule_pattern_present"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int | None] = mapped_column(
        ForeignKey("sources.id", ondelete="CASCADE"), nullable=True
    )
    match_type: Mapped[str] = mapped_column(String(20), nullable=False, default="literal")
    pattern: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)

    source: Mapped[Source | None] = relationship()
    matches: Mapped[list["FilterMatch"]] = relationship(
        back_populates="rule", cascade="all, delete-orphan", passive_deletes=True
    )


class FilterMatch(Base):
    __tablename__ = "filter_matches"
    __table_args__ = (Index("ix_filter_matches_content_item_id", "content_item_id"),)

    rule_id: Mapped[int] = mapped_column(
        ForeignKey("filter_rules.id", ondelete="CASCADE"), primary_key=True
    )
    content_item_id: Mapped[int] = mapped_column(
        ForeignKey("content_items.id", ondelete="CASCADE"), primary_key=True
    )
    matched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)

    rule: Mapped[FilterRule] = relationship(back_populates="matches")
    content_item: Mapped[ContentItem] = relationship()


class ContentEmbedding(Base):
    __tablename__ = "content_embeddings"
    __table_args__ = (UniqueConstraint("content_item_id", "representation", "model", name="uq_content_embedding_representation"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    content_item_id: Mapped[int] = mapped_column(ForeignKey("content_items.id"), nullable=False)
    representation: Mapped[str] = mapped_column(String(40), nullable=False)
    model: Mapped[str] = mapped_column(String(120), default="")
    vector: Mapped[str | None] = mapped_column(EmbeddingVectorType(), nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    content_item: Mapped[ContentItem] = relationship(back_populates="content_embeddings")


class Cluster(Base):
    __tablename__ = "clusters"

    id: Mapped[int] = mapped_column(primary_key=True)
    cluster_key: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    generated_title: Mapped[str] = mapped_column(Text, default="")
    generated_summary: Mapped[str] = mapped_column(Text, default="")
    generated_content: Mapped[str] = mapped_column(Text, default="")
    citations: Mapped[str] = mapped_column(Text, default="[]")
    model_version: Mapped[str] = mapped_column(String(120), default="")
    prompt_version: Mapped[str] = mapped_column(String(120), default="")
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    items: Mapped[list["ClusterItem"]] = relationship(back_populates="cluster")


class ClusterItem(Base):
    __tablename__ = "cluster_items"
    __table_args__ = (UniqueConstraint("cluster_id", "content_item_id", name="uq_cluster_item"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    cluster_id: Mapped[int] = mapped_column(ForeignKey("clusters.id"), nullable=False)
    content_item_id: Mapped[int] = mapped_column(ForeignKey("content_items.id"), nullable=False)
    duplicate_score: Mapped[float] = mapped_column(Float, default=1.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    cluster: Mapped[Cluster] = relationship(back_populates="items")
    content_item: Mapped[ContentItem] = relationship(back_populates="cluster_items")


class Event(Base):
    __tablename__ = "events"
    __mapper_args__ = {"eager_defaults": False}
    __table_args__ = (
        CheckConstraint(
            column("status", String()).in_(("active", "superseded")),
            name="ck_event_status",
        ),
        CheckConstraint(
            "(status = 'active' AND superseded_at IS NULL) OR "
            "(status = 'superseded' AND superseded_at IS NOT NULL)",
            name="ck_event_superseded_time",
        ),
        ForeignKeyConstraint(
            ["id", "current_synthesis_version_id"],
            ["synthesis_versions.event_id", "synthesis_versions.id"],
            name="fk_event_current_synthesis_version",
            use_alter=True,
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["id", "reviewed_evidence_review_id"],
            ["evidence_reviews.event_id", "evidence_reviews.id"],
            name="fk_event_reviewed_evidence_review",
            use_alter=True,
            deferrable=True,
            initially="DEFERRED",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement="ignore_fk")
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    current_revision_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "event_revisions.id",
            name="fk_event_current_revision",
            use_alter=True,
            deferrable=True,
            initially="DEFERRED",
        ),
        nullable=True,
    )
    current_synthesis_version_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, deferred=True, server_default=FetchedValue()
    )
    reviewed_evidence_review_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, deferred=True, server_default=FetchedValue()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )
    superseded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class EventRevision(Base):
    __tablename__ = "event_revisions"
    __table_args__ = (
        UniqueConstraint(
            "event_id", "revision_no", name="uq_event_revision_number"
        ),
        UniqueConstraint("event_id", "id", name="uq_event_revision_event_id"),
        CheckConstraint("revision_no >= 1", name="ck_event_revision_positive"),
        CheckConstraint(
            "length(evidence_fingerprint) = 64",
            name="ck_event_revision_fingerprint_length",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    title_snapshot: Mapped[str] = mapped_column(Text, nullable=False, default="")
    event_time_snapshot: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EventEvidence(Base):
    __tablename__ = "event_evidence"
    __table_args__ = (
        UniqueConstraint(
            "identity_fingerprint", name="uq_event_evidence_identity_fingerprint"
        ),
        UniqueConstraint(
            "source_entry_id",
            "fragment_fingerprint",
            name="uq_event_evidence_source_fragment",
        ),
        UniqueConstraint(
            "id",
            "source_entry_id",
            "fragment_fingerprint",
            name="uq_event_evidence_parent_reference",
        ),
        CheckConstraint(
            "length(identity_fingerprint) = 64",
            name="ck_event_evidence_identity_fingerprint_length",
        ),
        CheckConstraint(
            "length(fragment_fingerprint) = 64",
            name="ck_event_evidence_fragment_fingerprint_length",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    identity_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    source_entry_id: Mapped[int] = mapped_column(
        ForeignKey("source_entry_identities.id", ondelete="RESTRICT"), nullable=False
    )
    fragment_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EventEvidenceVersion(Base):
    __tablename__ = "event_evidence_versions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["evidence_id", "source_entry_id", "fragment_fingerprint"],
            [
                "event_evidence.id",
                "event_evidence.source_entry_id",
                "event_evidence.fragment_fingerprint",
            ],
            name="fk_event_evidence_version_parent",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["raw_entry_id", "source_entry_id", "source_id", "raw_revision_no"],
            [
                "raw_entries.id",
                "raw_entries.source_entry_id",
                "raw_entries.source_id",
                "raw_entries.revision_no",
            ],
            name="fk_event_evidence_version_raw_revision",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "evidence_id",
            "version_fingerprint",
            name="uq_event_evidence_version_fingerprint",
        ),
        UniqueConstraint(
            "evidence_id", "id", name="uq_event_evidence_version_evidence_id"
        ),
        CheckConstraint(
            "raw_revision_no >= 1", name="ck_event_evidence_version_revision_positive"
        ),
        CheckConstraint(
            "length(version_fingerprint) = 64",
            name="ck_event_evidence_version_fingerprint_length",
        ),
        CheckConstraint(
            "length(fragment_fingerprint) = 64",
            name="ck_event_evidence_version_fragment_fingerprint_length",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    evidence_id: Mapped[int] = mapped_column(Integer, nullable=False)
    version_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_entry_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_entry_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_revision_no: Mapped[int] = mapped_column(Integer, nullable=False)
    legacy_content_item_id: Mapped[int | None] = mapped_column(
        ForeignKey("content_items.id", ondelete="SET NULL"), nullable=True
    )
    legacy_content_item_id_snapshot: Mapped[int] = mapped_column(
        Integer, nullable=False
    )
    fragment_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    title_snapshot: Mapped[str] = mapped_column(Text, nullable=False, default="")
    url_snapshot: Mapped[str] = mapped_column(Text, nullable=False, default="")
    author_snapshot: Mapped[str] = mapped_column(Text, nullable=False, default="")
    published_at_snapshot: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    content_snapshot: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EventRevisionEvidence(Base):
    __tablename__ = "event_revision_evidence"
    __table_args__ = (
        UniqueConstraint(
            "revision_id",
            "evidence_version_id",
            name="uq_event_revision_evidence_version",
        ),
        UniqueConstraint(
            "revision_id",
            "evidence_version_id",
            "evidence_type",
            "role",
            name="uq_event_revision_evidence_snapshot_role",
        ),
        CheckConstraint(
            column("evidence_type", String()).in_(EVENT_EVIDENCE_TYPES),
            name="ck_event_revision_evidence_type",
        ),
        CheckConstraint(
            column("role", String()).in_(EVENT_EVIDENCE_ROLES),
            name="ck_event_revision_evidence_role",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    revision_id: Mapped[int] = mapped_column(
        ForeignKey("event_revisions.id", ondelete="RESTRICT"), nullable=False
    )
    evidence_version_id: Mapped[int] = mapped_column(
        ForeignKey("event_evidence_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    evidence_type: Mapped[str] = mapped_column(String(24), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EvidenceSnapshot(Base):
    __tablename__ = "evidence_snapshots"
    __table_args__ = (
        ForeignKeyConstraint(
            ["event_id", "target_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_evidence_snapshot_target_revision",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "event_id", "target_revision_id", "id", name="uq_evidence_snapshot_owner"
        ),
        UniqueConstraint(
            "id", "target_revision_id", name="uq_evidence_snapshot_target"
        ),
        CheckConstraint(
            "length(source_coverage_fingerprint) = 64",
            name="ck_evidence_snapshot_source_fingerprint_length",
        ),
        CheckConstraint(
            "length(content_fingerprint) = 64",
            name="ck_evidence_snapshot_content_fingerprint_length",
        ),
        CheckConstraint(
            "policy_version <> ''", name="ck_evidence_snapshot_policy_nonempty"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    target_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_coverage_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    content_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(120), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EvidenceSnapshotMember(Base):
    __tablename__ = "evidence_snapshot_members"
    __table_args__ = (
        ForeignKeyConstraint(
            ["snapshot_id", "target_revision_id"],
            ["evidence_snapshots.id", "evidence_snapshots.target_revision_id"],
            name="fk_evidence_snapshot_member_snapshot",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["target_revision_id", "evidence_version_id", "evidence_type", "role"],
            [
                "event_revision_evidence.revision_id",
                "event_revision_evidence.evidence_version_id",
                "event_revision_evidence.evidence_type",
                "event_revision_evidence.role",
            ],
            name="fk_evidence_snapshot_member_revision_evidence",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "snapshot_id",
            "evidence_version_id",
            name="uq_evidence_snapshot_member_version",
        ),
        UniqueConstraint(
            "snapshot_id", "position", name="uq_evidence_snapshot_member_position"
        ),
        UniqueConstraint(
            "snapshot_id",
            "evidence_version_id",
            "evidence_type",
            "role",
            name="uq_evidence_snapshot_member_citation_target",
        ),
        CheckConstraint("position >= 1", name="ck_evidence_snapshot_member_position"),
        CheckConstraint(
            column("evidence_type", String()).in_(EVENT_EVIDENCE_TYPES),
            name="ck_evidence_snapshot_member_type",
        ),
        CheckConstraint(
            column("role", String()).in_(EVENT_EVIDENCE_ROLES),
            name="ck_evidence_snapshot_member_role",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_version_id: Mapped[int] = mapped_column(
        ForeignKey("event_evidence_versions.id", ondelete="RESTRICT"), nullable=False
    )
    evidence_type: Mapped[str] = mapped_column(String(24), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EvidenceReview(Base):
    __tablename__ = "evidence_reviews"
    __table_args__ = (
        ForeignKeyConstraint(
            ["event_id", "baseline_revision_id", "baseline_snapshot_id"],
            [
                "evidence_snapshots.event_id",
                "evidence_snapshots.target_revision_id",
                "evidence_snapshots.id",
            ],
            name="fk_evidence_review_baseline_snapshot",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["event_id", "target_revision_id", "target_snapshot_id"],
            [
                "evidence_snapshots.event_id",
                "evidence_snapshots.target_revision_id",
                "evidence_snapshots.id",
            ],
            name="fk_evidence_review_target_snapshot",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("event_id", "id", name="uq_evidence_review_event_id"),
        UniqueConstraint(
            "event_id",
            "comparison_fingerprint",
            name="uq_evidence_review_comparison_fingerprint",
        ),
        UniqueConstraint(
            "id", "target_snapshot_id", name="uq_evidence_review_target_snapshot"
        ),
        CheckConstraint(
            column("result", String()).in_(EVIDENCE_REVIEW_RESULTS),
            name="ck_evidence_review_result",
        ),
        CheckConstraint("trim(reason) <> ''", name="ck_evidence_review_reason"),
        CheckConstraint("provider <> ''", name="ck_evidence_review_provider"),
        CheckConstraint("model <> ''", name="ck_evidence_review_model"),
        CheckConstraint(
            "policy_version <> ''", name="ck_evidence_review_policy_version"
        ),
        CheckConstraint(
            "length(comparison_fingerprint) = 64",
            name="ck_evidence_review_comparison_fingerprint_length",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    baseline_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    baseline_snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    comparison_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    result: Mapped[str] = mapped_column(String(24), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(120), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EvidenceReviewCitation(Base):
    __tablename__ = "evidence_review_citations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["review_id", "target_snapshot_id"],
            ["evidence_reviews.id", "evidence_reviews.target_snapshot_id"],
            name="fk_evidence_review_citation_review",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["target_snapshot_id", "evidence_version_id", "evidence_type", "role"],
            [
                "evidence_snapshot_members.snapshot_id",
                "evidence_snapshot_members.evidence_version_id",
                "evidence_snapshot_members.evidence_type",
                "evidence_snapshot_members.role",
            ],
            name="fk_evidence_review_citation_snapshot_member",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "review_id",
            "evidence_version_id",
            name="uq_evidence_review_citation_evidence",
        ),
        UniqueConstraint(
            "review_id", "position", name="uq_evidence_review_citation_position"
        ),
        CheckConstraint(
            "position >= 1", name="ck_evidence_review_citation_position"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    review_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(24), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class SynthesisVersion(Base):
    __tablename__ = "synthesis_versions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["event_id", "target_revision_id", "snapshot_id"],
            [
                "evidence_snapshots.event_id",
                "evidence_snapshots.target_revision_id",
                "evidence_snapshots.id",
            ],
            name="fk_synthesis_version_snapshot_owner",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("event_id", "id", name="uq_synthesis_version_event_id"),
        UniqueConstraint(
            "event_id",
            "generation_fingerprint",
            name="uq_synthesis_version_generation_fingerprint",
        ),
        UniqueConstraint("id", "snapshot_id", name="uq_synthesis_version_snapshot"),
        CheckConstraint("source_count >= 2", name="ck_synthesis_version_source_count"),
        CheckConstraint(
            "provider <> ''", name="ck_synthesis_version_provider_nonempty"
        ),
        CheckConstraint("model <> ''", name="ck_synthesis_version_model_nonempty"),
        CheckConstraint(
            "prompt_version <> ''", name="ck_synthesis_version_prompt_nonempty"
        ),
        CheckConstraint(
            "schema_version <> ''", name="ck_synthesis_version_schema_nonempty"
        ),
        CheckConstraint(
            "length(generation_fingerprint) = 64",
            name="ck_synthesis_version_generation_fingerprint_length",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(120), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(120), nullable=False)
    generation_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class SynthesisBlock(Base):
    __tablename__ = "synthesis_blocks"
    __table_args__ = (
        UniqueConstraint(
            "synthesis_version_id", "position", name="uq_synthesis_block_position"
        ),
        UniqueConstraint(
            "id", "synthesis_version_id", name="uq_synthesis_block_version_id"
        ),
        CheckConstraint("position >= 1", name="ck_synthesis_block_position"),
        CheckConstraint(
            column("kind", String()).in_(SYNTHESIS_BLOCK_KINDS),
            name="ck_synthesis_block_kind",
        ),
        CheckConstraint("trim(body) <> ''", name="ck_synthesis_block_body_nonempty"),
        CheckConstraint(
            "kind <> 'viewpoint' OR trim(attribution) <> ''",
            name="ck_synthesis_block_viewpoint_attribution",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    synthesis_version_id: Mapped[int] = mapped_column(
        ForeignKey("synthesis_versions.id", ondelete="RESTRICT"), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    attribution: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class SynthesisCitation(Base):
    __tablename__ = "synthesis_citations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["block_id", "synthesis_version_id"],
            ["synthesis_blocks.id", "synthesis_blocks.synthesis_version_id"],
            name="fk_synthesis_citation_block",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["synthesis_version_id", "snapshot_id"],
            ["synthesis_versions.id", "synthesis_versions.snapshot_id"],
            name="fk_synthesis_citation_version_snapshot",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["snapshot_id", "evidence_version_id", "evidence_type", "role"],
            [
                "evidence_snapshot_members.snapshot_id",
                "evidence_snapshot_members.evidence_version_id",
                "evidence_snapshot_members.evidence_type",
                "evidence_snapshot_members.role",
            ],
            name="fk_synthesis_citation_snapshot_member",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "block_id", "evidence_version_id", name="uq_synthesis_citation_evidence"
        ),
        UniqueConstraint("block_id", "position", name="uq_synthesis_citation_position"),
        CheckConstraint("position >= 1", name="ck_synthesis_citation_position"),
        CheckConstraint("trim(side) <> ''", name="ck_synthesis_citation_side_nonempty"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    block_id: Mapped[int] = mapped_column(Integer, nullable=False)
    synthesis_version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_version_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_type: Mapped[str] = mapped_column(String(24), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    side: Mapped[str] = mapped_column(
        String(SYNTHESIS_CITATION_SIDE_MAX_LENGTH), nullable=False
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class ClusterEventProjection(Base):
    __tablename__ = "cluster_event_projections"
    __table_args__ = (
        Index(
            "ix_cluster_event_projections_cluster_snapshot_id",
            "cluster_id_snapshot",
            "id",
        ),
        Index(
            "ix_cluster_event_projections_event_id",
            "event_id",
            "id",
        ),
        ForeignKeyConstraint(
            ["event_id", "event_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_cluster_event_projection_revision",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "clustering_run_id",
            "cluster_anchor",
            "cluster_occurrence",
            name="uq_cluster_event_projection_run_cluster",
        ),
        CheckConstraint(
            "cluster_occurrence >= 1",
            name="ck_cluster_event_projection_occurrence",
        ),
        CheckConstraint(
            "length(cluster_anchor) = 64",
            name="ck_cluster_event_projection_anchor_length",
        ),
        CheckConstraint(
            column("reconciliation_kind", String()).in_(
                ("initial", "continued", "split", "merged", "ambiguous")
            ),
            name="ck_cluster_event_projection_reconciliation_kind",
        ),
        CheckConstraint(
            "length(after_evidence_fingerprint) = 64",
            name="ck_cluster_event_projection_after_fingerprint_length",
        ),
        CheckConstraint(
            "before_evidence_fingerprint IS NULL "
            "OR length(before_evidence_fingerprint) = 64",
            name="ck_cluster_event_projection_before_fingerprint_length",
        ),
        CheckConstraint(
            "(reconciliation_kind = 'initial' "
            " AND predecessor_projection_id IS NULL "
            " AND reconciliation_rule_version IS NULL "
            " AND before_evidence_fingerprint IS NULL) OR "
            "(reconciliation_kind IN ('continued', 'split') "
            " AND predecessor_projection_id IS NOT NULL "
            " AND reconciliation_rule_version IS NOT NULL "
            " AND reconciliation_rule_version <> '' "
            " AND before_evidence_fingerprint IS NOT NULL) OR "
            "(reconciliation_kind IN ('merged', 'ambiguous') "
            " AND predecessor_projection_id IS NULL "
            " AND reconciliation_rule_version IS NOT NULL "
            " AND reconciliation_rule_version <> '' "
            " AND before_evidence_fingerprint IS NULL)",
            name="ck_cluster_event_projection_reconciliation_shape",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    cluster_id: Mapped[int | None] = mapped_column(
        ForeignKey("clusters.id", ondelete="SET NULL"), nullable=True
    )
    cluster_id_snapshot: Mapped[int] = mapped_column(Integer, nullable=False)
    clustering_run_id: Mapped[str] = mapped_column(
        ForeignKey("clustering_runs.id", ondelete="RESTRICT"), nullable=False
    )
    cluster_anchor: Mapped[str] = mapped_column(String(64), nullable=False)
    cluster_occurrence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    event_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    predecessor_projection_id: Mapped[int | None] = mapped_column(
        ForeignKey("cluster_event_projections.id", ondelete="RESTRICT"),
        nullable=True,
    )
    reconciliation_kind: Mapped[str] = mapped_column(
        String(20), nullable=False, default="initial"
    )
    reconciliation_rule_version: Mapped[str | None] = mapped_column(
        String(120), nullable=True
    )
    before_evidence_fingerprint: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    after_evidence_fingerprint: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    projected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class ClusterCurrentEventProjection(Base):
    __tablename__ = "cluster_current_event_projections"
    __table_args__ = (
        UniqueConstraint(
            "projection_id",
            name="uq_cluster_current_event_projection_projection",
        ),
    )

    cluster_id: Mapped[int] = mapped_column(
        ForeignKey("clusters.id", ondelete="CASCADE"), primary_key=True
    )
    projection_id: Mapped[int] = mapped_column(
        ForeignKey("cluster_event_projections.id", ondelete="RESTRICT"),
        nullable=False,
    )


class EventLineage(Base):
    __tablename__ = "event_lineages"
    __table_args__ = (
        UniqueConstraint(
            "clustering_run_id",
            "relation_type",
            "source_event_id",
            "target_event_id",
            name="uq_event_lineage_relation",
        ),
        CheckConstraint(
            column("relation_type", String()).in_(
                ("split_from", "merged_from", "ambiguous_from")
            ),
            name="ck_event_lineage_relation_type",
        ),
        CheckConstraint(
            "source_event_id <> target_event_id",
            name="ck_event_lineage_distinct_events",
        ),
        CheckConstraint(
            "length(before_evidence_fingerprint) = 64",
            name="ck_event_lineage_before_fingerprint_length",
        ),
        CheckConstraint(
            "length(after_evidence_fingerprint) = 64",
            name="ck_event_lineage_after_fingerprint_length",
        ),
        CheckConstraint(
            "rule_version <> ''",
            name="ck_event_lineage_rule_version_nonempty",
        ),
        CheckConstraint(
            "decision_reason <> ''",
            name="ck_event_lineage_decision_reason_nonempty",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    clustering_run_id: Mapped[str] = mapped_column(
        ForeignKey("clustering_runs.id", ondelete="RESTRICT"), nullable=False
    )
    relation_type: Mapped[str] = mapped_column(String(24), nullable=False)
    source_event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    target_event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    rule_version: Mapped[str] = mapped_column(String(120), nullable=False)
    before_evidence_fingerprint: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    after_evidence_fingerprint: Mapped[str] = mapped_column(
        String(64), nullable=False
    )
    decision_reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class ClusteringRun(Base):
    __tablename__ = "clustering_runs"
    __table_args__ = (
        CheckConstraint(
            column("status", String()).in_(("started", "completed", "failed")),
            name="ck_clustering_run_status",
        ),
        CheckConstraint(
            "(status = 'started' AND completed_at IS NULL AND failed_at IS NULL) OR "
            "(status = 'completed' AND completed_at IS NOT NULL AND failed_at IS NULL) OR "
            "(status = 'failed' AND completed_at IS NULL AND failed_at IS NOT NULL)",
            name="ck_clustering_run_terminal_time",
        ),
        CheckConstraint(
            "(completed_at IS NULL OR completed_at >= started_at) AND "
            "(failed_at IS NULL OR failed_at >= started_at)",
            name="ck_clustering_run_terminal_time_order",
        ),
        CheckConstraint(
            "after_snapshot_finalized = (status = 'completed')",
            name="ck_clustering_run_snapshot_finalized",
        ),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    scope_type: Mapped[str] = mapped_column(String(80), nullable=False)
    scope_key: Mapped[str] = mapped_column(String(64), nullable=False)
    rule_version: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="started")
    failure_info: Mapped[str] = mapped_column(Text, nullable=False, default="")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    after_snapshot_finalized: Mapped[bool] = mapped_column(
        nullable=False, default=False
    )


class ClusteringRunScopeEvidence(Base):
    __tablename__ = "clustering_run_scope_evidence"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "evidence_anchor",
            "evidence_occurrence",
            name="uq_clustering_run_scope_evidence",
        ),
        CheckConstraint(
            "evidence_occurrence >= 1",
            name="ck_clustering_run_scope_occurrence",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("clustering_runs.id"), nullable=False
    )
    evidence_anchor: Mapped[str] = mapped_column(String(160), nullable=False)
    evidence_occurrence: Mapped[int] = mapped_column(nullable=False, default=1)


class ClusteringRunSnapshotSeal(Base):
    __tablename__ = "clustering_run_snapshot_seals"
    __table_args__ = (
        UniqueConstraint(
            "run_id", "snapshot_phase", name="uq_clustering_run_snapshot_seal"
        ),
        CheckConstraint(
            column("snapshot_phase", String()).in_(("before", "after")),
            name="ck_clustering_run_snapshot_seal_phase",
        ),
        CheckConstraint(
            "snapshot_row_count >= 0",
            name="ck_clustering_run_snapshot_seal_count",
        ),
        CheckConstraint(
            "length(snapshot_fingerprint) = 64",
            name="ck_clustering_run_snapshot_seal_fingerprint",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("clustering_runs.id"), nullable=False
    )
    snapshot_phase: Mapped[str] = mapped_column(String(16), nullable=False)
    snapshot_row_count: Mapped[int] = mapped_column(nullable=False)
    snapshot_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    sealed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class ClusteringRunMembership(Base):
    __tablename__ = "clustering_run_memberships"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "snapshot_phase",
            "cluster_anchor",
            "cluster_occurrence",
            "evidence_anchor",
            "evidence_occurrence",
            name="uq_clustering_run_membership_evidence",
        ),
        CheckConstraint(
            column("snapshot_phase", String()).in_(("before", "after")),
            name="ck_clustering_run_membership_phase",
        ),
        CheckConstraint(
            "cluster_occurrence >= 1 AND evidence_occurrence >= 1",
            name="ck_clustering_run_membership_occurrence",
        ),
        Index(
            "ix_clustering_run_membership_evidence_lookup",
            "run_id",
            "snapshot_phase",
            "evidence_anchor",
            "evidence_occurrence",
            "cluster_anchor",
            "cluster_occurrence",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("clustering_runs.id"), nullable=False
    )
    snapshot_phase: Mapped[str] = mapped_column(String(16), nullable=False)
    cluster_anchor: Mapped[str] = mapped_column(String(64), nullable=False)
    cluster_occurrence: Mapped[int] = mapped_column(nullable=False, default=1)
    evidence_anchor: Mapped[str] = mapped_column(String(160), nullable=False)
    evidence_occurrence: Mapped[int] = mapped_column(nullable=False, default=1)


class ClusteringRunProjectionPredecessor(Base):
    __tablename__ = "clustering_run_projection_predecessors"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "cluster_anchor",
            "cluster_occurrence",
            name="uq_clustering_run_projection_predecessor_cluster",
        ),
        UniqueConstraint(
            "run_id",
            "predecessor_projection_id",
            name="uq_clustering_run_projection_predecessor_mapping",
        ),
        CheckConstraint(
            "cluster_occurrence >= 1",
            name="ck_clustering_run_projection_predecessor_occurrence",
        ),
        CheckConstraint(
            "length(cluster_anchor) = 64",
            name="ck_clustering_run_projection_predecessor_anchor_length",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("clustering_runs.id", ondelete="RESTRICT"), nullable=False
    )
    cluster_anchor: Mapped[str] = mapped_column(String(64), nullable=False)
    cluster_occurrence: Mapped[int] = mapped_column(Integer, nullable=False)
    predecessor_projection_id: Mapped[int] = mapped_column(
        ForeignKey("cluster_event_projections.id", ondelete="RESTRICT"),
        nullable=False,
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class TopicGroup(Base):
    __tablename__ = "topic_groups"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(240), unique=True, nullable=False)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class UserState(Base):
    __tablename__ = "user_states"
    __table_args__ = (
        UniqueConstraint("object_type", "object_id", name="uq_user_state_object"),
        CheckConstraint(
            "object_type <> 'cluster'",
            name="ck_user_state_event_authority",
        ),
        CheckConstraint(
            "uninterested_reason IS NULL OR uninterested_reason IN "
            "('promotion', 'repetitive', 'topic', 'low_quality', 'other')",
            name="ck_user_state_uninterested_reason",
        ),
        CheckConstraint(
            "(uninterested = false AND uninterested_reason IS NULL "
            "AND uninterested_note IS NULL AND uninterested_at IS NULL) OR "
            "(uninterested = true AND uninterested_at IS NOT NULL "
            "AND ((uninterested_reason = 'other' "
            "AND length(trim(uninterested_note)) > 0) OR "
            "(uninterested_reason <> 'other' AND uninterested_note IS NULL) OR "
            "(uninterested_reason IS NULL AND uninterested_note IS NULL)))",
            name="ck_user_state_uninterested_shape",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    object_type: Mapped[str] = mapped_column(String(40), nullable=False)
    object_id: Mapped[int] = mapped_column(Integer, nullable=False)
    read_status: Mapped[str] = mapped_column(String(40), default="unread")
    read_later: Mapped[bool] = mapped_column(Boolean, default=False)
    starred: Mapped[bool] = mapped_column(Boolean, default=False)
    uninterested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    uninterested_reason: Mapped[str | None] = mapped_column(String(40), nullable=True)
    uninterested_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    uninterested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class MigrationBaseline(Base):
    __tablename__ = "migration_baselines"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key", name="uq_migration_baseline_idempotency_key"
        ),
        UniqueConstraint(
            "legacy_user_state_id", name="uq_migration_baseline_legacy_user_state"
        ),
        UniqueConstraint(
            "id", "resolved_event_id", name="uq_migration_baseline_event_reference"
        ),
        ForeignKeyConstraint(
            ["resolved_event_id", "resolved_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_migration_baseline_event_revision",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "length(idempotency_key) = 64",
            name="ck_migration_baseline_idempotency_key_length",
        ),
        CheckConstraint(
            "migration_version = 'legacy-user-state-baseline-v1'",
            name="ck_migration_baseline_version",
        ),
        CheckConstraint(
            "(legacy_object_type = 'cluster' "
            " AND resolved_event_id IS NOT NULL "
            " AND resolved_revision_id IS NOT NULL) OR "
            "(legacy_object_type <> 'cluster' "
            " AND resolved_event_id IS NULL "
            " AND resolved_revision_id IS NULL)",
            name="ck_migration_baseline_target_resolution",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    migration_version: Mapped[str] = mapped_column(String(120), nullable=False)
    legacy_user_state_id: Mapped[int] = mapped_column(Integer, nullable=False)
    legacy_object_type: Mapped[str] = mapped_column(String(40), nullable=False)
    legacy_object_id: Mapped[int] = mapped_column(Integer, nullable=False)
    resolved_event_id: Mapped[int | None] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=True
    )
    resolved_revision_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    read_status: Mapped[str] = mapped_column(String(40), nullable=False)
    read_later: Mapped[bool] = mapped_column(Boolean, nullable=False)
    starred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    source_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class EventUserState(Base):
    __tablename__ = "event_user_states"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_event_user_state_event"),
        UniqueConstraint("baseline_id", name="uq_event_user_state_baseline"),
        ForeignKeyConstraint(
            ["event_id", "seen_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_event_user_state_seen_revision",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["baseline_id", "event_id"],
            ["migration_baselines.id", "migration_baselines.resolved_event_id"],
            name="fk_event_user_state_baseline_event",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "uninterested_reason IS NULL OR uninterested_reason IN "
            "('promotion', 'repetitive', 'topic', 'low_quality', 'other')",
            name="ck_event_user_state_uninterested_reason",
        ),
        CheckConstraint(
            "(uninterested = false AND uninterested_reason IS NULL "
            "AND uninterested_note IS NULL AND uninterested_at IS NULL) OR "
            "(uninterested = true AND uninterested_at IS NOT NULL "
            "AND ((uninterested_reason = 'other' "
            "AND length(trim(uninterested_note)) > 0) OR "
            "(uninterested_reason <> 'other' AND uninterested_note IS NULL) OR "
            "(uninterested_reason IS NULL AND uninterested_note IS NULL)))",
            name="ck_event_user_state_uninterested_shape",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    baseline_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    seen_revision_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    read_status: Mapped[str] = mapped_column(String(40), nullable=False)
    read_later: Mapped[bool] = mapped_column(Boolean, nullable=False)
    starred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    uninterested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    uninterested_reason: Mapped[str | None] = mapped_column(String(40), nullable=True)
    uninterested_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    uninterested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class InteractionEvent(Base):
    __tablename__ = "interaction_events"
    __table_args__ = (
        UniqueConstraint("operation_id", name="uq_interaction_event_operation"),
        ForeignKeyConstraint(
            ["event_id", "observed_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_interaction_event_observed_revision",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            column("target_kind", String()).in_(("event", "legacy")),
            name="ck_interaction_event_target_kind",
        ),
        CheckConstraint(
            "operation_id <> ''", name="ck_interaction_event_operation_nonempty"
        ),
        CheckConstraint(
            "action <> ''", name="ck_interaction_event_action_nonempty"
        ),
        CheckConstraint(
            "(target_kind = 'event' "
            " AND event_id IS NOT NULL "
            " AND observed_revision_id IS NOT NULL "
            " AND legacy_object_type IS NULL "
            " AND legacy_object_id IS NULL) OR "
            "(target_kind = 'legacy' "
            " AND event_id IS NULL "
            " AND observed_revision_id IS NULL "
            " AND legacy_object_type IS NOT NULL "
            " AND legacy_object_id IS NOT NULL)",
            name="ck_interaction_event_target_shape",
        ),
    )

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid4())
    )
    operation_id: Mapped[str] = mapped_column(String(120), nullable=False)
    target_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    event_id: Mapped[int | None] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=True
    )
    observed_revision_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    object_type: Mapped[str | None] = mapped_column(
        "legacy_object_type", String(40), nullable=True
    )
    object_id: Mapped[int | None] = mapped_column(
        "legacy_object_id", Integer, nullable=True
    )
    action: Mapped[str] = mapped_column(String(40), nullable=False)
    set_value: Mapped[object] = mapped_column(JSON, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class FeedMetric(Base):
    __tablename__ = "feed_metrics"
    __table_args__ = (
        UniqueConstraint("source_id", name="uq_feed_metric_source"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    fetched_count: Mapped[int] = mapped_column(Integer, default=0)
    read_count: Mapped[int] = mapped_column(Integer, default=0)
    opened_count: Mapped[int] = mapped_column(Integer, default=0)
    starred_count: Mapped[int] = mapped_column(Integer, default=0)
    read_later_count: Mapped[int] = mapped_column(Integer, default=0)
    cluster_count: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class GenerationRequest(Base):
    __tablename__ = "generation_requests"
    __table_args__ = (
        CheckConstraint("length(request_fingerprint) = 64", name="ck_generation_request_fingerprint_length"),
        CheckConstraint("length(input_fingerprint) = 64", name="ck_generation_request_input_fingerprint_length"),
        CheckConstraint("trim(task_type) <> ''", name="ck_generation_request_task_type"),
        CheckConstraint("trim(reason) <> ''", name="ck_generation_request_reason"),
        CheckConstraint("trim(target_type) <> ''", name="ck_generation_request_target_type"),
        CheckConstraint("target_id >= 1", name="ck_generation_request_target_id"),
        CheckConstraint("trim(target_uid) <> ''", name="ck_generation_request_target_uid"),
        CheckConstraint("trim(provider) <> ''", name="ck_generation_request_provider"),
        CheckConstraint("trim(model) <> ''", name="ck_generation_request_model"),
        CheckConstraint("trim(prompt_version) <> ''", name="ck_generation_request_prompt_version"),
        CheckConstraint("trim(schema_version) <> ''", name="ck_generation_request_schema_version"),
        CheckConstraint(
            column("privacy_status", String()).in_(("local", "eligible", "blocked")),
            name="ck_generation_request_privacy_status",
        ),
        CheckConstraint(
            "(privacy_status = 'local' AND source_policy_fingerprint IS NULL AND privacy_reason = '') OR "
            "(privacy_status = 'eligible' AND length(source_policy_fingerprint) = 64 AND privacy_reason = '') OR "
            "(privacy_status = 'blocked' AND length(source_policy_fingerprint) = 64 AND trim(privacy_reason) <> '')",
            name="ck_generation_request_privacy_fields",
        ),
        CheckConstraint(
            "provider NOT IN ('legacy', 'openai_compatible') OR privacy_status <> 'local'",
            name="ck_generation_request_external_provider_privacy",
        ),
        Index(
            "ix_generation_request_target_input",
            "task_type",
            "target_type",
            "target_id",
            "input_fingerprint",
        ),
        Index("ix_generation_request_fingerprint", "request_fingerprint"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False, default=lambda: str(uuid4()))
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    input_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    task_type: Mapped[str] = mapped_column(String(80), nullable=False)
    reason: Mapped[str] = mapped_column(String(80), nullable=False)
    target_type: Mapped[str] = mapped_column(String(40), nullable=False)
    target_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_uid: Mapped[str] = mapped_column(String(36), nullable=False)
    provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model: Mapped[str] = mapped_column(String(120), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(120), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(120), nullable=False)
    privacy_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="local", server_default="local"
    )
    privacy_reason: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    source_policy_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)


class GenerationRequestSource(Base):
    __tablename__ = "generation_request_sources"
    __table_args__ = (
        CheckConstraint(
            column("privacy_class", String()).in_(("unclassified", "public", "private")),
            name="ck_generation_request_source_privacy_class",
        ),
        CheckConstraint(
            "source_policy_version >= 1",
            name="ck_generation_request_source_policy_version_positive",
        ),
        CheckConstraint(
            "external_generation_allowed = false OR privacy_class = 'public'",
            name="ck_generation_request_source_external_public",
        ),
    )

    request_id: Mapped[int] = mapped_column(
        ForeignKey("generation_requests.id", ondelete="RESTRICT"), primary_key=True
    )
    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="RESTRICT"), primary_key=True
    )
    source_name: Mapped[str] = mapped_column(String(320), nullable=False)
    privacy_class: Mapped[str] = mapped_column(String(20), nullable=False)
    external_generation_allowed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    source_policy_version: Mapped[int] = mapped_column(Integer, nullable=False)


class GenerationControl(Base):
    __tablename__ = "generation_controls"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_generation_control_singleton"),
        CheckConstraint(
            "daily_budget_tokens IS NULL OR daily_budget_tokens >= 0",
            name="ck_generation_control_daily_budget_nonnegative",
        ),
        CheckConstraint(
            "output_reserve_tokens >= 0",
            name="ck_generation_control_output_reserve_nonnegative",
        ),
        CheckConstraint(
            "input_estimator IN ('unicode-codepoints-v1', 'utf8-bytes-v1')",
            name="ck_generation_control_input_estimator",
        ),
        CheckConstraint(
            "trim(day_timezone) <> ''",
            name="ck_generation_control_day_timezone",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    global_pause: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    auto_run: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    daily_budget_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_estimator: Mapped[str] = mapped_column(
        String(40), nullable=False, default="unicode-codepoints-v1"
    )
    output_reserve_tokens: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    day_timezone: Mapped[str] = mapped_column(
        String(80), nullable=False, default="Asia/Shanghai"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class GenerationAdmission(Base):
    __tablename__ = "generation_admissions"
    __table_args__ = (
        CheckConstraint(
            column("approval_status", String()).in_(
                ("awaiting", "approved", "consumed")
            ),
            name="ck_generation_admission_approval_status",
        ),
        CheckConstraint(
            column("admission_status", String()).in_(
                (
                    "awaiting",
                    "blocked_paused",
                    "blocked_budget_unconfigured",
                    "blocked_budget",
                    "blocked_concurrency",
                    "admitted",
                    "canceled",
                )
            ),
            name="ck_generation_admission_status",
        ),
        CheckConstraint(
            "(approval_status = 'awaiting' AND approved_at IS NULL "
            " AND consumed_at IS NULL) OR "
            "(approval_status = 'approved' AND approved_at IS NOT NULL "
            " AND consumed_at IS NULL) OR "
            "(approval_status = 'consumed' AND approved_at IS NOT NULL "
            " AND consumed_at IS NOT NULL)",
            name="ck_generation_admission_approval_times",
        ),
        CheckConstraint(
            "input_tokens_estimated IS NULL OR input_tokens_estimated >= 0",
            name="ck_generation_admission_input_estimate",
        ),
        CheckConstraint(
            "output_tokens_reserved IS NULL OR output_tokens_reserved >= 0",
            name="ck_generation_admission_output_reserve",
        ),
        CheckConstraint(
            "(input_tokens_estimated IS NULL) = "
            "(output_tokens_reserved IS NULL)",
            name="ck_generation_admission_estimate_pair",
        ),
        CheckConstraint(
            "admission_status NOT LIKE 'blocked_%' "
            "OR trim(admission_reason) <> ''",
            name="ck_generation_admission_block_reason",
        ),
        CheckConstraint(
            "next_attempt_kind IS NULL OR next_attempt_kind IN ('initial', 'automatic', 'manual')",
            name="ck_generation_admission_next_attempt_kind",
        ),
    )

    request_id: Mapped[int] = mapped_column(
        ForeignKey("generation_requests.id", ondelete="RESTRICT"), primary_key=True
    )
    approval_status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="awaiting"
    )
    admission_status: Mapped[str] = mapped_column(
        String(40), nullable=False, default="awaiting"
    )
    admission_reason: Mapped[str] = mapped_column(Text, nullable=False, default="")
    next_attempt_kind: Mapped[GenerationRetryKind | None] = mapped_column(
        String(20), nullable=True, default="initial", server_default="initial"
    )
    canceled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    input_tokens_estimated: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens_reserved: Mapped[int | None] = mapped_column(Integer, nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    consumed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class GenerationRequestPayload(Base):
    __tablename__ = "generation_request_payloads"
    __table_args__ = (
        CheckConstraint("length(payload_fingerprint) = 64", name="ck_generation_request_payload_fingerprint_length"),
        CheckConstraint(
            "(payload_json IS NULL) = (purged_at IS NOT NULL)",
            name="ck_generation_request_payload_retention_state",
        ),
        Index(
            "ix_generation_request_payload_retention",
            "created_at",
            "request_id",
            postgresql_where=text("payload_json IS NOT NULL"),
            sqlite_where=text("payload_json IS NOT NULL"),
        ),
    )

    request_id: Mapped[int] = mapped_column(
        ForeignKey("generation_requests.id", ondelete="RESTRICT"), primary_key=True
    )
    payload_json: Mapped[dict[str, object] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    application_context_json: Mapped[dict[str, object] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)
    purged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class GenerationAttempt(Base):
    __tablename__ = "generation_attempts"
    __table_args__ = (
        UniqueConstraint("request_id", "attempt_no", name="uq_generation_attempt_number"),
        UniqueConstraint("id", "request_id", name="uq_generation_attempt_request"),
        CheckConstraint("attempt_no >= 1", name="ck_generation_attempt_positive"),
        CheckConstraint(
            column("retry_kind", String()).in_(("initial", "automatic", "manual")),
            name="ck_generation_attempt_retry_kind",
        ),
        CheckConstraint(
            column("status", String()).in_(("pending", "running", "failed", "complete", "canceled", "expired")),
            name="ck_generation_attempt_status",
        ),
        CheckConstraint(
            "(status = 'failed' AND failure_class IN ('transport', 'validation')) OR "
            "(status = 'expired' AND failure_class = 'transport') OR "
            "(status = 'canceled' AND failure_class = 'canceled') OR "
            "(status IN ('pending', 'running', 'complete') AND failure_class IS NULL)",
            name="ck_generation_attempt_failure_class",
        ),
        CheckConstraint(
            "(cancel_requested_at IS NULL AND status <> 'canceled') OR "
            "(cancel_requested_at IS NOT NULL AND status IN ('running', 'canceled'))",
            name="ck_generation_attempt_cancel_request",
        ),
        CheckConstraint(
            "(status = 'pending' AND started_at IS NULL AND finished_at IS NULL AND error = '') OR "
            "(status = 'running' AND started_at IS NOT NULL AND finished_at IS NULL AND error = '') OR "
            "(status = 'failed' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND trim(error) <> '') OR "
            "(status = 'canceled' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND trim(error) <> '') OR "
            "(status = 'expired' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND trim(error) <> '') OR "
            "(status = 'complete' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND error = '')",
            name="ck_generation_attempt_state_fields",
        ),
        CheckConstraint(
            "input_tokens_estimated IS NULL OR input_tokens_estimated >= 0",
            name="ck_generation_attempt_input_estimate",
        ),
        CheckConstraint(
            "output_tokens_reserved IS NULL OR output_tokens_reserved >= 0",
            name="ck_generation_attempt_output_reserve",
        ),
        CheckConstraint(
            "input_tokens_actual IS NULL OR input_tokens_actual >= 0",
            name="ck_generation_attempt_input_actual",
        ),
        CheckConstraint(
            "output_tokens_actual IS NULL OR output_tokens_actual >= 0",
            name="ck_generation_attempt_output_actual",
        ),
        CheckConstraint(
            "status IN ('failed', 'complete', 'canceled', 'expired') OR "
            "(input_tokens_actual IS NULL AND output_tokens_actual IS NULL)",
            name="ck_generation_attempt_actual_terminal_only",
        ),
        CheckConstraint(
            "status IN ('failed', 'complete', 'canceled', 'expired') OR "
            "runner_exit_code IS NULL",
            name="ck_generation_attempt_runner_exit_terminal_only",
        ),
        CheckConstraint(
            "runner_cli_version IS NULL OR "
            "(runner_id IS NOT NULL AND trim(runner_cli_version) <> '')",
            name="ck_generation_attempt_runner_cli_version",
        ),
        CheckConstraint(
            "runner_exit_code IS NULL OR runner_exit_code BETWEEN -255 AND 255",
            name="ck_generation_attempt_runner_exit_code",
        ),
        CheckConstraint(
            "(estimator_version IS NULL AND input_tokens_estimated IS NULL "
            " AND output_tokens_reserved IS NULL) OR "
            "(trim(estimator_version) <> '' AND input_tokens_estimated IS NOT NULL "
            " AND output_tokens_reserved IS NOT NULL)",
            name="ck_generation_attempt_estimate_shape",
        ),
        CheckConstraint(
            "(runner_id IS NULL AND runner_environment_id IS NULL "
            " AND lease_token_hash IS NULL AND lease_expires_at IS NULL "
            " AND last_heartbeat_at IS NULL) OR "
            "(runner_id IS NOT NULL AND trim(runner_id) <> '' "
            " AND runner_environment_id IS NOT NULL AND trim(runner_environment_id) <> '' "
            " AND lease_token_hash IS NOT NULL AND length(lease_token_hash) = 64 "
            " AND lease_expires_at IS NOT NULL "
            " AND last_heartbeat_at IS NOT NULL "
            " AND last_heartbeat_at <= lease_expires_at)",
            name="ck_generation_attempt_runner_lease_shape",
        ),
        Index(
            "uq_generation_attempt_single_active",
            "status",
            unique=True,
            postgresql_where=text("status = 'running'"),
            sqlite_where=text("status = 'running'"),
        ),
        Index(
            "uq_generation_attempt_single_automatic_retry",
            "request_id",
            unique=True,
            postgresql_where=text("retry_kind = 'automatic'"),
            sqlite_where=text("retry_kind = 'automatic'"),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False, default=lambda: str(uuid4()))
    request_id: Mapped[int] = mapped_column(ForeignKey("generation_requests.id", ondelete="RESTRICT"), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_kind: Mapped[GenerationRetryKind] = mapped_column(
        String(20), nullable=False, default="initial", server_default="initial"
    )
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    estimator_version: Mapped[str | None] = mapped_column(String(40), nullable=True)
    input_tokens_estimated: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens_reserved: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_tokens_actual: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens_actual: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    failure_class: Mapped[GenerationFailureClass | None] = mapped_column(
        String(20), nullable=True
    )
    runner_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    runner_environment_id: Mapped[str | None] = mapped_column(String(120), nullable=True)
    runner_cli_version: Mapped[str | None] = mapped_column(String(120), nullable=True)
    runner_exit_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lease_token_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cancel_requested_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)


class GenerationAttemptRunnerAudit(Base):
    __tablename__ = "generation_attempt_runner_audits"
    __table_args__ = (
        CheckConstraint(
            "length(events_fingerprint) = 64",
            name="ck_generation_attempt_runner_audit_fingerprint_length",
        ),
        CheckConstraint(
            "(events_json IS NULL) = (purged_at IS NOT NULL)",
            name="ck_generation_attempt_runner_audit_retention_state",
        ),
        Index(
            "ix_generation_attempt_runner_audit_retention",
            "created_at",
            "attempt_id",
            postgresql_where=text("events_json IS NOT NULL"),
            sqlite_where=text("events_json IS NOT NULL"),
        ),
    )

    attempt_id: Mapped[int] = mapped_column(
        ForeignKey("generation_attempts.id", ondelete="RESTRICT"), primary_key=True
    )
    events_json: Mapped[list[dict[str, object]] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    events_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )
    purged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class GenerationRunnerPresence(Base):
    __tablename__ = "generation_runner_presences"
    __table_args__ = (
        UniqueConstraint(
            "current_attempt_id", name="uq_generation_runner_current_attempt"
        ),
        CheckConstraint("id = 1", name="ck_generation_runner_presence_singleton"),
        CheckConstraint("trim(environment_id) <> ''", name="ck_generation_runner_environment"),
        CheckConstraint("trim(runner_id) <> ''", name="ck_generation_runner_id"),
        CheckConstraint("trim(runner_version) <> ''", name="ck_generation_runner_version"),
        CheckConstraint("trim(cli_version) <> ''", name="ck_generation_runner_cli_version"),
        CheckConstraint(
            column("status", String()).in_(("idle", "running")),
            name="ck_generation_runner_status",
        ),
        CheckConstraint(
            "(status = 'idle' AND current_attempt_id IS NULL) OR "
            "(status = 'running' AND current_attempt_id IS NOT NULL)",
            name="ck_generation_runner_state_fields",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    environment_id: Mapped[str] = mapped_column(String(120), nullable=False)
    runner_id: Mapped[str] = mapped_column(String(120), nullable=False)
    runner_version: Mapped[str] = mapped_column(String(120), nullable=False)
    cli_version: Mapped[str] = mapped_column(String(120), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="idle")
    current_attempt_id: Mapped[int | None] = mapped_column(
        ForeignKey("generation_attempts.id", ondelete="RESTRICT"),
        nullable=True,
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=now_utc
    )


class GenerationResult(Base):
    __tablename__ = "generation_results"
    __table_args__ = (
        ForeignKeyConstraint(
            ["attempt_id", "request_id"],
            ["generation_attempts.id", "generation_attempts.request_id"],
            name="fk_generation_result_attempt_request",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("attempt_id", name="uq_generation_result_attempt"),
        UniqueConstraint("id", "request_id", name="uq_generation_result_request"),
        CheckConstraint("length(payload_fingerprint) = 64", name="ck_generation_result_payload_fingerprint_length"),
        CheckConstraint("output_fingerprint IS NULL OR length(output_fingerprint) = 64", name="ck_generation_result_output_fingerprint_length"),
        CheckConstraint("schema_version IS NULL OR trim(schema_version) <> ''", name="ck_generation_result_schema_version"),
        CheckConstraint("input_tokens IS NULL OR input_tokens >= 0", name="ck_generation_result_input_tokens"),
        CheckConstraint("output_tokens IS NULL OR output_tokens >= 0", name="ck_generation_result_output_tokens"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(String(36), unique=True, nullable=False, default=lambda: str(uuid4()))
    request_id: Mapped[int] = mapped_column(Integer, nullable=False)
    attempt_id: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    output_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    schema_version: Mapped[str | None] = mapped_column(String(120), nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)


class GenerationApplication(Base):
    __tablename__ = "generation_applications"
    __table_args__ = (
        ForeignKeyConstraint(
            ["result_id", "request_id"],
            ["generation_results.id", "generation_results.request_id"],
            name="fk_generation_application_result_request",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("result_id", name="uq_generation_application_result"),
        CheckConstraint(
            column("status", String()).in_(("pending", "applied", "failed")),
            name="ck_generation_application_status",
        ),
        CheckConstraint(
            "(status = 'pending' AND artifact_type = '' AND artifact_id IS NULL AND error = '' AND applied_at IS NULL) OR "
            "(status = 'applied' AND trim(artifact_type) <> '' AND artifact_id IS NOT NULL AND error = '' AND applied_at IS NOT NULL) OR "
            "(status = 'failed' AND artifact_type = '' AND artifact_id IS NULL AND trim(error) <> '' AND applied_at IS NULL)",
            name="ck_generation_application_state_fields",
        ),
        CheckConstraint("apply_attempt_count >= 0", name="ck_generation_application_attempt_count"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    request_id: Mapped[int] = mapped_column(Integer, nullable=False)
    result_id: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="pending")
    artifact_type: Mapped[str] = mapped_column(String(40), nullable=False, default="")
    artifact_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    apply_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=now_utc)


class LLMTask(Base):
    __tablename__ = "llm_tasks"
    __table_args__ = (
        CheckConstraint(
            "input_fingerprint IS NULL OR length(input_fingerprint) = 64",
            name="ck_llm_task_input_fingerprint_length",
        ),
        CheckConstraint(
            "task_type NOT IN ('event-synthesis', 'evidence-review') "
            "OR status NOT IN ('pending', 'running') "
            "OR input_fingerprint IS NOT NULL",
            name="ck_llm_task_active_synthesis_fingerprint",
        ),
        Index(
            "uq_llm_task_active_input_fingerprint",
            "task_type",
            "object_type",
            "object_id",
            "input_fingerprint",
            unique=True,
            postgresql_where=text(
                "input_fingerprint IS NOT NULL AND status IN ('pending', 'running')"
            ),
            sqlite_where=text(
                "input_fingerprint IS NOT NULL AND status IN ('pending', 'running')"
            ),
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    task_type: Mapped[str] = mapped_column(String(80), nullable=False)
    provider: Mapped[str] = mapped_column(String(80), default="none")
    object_type: Mapped[str] = mapped_column(String(40), nullable=False)
    object_id: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(40), default="pending")
    prompt_version: Mapped[str] = mapped_column(String(120), default="")
    model_version: Mapped[str] = mapped_column(String(120), default="")
    input_fingerprint: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    result_json: Mapped[str] = mapped_column(Text, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
