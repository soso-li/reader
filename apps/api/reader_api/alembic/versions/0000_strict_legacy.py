"""Freeze the strict pre-v0.3 Reader schema found in the migration backup."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0000_strict_legacy"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


class HalfVec(sa.types.UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **_: object) -> str:
        return "halfvec(2560)"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "app_settings",
        sa.Column("key", sa.String(length=80), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("key"),
    )
    op.create_table(
        "clusters",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("cluster_key", sa.String(length=80), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("generated_title", sa.Text(), nullable=False),
        sa.Column("generated_summary", sa.Text(), nullable=False),
        sa.Column("generated_content", sa.Text(), nullable=False),
        sa.Column("citations", sa.Text(), nullable=False),
        sa.Column("model_version", sa.String(length=120), nullable=False),
        sa.Column("prompt_version", sa.String(length=120), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cluster_key", name="clusters_cluster_key_key"),
    )
    op.create_table(
        "folders",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="folders_name_key"),
    )
    op.create_table(
        "llm_tasks",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("task_type", sa.String(length=80), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("object_type", sa.String(length=40), nullable=False),
        sa.Column("object_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("prompt_version", sa.String(length=120), nullable=False),
        sa.Column("model_version", sa.String(length=120), nullable=False),
        sa.Column("result_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "topic_groups",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=240), nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="topic_groups_name_key"),
    )
    op.create_table(
        "user_states",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("object_type", sa.String(length=40), nullable=False),
        sa.Column("object_id", sa.Integer(), nullable=False),
        sa.Column("read_status", sa.String(length=40), nullable=False),
        sa.Column("read_later", sa.Boolean(), nullable=False),
        sa.Column("starred", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("object_type", "object_id", name="uq_user_state_object"),
    )
    op.create_table(
        "sources",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("folder_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=320), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("site_url", sa.Text(), nullable=False),
        sa.Column("media_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("fetch_full_content", sa.Boolean(), nullable=False),
        sa.Column("feed_trust_score", sa.Float(), nullable=False),
        sa.Column("last_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status_changed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["folder_id"], ["folders.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("url", name="sources_url_key"),
    )
    op.create_table(
        "feed_metrics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("fetched_count", sa.Integer(), nullable=False),
        sa.Column("read_count", sa.Integer(), nullable=False),
        sa.Column("opened_count", sa.Integer(), nullable=False),
        sa.Column("starred_count", sa.Integer(), nullable=False),
        sa.Column("read_later_count", sa.Integer(), nullable=False),
        sa.Column("cluster_count", sa.Integer(), nullable=False),
        sa.Column("duplicate_count", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "raw_entries",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_summary", sa.Text(), nullable=False),
        sa.Column("raw_content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_id", "external_id", name="uq_raw_source_external"),
    )
    op.create_table(
        "documents",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("raw_entry_id", sa.Integer(), nullable=False),
        sa.Column("document_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("content_text", sa.Text(), nullable=False),
        sa.Column("digest_score", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["raw_entry_id"], ["raw_entries.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("raw_entry_id", name="documents_raw_entry_id_key"),
    )
    op.create_table(
        "content_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("document_id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("content_text", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("canonical_url", sa.Text(), nullable=False),
        sa.Column("normalized_title", sa.Text(), nullable=False),
        sa.Column("lsh_signature", sa.Text(), nullable=False),
        sa.Column("media_url", sa.Text(), nullable=False),
        sa.Column("media_kind", sa.String(length=32), nullable=False),
        sa.Column("media_duration", sa.Integer(), nullable=False),
        sa.Column("embedding_vector", HalfVec(), nullable=True),
        sa.Column("embedding_model", sa.String(length=120), nullable=False),
        sa.Column("cluster_score", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["document_id"], ["documents.id"]),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "cluster_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("content_item_id", sa.Integer(), nullable=False),
        sa.Column("duplicate_score", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["cluster_id"], ["clusters.id"]),
        sa.ForeignKeyConstraint(["content_item_id"], ["content_items.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cluster_id", "content_item_id", name="uq_cluster_item"),
    )
    op.create_table(
        "content_embeddings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("content_item_id", sa.Integer(), nullable=False),
        sa.Column("representation", sa.String(length=40), nullable=False),
        sa.Column("model", sa.String(length=120), nullable=False),
        sa.Column("vector", HalfVec(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["content_item_id"], ["content_items.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "content_item_id",
            "representation",
            "model",
            name="uq_content_embedding_representation",
        ),
    )

    op.execute(
        """
        CREATE INDEX ix_content_items_fts
        ON content_items USING GIN (
          to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(summary,'') || ' ' || coalesce(content_text,''))
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_sources_name_fts
        ON sources USING GIN (to_tsvector('simple', coalesce(name,'')))
        """
    )
    op.execute(
        """
        CREATE INDEX ix_clusters_fts
        ON clusters USING GIN (
          to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(generated_title,'') || ' ' || coalesce(generated_summary,'') || ' ' || coalesce(generated_content,''))
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_content_items_embedding_hnsw
        ON content_items USING hnsw (embedding_vector halfvec_cosine_ops)
        WHERE embedding_vector IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_content_embeddings_zh_hnsw
        ON content_embeddings USING hnsw (vector halfvec_cosine_ops)
        WHERE vector IS NOT NULL AND representation = 'zh_canonical'
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "legacy baseline 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
