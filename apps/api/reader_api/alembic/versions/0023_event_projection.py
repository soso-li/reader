"""Add stable Event, Evidence, Revision, and Cluster projection identity."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0023_event_projection"
down_revision: str = "0022_terminal_time_order"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


UUID_CHECK = (
    "uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)
SHA256_CHECK = "{column} ~ '^[0-9a-f]{{64}}$'"


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_raw_event_evidence_reference",
        "raw_entries",
        ["id", "source_entry_id", "source_id", "revision_no"],
    )
    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("current_revision_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(UUID_CHECK, name="ck_event_uid_uuid"),
        sa.CheckConstraint(
            "status IN ('active', 'superseded')", name="ck_event_status"
        ),
        sa.CheckConstraint(
            "(status = 'active' AND superseded_at IS NULL) OR "
            "(status = 'superseded' AND superseded_at IS NOT NULL)",
            name="ck_event_superseded_time",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_events_uid"),
    )
    op.create_table(
        "event_revisions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("revision_no", sa.Integer(), nullable=False),
        sa.Column("evidence_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("title_snapshot", sa.Text(), server_default="", nullable=False),
        sa.Column("event_time_snapshot", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_event_revision_uid_uuid"),
        sa.CheckConstraint("revision_no >= 1", name="ck_event_revision_positive"),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="evidence_fingerprint"),
            name="ck_event_revision_fingerprint_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["event_id"], ["events.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_event_revisions_uid"),
        sa.UniqueConstraint(
            "event_id", "revision_no", name="uq_event_revision_number"
        ),
        sa.UniqueConstraint(
            "event_id",
            "evidence_fingerprint",
            name="uq_event_revision_fingerprint",
        ),
        sa.UniqueConstraint("event_id", "id", name="uq_event_revision_event_id"),
    )
    op.create_foreign_key(
        "fk_event_current_revision",
        "events",
        "event_revisions",
        ["id", "current_revision_id"],
        ["event_id", "id"],
        ondelete="RESTRICT",
        deferrable=True,
        initially="DEFERRED",
    )
    op.create_table(
        "event_evidence",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("identity_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("source_entry_id", sa.Integer(), nullable=False),
        sa.Column("fragment_fingerprint", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_event_evidence_uid_uuid"),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="identity_fingerprint"),
            name="ck_event_evidence_identity_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "fragment_fingerprint IS NULL OR "
            + SHA256_CHECK.format(column="fragment_fingerprint"),
            name="ck_event_evidence_fragment_fingerprint_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["source_entry_id"],
            ["source_entry_identities.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_event_evidence_uid"),
        sa.UniqueConstraint(
            "identity_fingerprint", name="uq_event_evidence_identity_fingerprint"
        ),
    )
    op.create_table(
        "event_evidence_versions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uid", sa.String(length=36), nullable=False),
        sa.Column("evidence_id", sa.Integer(), nullable=False),
        sa.Column("version_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("raw_entry_id", sa.Integer(), nullable=False),
        sa.Column("source_entry_id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("raw_revision_no", sa.Integer(), nullable=False),
        sa.Column("legacy_content_item_id", sa.Integer(), nullable=True),
        sa.Column("legacy_content_item_id_snapshot", sa.Integer(), nullable=False),
        sa.Column("fragment_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("title_snapshot", sa.Text(), server_default="", nullable=False),
        sa.Column("url_snapshot", sa.Text(), server_default="", nullable=False),
        sa.Column("author_snapshot", sa.Text(), server_default="", nullable=False),
        sa.Column("published_at_snapshot", sa.DateTime(timezone=True), nullable=True),
        sa.Column("content_snapshot", sa.Text(), server_default="", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_event_evidence_version_uid_uuid"),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="version_fingerprint"),
            name="ck_event_evidence_version_fingerprint_sha256",
        ),
        sa.CheckConstraint(
            "raw_revision_no >= 1",
            name="ck_event_evidence_version_revision_positive",
        ),
        sa.CheckConstraint(
            "fragment_fingerprint IS NULL OR "
            + SHA256_CHECK.format(column="fragment_fingerprint"),
            name="ck_event_evidence_version_fragment_fingerprint_sha256",
        ),
        sa.ForeignKeyConstraint(
            ["evidence_id"], ["event_evidence.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["legacy_content_item_id"],
            ["content_items.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
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
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uid", name="uq_event_evidence_versions_uid"),
        sa.UniqueConstraint(
            "evidence_id",
            "version_fingerprint",
            name="uq_event_evidence_version_fingerprint",
        ),
        sa.UniqueConstraint(
            "evidence_id", "id", name="uq_event_evidence_version_evidence_id"
        ),
    )
    op.create_table(
        "event_revision_evidence",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("revision_id", sa.Integer(), nullable=False),
        sa.Column("evidence_version_id", sa.Integer(), nullable=False),
        sa.Column("evidence_type", sa.String(length=24), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "evidence_type IN ('article', 'social', 'notification')",
            name="ck_event_revision_evidence_type",
        ),
        sa.CheckConstraint(
            "role IN ('primary_source', 'corroboration', 'challenge', "
            "'opinion', 'social_reaction', 'material')",
            name="ck_event_revision_evidence_role",
        ),
        sa.ForeignKeyConstraint(
            ["revision_id"], ["event_revisions.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["evidence_version_id"],
            ["event_evidence_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "revision_id",
            "evidence_version_id",
            name="uq_event_revision_evidence_version",
        ),
    )
    op.create_table(
        "cluster_event_projections",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=True),
        sa.Column("cluster_id_snapshot", sa.Integer(), nullable=False),
        sa.Column("clustering_run_id", sa.String(length=36), nullable=False),
        sa.Column("cluster_anchor", sa.String(length=64), nullable=False),
        sa.Column("cluster_occurrence", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("event_revision_id", sa.Integer(), nullable=False),
        sa.Column(
            "projected_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="cluster_anchor"),
            name="ck_cluster_event_projection_anchor_sha256",
        ),
        sa.CheckConstraint(
            "cluster_occurrence >= 1",
            name="ck_cluster_event_projection_occurrence",
        ),
        sa.ForeignKeyConstraint(
            ["cluster_id"], ["clusters.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["clustering_run_id"],
            ["clustering_runs.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["event_id"], ["events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["event_id", "event_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_cluster_event_projection_revision",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "clustering_run_id",
            "cluster_anchor",
            "cluster_occurrence",
            name="uq_cluster_event_projection_run_cluster",
        ),
    )
    op.create_index(
        "ix_cluster_event_projections_cluster",
        "cluster_event_projections",
        ["cluster_id", "id"],
    )

    op.execute(
        """
        CREATE FUNCTION reader_event_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            old_revision_no integer;
            new_revision_no integer;
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'event_immutable: Event 不可删除';
            END IF;
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.uid IS DISTINCT FROM OLD.uid
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'event_identity_immutable: Event 身份与创建时间不可修改';
            END IF;
            IF OLD.status = 'superseded' AND NEW.status <> 'superseded' THEN
                RAISE EXCEPTION 'event_lifecycle_terminal: superseded Event 不可恢复 active';
            END IF;
            IF OLD.current_revision_id IS NOT NULL
               AND NEW.current_revision_id IS DISTINCT FROM OLD.current_revision_id THEN
                SELECT revision_no INTO old_revision_no
                FROM event_revisions WHERE id = OLD.current_revision_id;
                SELECT revision_no INTO new_revision_no
                FROM event_revisions
                WHERE id = NEW.current_revision_id AND event_id = NEW.id;
                IF new_revision_no IS NULL OR new_revision_no <= old_revision_no THEN
                    RAISE EXCEPTION 'event_revision_order: current revision 只能单调推进';
                END IF;
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_event_lifecycle
        BEFORE UPDATE OR DELETE ON events
        FOR EACH ROW EXECUTE FUNCTION reader_event_lifecycle_guard();

        CREATE FUNCTION reader_event_revision_sequence_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            expected_revision_no integer;
        BEGIN
            PERFORM 1 FROM events WHERE id = NEW.event_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'event_revision_event_missing: Event 不存在';
            END IF;
            SELECT COALESCE(max(revision_no), 0) + 1
            INTO expected_revision_no
            FROM event_revisions
            WHERE event_id = NEW.event_id;
            IF NEW.revision_no <> expected_revision_no THEN
                RAISE EXCEPTION 'event_revision_sequence: 期望 revision_no %，实际 %',
                    expected_revision_no, NEW.revision_no;
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_event_revision_sequence
        BEFORE INSERT ON event_revisions
        FOR EACH ROW EXECUTE FUNCTION reader_event_revision_sequence_guard();

        CREATE FUNCTION reader_event_current_revision_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            current_revision integer;
        BEGIN
            SELECT revision.id INTO current_revision
            FROM events event
            JOIN event_revisions revision
              ON revision.id = event.current_revision_id
             AND revision.event_id = event.id
            WHERE event.id = NEW.id;
            IF current_revision IS NULL THEN
                RAISE EXCEPTION 'event_current_revision_required: Event 必须引用自身 Revision';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE CONSTRAINT TRIGGER trg_event_current_revision
        AFTER INSERT OR UPDATE ON events
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_event_current_revision_guard();

        CREATE FUNCTION reader_event_immutable_row()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            RAISE EXCEPTION 'event_history_immutable: % 不可修改或删除', TG_TABLE_NAME;
        END
        $reader$;

        CREATE TRIGGER trg_event_revision_immutable
        BEFORE UPDATE OR DELETE ON event_revisions
        FOR EACH ROW EXECUTE FUNCTION reader_event_immutable_row();

        CREATE TRIGGER trg_event_evidence_immutable
        BEFORE UPDATE OR DELETE ON event_evidence
        FOR EACH ROW EXECUTE FUNCTION reader_event_immutable_row();

        CREATE FUNCTION reader_event_evidence_version_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF TG_OP = 'UPDATE'
               AND OLD.legacy_content_item_id IS NOT NULL
               AND NEW.legacy_content_item_id IS NULL
               AND ROW(NEW.id, NEW.uid, NEW.evidence_id, NEW.version_fingerprint,
                       NEW.raw_entry_id, NEW.source_entry_id, NEW.source_id,
                       NEW.raw_revision_no, NEW.legacy_content_item_id_snapshot,
                       NEW.fragment_fingerprint, NEW.title_snapshot, NEW.url_snapshot,
                       NEW.author_snapshot, NEW.published_at_snapshot,
                       NEW.content_snapshot, NEW.created_at)
                   IS NOT DISTINCT FROM
                   ROW(OLD.id, OLD.uid, OLD.evidence_id, OLD.version_fingerprint,
                       OLD.raw_entry_id, OLD.source_entry_id, OLD.source_id,
                       OLD.raw_revision_no, OLD.legacy_content_item_id_snapshot,
                       OLD.fragment_fingerprint, OLD.title_snapshot, OLD.url_snapshot,
                       OLD.author_snapshot, OLD.published_at_snapshot,
                       OLD.content_snapshot, OLD.created_at) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'event_history_immutable: Event Evidence Version 不可修改或删除';
        END
        $reader$;

        CREATE TRIGGER trg_event_evidence_version_immutable
        BEFORE UPDATE OR DELETE ON event_evidence_versions
        FOR EACH ROW EXECUTE FUNCTION reader_event_evidence_version_guard();

        CREATE TRIGGER trg_event_revision_evidence_immutable
        BEFORE UPDATE OR DELETE ON event_revision_evidence
        FOR EACH ROW EXECUTE FUNCTION reader_event_immutable_row();

        CREATE FUNCTION reader_event_revision_fingerprint_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            target_revision_id integer;
            expected_fingerprint text;
            actual_fingerprint text;
        BEGIN
            IF TG_TABLE_NAME = 'event_revisions' THEN
                target_revision_id := NEW.id;
                expected_fingerprint := NEW.evidence_fingerprint;
            ELSE
                target_revision_id := COALESCE(NEW.revision_id, OLD.revision_id);
                SELECT evidence_fingerprint INTO expected_fingerprint
                FROM event_revisions WHERE id = target_revision_id;
            END IF;
            SELECT encode(
                       sha256(
                           convert_to(
                               COALESCE(
                                   string_agg(
                                       link.evidence_type || '|' || link.role || '|' || version.uid,
                                       E'\\n'
                                       ORDER BY link.evidence_type, link.role, version.uid
                                   ),
                                   ''
                               ),
                               'UTF8'
                           )
                       ),
                       'hex'
                   )
            INTO actual_fingerprint
            FROM event_revision_evidence link
            JOIN event_evidence_versions version
              ON version.id = link.evidence_version_id
            WHERE link.revision_id = target_revision_id;
            IF expected_fingerprint IS DISTINCT FROM actual_fingerprint THEN
                RAISE EXCEPTION 'event_revision_fingerprint_mismatch: Revision Evidence 与 fingerprint 不一致';
            END IF;
            RETURN NULL;
        END
        $reader$;

        CREATE CONSTRAINT TRIGGER trg_event_revision_fingerprint
        AFTER INSERT ON event_revisions
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_event_revision_fingerprint_guard();

        CREATE CONSTRAINT TRIGGER trg_event_revision_evidence_fingerprint
        AFTER INSERT OR UPDATE OR DELETE ON event_revision_evidence
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION reader_event_revision_fingerprint_guard();

        CREATE FUNCTION reader_cluster_event_projection_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_completed boolean;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT status = 'completed' AND after_snapshot_finalized
                INTO run_completed
                FROM clustering_runs WHERE id = NEW.clustering_run_id;
                IF run_completed IS DISTINCT FROM true THEN
                    RAISE EXCEPTION 'event_projection_requires_completed_run: 投影只能引用完成且封印的 Clustering Run';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'UPDATE'
               AND OLD.cluster_id IS NOT NULL
               AND NEW.cluster_id IS NULL
               AND ROW(NEW.id, NEW.cluster_id_snapshot, NEW.clustering_run_id,
                       NEW.cluster_anchor, NEW.cluster_occurrence, NEW.event_id,
                       NEW.event_revision_id, NEW.projected_at)
                   IS NOT DISTINCT FROM
                   ROW(OLD.id, OLD.cluster_id_snapshot, OLD.clustering_run_id,
                       OLD.cluster_anchor, OLD.cluster_occurrence, OLD.event_id,
                       OLD.event_revision_id, OLD.projected_at) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'event_projection_immutable: Cluster projection mapping 不可修改或删除';
        END
        $reader$;

        CREATE TRIGGER trg_cluster_event_projection_guard
        BEFORE INSERT OR UPDATE OR DELETE ON cluster_event_projections
        FOR EACH ROW EXECUTE FUNCTION reader_cluster_event_projection_guard();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event/Revision/Evidence 历史不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
