"""Freeze sanitized reading HTML on new Event Evidence Versions."""

import sqlalchemy as sa
from alembic import op


revision: str = "0074_event_evidence_reading_html"
down_revision: str | None = "0073_unified_saved_state"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "event_evidence_versions",
        sa.Column("reading_html_snapshot", sa.Text(), nullable=True),
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_event_evidence_version_guard()
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
                       NEW.content_snapshot, NEW.reading_html_snapshot, NEW.created_at)
                   IS NOT DISTINCT FROM
                   ROW(OLD.id, OLD.uid, OLD.evidence_id, OLD.version_fingerprint,
                       OLD.raw_entry_id, OLD.source_entry_id, OLD.source_id,
                       OLD.raw_revision_no, OLD.legacy_content_item_id_snapshot,
                       OLD.fragment_fingerprint, OLD.title_snapshot, OLD.url_snapshot,
                       OLD.author_snapshot, OLD.published_at_snapshot,
                       OLD.content_snapshot, OLD.reading_html_snapshot, OLD.created_at) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'event_history_immutable: Event Evidence Version 不可修改或删除';
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError("0074 不可原地降级；请恢复迁移前备份")
