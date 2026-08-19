"""Tighten stable Event Evidence identity after the frozen initial schema."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0024_event_evidence_integrity"
down_revision: str = "0023_event_projection"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Issue #21 has not run yet. Refuse to reinterpret any Event history that
    # might have been written by the frozen 0023 fingerprint formula.
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (SELECT 1 FROM events)
               OR EXISTS (SELECT 1 FROM event_revisions)
               OR EXISTS (SELECT 1 FROM event_evidence)
               OR EXISTS (SELECT 1 FROM event_evidence_versions)
               OR EXISTS (SELECT 1 FROM event_revision_evidence)
               OR EXISTS (SELECT 1 FROM cluster_event_projections) THEN
                RAISE EXCEPTION
                    'event_projection_integrity_requires_empty_graph: '
                    '0024 只允许在 #21 回填前的空 Event 图上执行';
            END IF;
        END
        $reader$;
        """
    )

    op.alter_column(
        "event_evidence",
        "fragment_fingerprint",
        existing_type=sa.String(length=64),
        nullable=False,
    )
    op.alter_column(
        "event_evidence_versions",
        "fragment_fingerprint",
        existing_type=sa.String(length=64),
        nullable=False,
    )
    op.create_unique_constraint(
        "uq_event_evidence_parent_reference",
        "event_evidence",
        ["id", "source_entry_id", "fragment_fingerprint"],
    )
    op.drop_constraint(
        "event_evidence_versions_evidence_id_fkey",
        "event_evidence_versions",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_event_evidence_version_parent",
        "event_evidence_versions",
        "event_evidence",
        ["evidence_id", "source_entry_id", "fragment_fingerprint"],
        ["id", "source_entry_id", "fragment_fingerprint"],
        ondelete="RESTRICT",
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_event_revision_fingerprint_guard()
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
                                       link.evidence_type || '|' || link.role || '|' ||
                                       version.version_fingerprint,
                                       E'\\n'
                                       ORDER BY link.evidence_type, link.role,
                                                version.version_fingerprint
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
                RAISE EXCEPTION
                    'event_revision_fingerprint_mismatch: '
                    'Revision Evidence 与 fingerprint 不一致';
            END IF;
            RETURN NULL;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event Evidence 稳定身份约束不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
