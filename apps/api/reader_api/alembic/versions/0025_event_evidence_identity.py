"""Make Event Evidence identity independent of database allocation order."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0025_event_evidence_identity"
down_revision: str = "0024_event_evidence_integrity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM event_evidence
                GROUP BY source_entry_id, fragment_fingerprint
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'event_evidence_source_fragment_duplicate: '
                    '同一 Source Entry 片段存在多个 Evidence identity';
            END IF;
        END
        $reader$;
        """
    )
    op.create_unique_constraint(
        "uq_event_evidence_source_fragment",
        "event_evidence",
        ["source_entry_id", "fragment_fingerprint"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event Evidence 稳定逻辑身份约束不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
