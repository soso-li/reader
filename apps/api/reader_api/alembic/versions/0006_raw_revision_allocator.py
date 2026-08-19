"""Add idempotency uniqueness for canonical Raw Entry payloads."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0006_raw_revision_allocator"
down_revision: str = "0005_source_entry_backfill"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE payload_fingerprint IS NOT NULL
                GROUP BY source_entry_id, payload_fingerprint
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'raw_revision_duplicate_fingerprint: 同一 Source Entry 存在重复 payload fingerprint';
            END IF;
        END
        $reader$
        """
    )
    op.create_unique_constraint(
        "uq_raw_source_entry_fingerprint",
        "raw_entries",
        ["source_entry_id", "payload_fingerprint"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Raw Entry revision allocator migration 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
