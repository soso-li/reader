"""Require every persisted Raw Entry fingerprint to be canonical SHA-256 hex."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0009_raw_fingerprint_format"
down_revision: str = "0008_raw_revision_contract"
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
                WHERE payload_fingerprint !~ '^[0-9a-f]{64}$'
            ) THEN
                RAISE EXCEPTION
                    'raw_fingerprint_format_invalid: fingerprint 必须为小写 SHA-256 hex';
            END IF;
        END
        $reader$
        """
    )
    op.create_check_constraint(
        "ck_raw_payload_fingerprint_sha256",
        "raw_entries",
        sa.column("payload_fingerprint").regexp_match(r"^[0-9a-f]{64}$"),
    )


def downgrade() -> None:
    raise RuntimeError(
        "Raw Entry fingerprint format contract 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
