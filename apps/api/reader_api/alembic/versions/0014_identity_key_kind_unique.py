"""Allow at most one identity key of each kind per Source Entry identity."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0014_identity_key_kind_unique"
down_revision: str = "0013_identity_key_mutation_lock"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT source_entry_id, identity_kind
                FROM source_entry_keys
                GROUP BY source_entry_id, identity_kind
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'source_entry_identity_kind_conflict: identity 的每种 key 最多保留一个';
            END IF;
        END
        $reader$
        """
    )
    op.create_unique_constraint(
        "uq_source_entry_key_identity_kind",
        "source_entry_keys",
        ["source_entry_id", "identity_kind"],
    )


def downgrade() -> None:
    raise RuntimeError(
        "Source Entry identity key kind uniqueness 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
