"""Serialize identity key mutations before checking key presence."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0013_identity_key_mutation_lock"
down_revision: str = "0012_identity_key_presence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE FUNCTION reader_lock_identity_for_key_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            checked_identity_ids INTEGER[];
        BEGIN
            IF TG_OP = 'DELETE' THEN
                checked_identity_ids := ARRAY[OLD.source_entry_id];
            ELSIF TG_OP = 'UPDATE' THEN
                checked_identity_ids := ARRAY[
                    OLD.source_entry_id,
                    NEW.source_entry_id
                ];
            ELSE
                checked_identity_ids := ARRAY[NEW.source_entry_id];
            END IF;

            PERFORM identity.id
            FROM source_entry_identities AS identity
            WHERE identity.id = ANY(checked_identity_ids)
            ORDER BY identity.id
            FOR UPDATE;

            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_source_entry_key_identity_lock
        BEFORE INSERT OR UPDATE OR DELETE ON source_entry_keys
        FOR EACH ROW
        EXECUTE FUNCTION reader_lock_identity_for_key_mutation()
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Source Entry identity key mutation lock 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
