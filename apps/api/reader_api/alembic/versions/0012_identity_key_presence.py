"""Require every persisted Source Entry Identity to retain an identity key."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0012_identity_key_presence"
down_revision: str = "0011_revision_write_protocol"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM source_entry_identities AS identity
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM source_entry_keys AS entry_key
                    WHERE entry_key.source_entry_id = identity.id
                      AND entry_key.source_id = identity.source_id
                )
            ) THEN
                RAISE EXCEPTION
                    'source_entry_identity_key_missing: identity 必须至少保留一个 key';
            END IF;
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reader_require_identity_key(checked_identity_id INTEGER)
        RETURNS void
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM source_entry_identities AS identity
                WHERE identity.id = checked_identity_id
                  AND NOT EXISTS (
                      SELECT 1
                      FROM source_entry_keys AS entry_key
                      WHERE entry_key.source_entry_id = identity.id
                        AND entry_key.source_id = identity.source_id
                  )
            ) THEN
                RAISE EXCEPTION
                    'source_entry_identity_key_missing: identity 必须至少保留一个 key';
            END IF;
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reader_assert_identity_key_presence()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF TG_TABLE_NAME = 'source_entry_identities' THEN
                PERFORM reader_require_identity_key(NEW.id);
            ELSIF TG_OP = 'DELETE' THEN
                PERFORM reader_require_identity_key(OLD.source_entry_id);
            ELSIF TG_OP = 'UPDATE' THEN
                PERFORM reader_require_identity_key(OLD.source_entry_id);
                IF NEW.source_entry_id <> OLD.source_entry_id THEN
                    PERFORM reader_require_identity_key(NEW.source_entry_id);
                END IF;
            ELSE
                PERFORM reader_require_identity_key(NEW.source_entry_id);
            END IF;
            RETURN NULL;
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_identity_key_presence
        AFTER INSERT ON source_entry_identities
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION reader_assert_identity_key_presence()
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_source_entry_key_presence
        AFTER INSERT OR UPDATE OR DELETE ON source_entry_keys
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION reader_assert_identity_key_presence()
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Source Entry identity key presence contract 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
