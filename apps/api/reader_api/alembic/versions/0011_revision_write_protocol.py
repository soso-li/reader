"""Enforce Source Entry key shape and immutable contiguous Raw revisions."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0011_revision_write_protocol"
down_revision: str = "0010_raw_fingerprint_canonical"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    _validate_existing_contract()
    _create_key_constraints()
    _create_revision_constraints()
    _create_revision_triggers()


def _validate_existing_contract() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM source_entry_keys
                WHERE identity_kind NOT IN ('legacy', 'guid', 'url', 'fallback')
                   OR identity_key !~ '^(legacy|guid|url|fallback):[0-9a-f]{64}$'
                   OR split_part(identity_key, ':', 1) <> identity_kind
            ) THEN
                RAISE EXCEPTION
                    'source_entry_key_contract_invalid: identity kind/key 不符合协议';
            END IF;

            IF EXISTS (
                SELECT identity.id
                FROM source_entry_identities AS identity
                LEFT JOIN raw_entries AS raw
                  ON raw.source_entry_id = identity.id
                GROUP BY identity.id, identity.current_revision_no
                HAVING count(raw.id) = 0
                    OR min(raw.revision_no) <> 1
                    OR max(raw.revision_no) <> identity.current_revision_no
                    OR count(raw.id) <> max(raw.revision_no)
            ) THEN
                RAISE EXCEPTION
                    'source_entry_revision_contract_invalid: revision 必须从 1 连续且 current 指向最大版本';
            END IF;
        END
        $reader$
        """
    )


def _create_key_constraints() -> None:
    op.create_check_constraint(
        "ck_source_entry_key_kind",
        "source_entry_keys",
        "identity_kind IN ('legacy', 'guid', 'url', 'fallback')",
    )
    op.create_check_constraint(
        "ck_source_entry_key_format",
        "source_entry_keys",
        "identity_key ~ '^(legacy|guid|url|fallback):[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        "ck_source_entry_key_kind_prefix",
        "source_entry_keys",
        "split_part(identity_key, ':', 1) = identity_kind",
    )


def _create_revision_constraints() -> None:
    op.create_check_constraint(
        "ck_source_entry_current_revision_positive",
        "source_entry_identities",
        "current_revision_no >= 1",
    )
    op.create_check_constraint(
        "ck_raw_revision_positive",
        "raw_entries",
        "revision_no >= 1",
    )


def _create_revision_triggers() -> None:
    op.execute(
        """
        CREATE FUNCTION reader_guard_raw_revision_insert()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            identity_current INTEGER;
            maximum_revision INTEGER;
        BEGIN
            SELECT current_revision_no
            INTO identity_current
            FROM source_entry_identities
            WHERE id = NEW.source_entry_id
            FOR UPDATE;

            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'raw_revision_identity_missing: Source Entry Identity 不存在';
            END IF;

            SELECT max(revision_no)
            INTO maximum_revision
            FROM raw_entries
            WHERE source_entry_id = NEW.source_entry_id;

            IF maximum_revision IS NULL THEN
                IF NEW.revision_no <> 1 OR identity_current <> 1 THEN
                    RAISE EXCEPTION
                        'raw_revision_initial_invalid: 首个 revision 与 current 必须为 1';
                END IF;
            ELSE
                IF identity_current <> maximum_revision THEN
                    RAISE EXCEPTION
                        'raw_revision_current_stale: 插入前 current 必须指向现有最大 revision';
                END IF;
                IF NEW.revision_no <> maximum_revision + 1 THEN
                    RAISE EXCEPTION
                        'raw_revision_sequence_invalid: 新 revision 必须连续追加';
                END IF;
            END IF;
            RETURN NEW;
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reader_reject_raw_revision_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            RAISE EXCEPTION
                'raw_revision_immutable: Raw Entry revision 不允许更新或删除';
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE FUNCTION reader_assert_revision_consistency()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            checked_identity_id INTEGER;
            identity_current INTEGER;
            minimum_revision INTEGER;
            maximum_revision INTEGER;
            revision_count BIGINT;
        BEGIN
            IF TG_TABLE_NAME = 'raw_entries' THEN
                checked_identity_id := NEW.source_entry_id;
            ELSE
                checked_identity_id := NEW.id;
            END IF;

            SELECT identity.current_revision_no,
                   min(raw.revision_no),
                   max(raw.revision_no),
                   count(raw.id)
            INTO identity_current,
                 minimum_revision,
                 maximum_revision,
                 revision_count
            FROM source_entry_identities AS identity
            LEFT JOIN raw_entries AS raw
              ON raw.source_entry_id = identity.id
            WHERE identity.id = checked_identity_id
            GROUP BY identity.id, identity.current_revision_no;

            IF NOT FOUND
               OR revision_count = 0
               OR minimum_revision <> 1
               OR maximum_revision <> identity_current
               OR revision_count <> maximum_revision THEN
                RAISE EXCEPTION
                    'source_entry_revision_consistency_failed: revision 必须连续且 current 指向最大版本';
            END IF;
            RETURN NULL;
        END
        $reader$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_raw_revision_insert_guard
        BEFORE INSERT ON raw_entries
        FOR EACH ROW
        EXECUTE FUNCTION reader_guard_raw_revision_insert()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_raw_revision_immutable
        BEFORE UPDATE OR DELETE ON raw_entries
        FOR EACH ROW
        EXECUTE FUNCTION reader_reject_raw_revision_mutation()
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_raw_revision_consistency
        AFTER INSERT ON raw_entries
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION reader_assert_revision_consistency()
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_identity_revision_consistency
        AFTER INSERT OR UPDATE ON source_entry_identities
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION reader_assert_revision_consistency()
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Source Entry revision write protocol 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
