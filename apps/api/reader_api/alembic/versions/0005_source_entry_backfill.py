"""Backfill one legacy Source Entry identity for every existing Raw Entry."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0005_source_entry_backfill"
down_revision: str = "0004_source_entry_expand"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    _validate_existing_compatibility_rows()
    _backfill_legacy_rows()
    _validate_backfill_result()

    op.create_unique_constraint(
        "uq_raw_source_entry_revision",
        "raw_entries",
        ["source_entry_id", "revision_no"],
    )
    op.alter_column(
        "raw_entries",
        "source_entry_id",
        existing_type=sa.Integer(),
        nullable=False,
    )
    op.alter_column(
        "raw_entries",
        "revision_no",
        existing_type=sa.Integer(),
        nullable=False,
    )


def _validate_existing_compatibility_rows() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE (source_entry_id IS NULL) <> (revision_no IS NULL)
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_partial_revision: source_entry_id 与 revision_no 必须同时为空或同时有值';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE source_entry_id IS NOT NULL
                  AND revision_no <> 1
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_invalid_revision: 兼容写入只能包含 revision 1';
            END IF;
        END
        $reader$
        """
    )
    _validate_assigned_rows()


def _backfill_legacy_rows() -> None:
    op.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('source_entry_identities', 'id'),
            greatest(coalesce((SELECT max(id) FROM source_entry_identities), 0), 1),
            EXISTS (SELECT 1 FROM source_entry_identities)
        )
        """
    )
    op.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('source_entry_keys', 'id'),
            greatest(coalesce((SELECT max(id) FROM source_entry_keys), 0), 1),
            EXISTS (SELECT 1 FROM source_entry_keys)
        )
        """
    )
    op.execute(
        """
        CREATE TEMPORARY TABLE source_entry_backfill_map (
            raw_entry_id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            identity_id INTEGER NOT NULL UNIQUE,
            identity_key VARCHAR(80) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
        ) ON COMMIT DROP
        """
    )
    op.execute(
        """
        INSERT INTO source_entry_backfill_map (
            raw_entry_id,
            source_id,
            identity_id,
            identity_key,
            created_at
        )
        SELECT
            raw.id,
            raw.source_id,
            nextval(
                pg_get_serial_sequence('source_entry_identities', 'id')
            )::INTEGER,
            'legacy:' || encode(
                sha256(convert_to(raw.external_id, 'UTF8')),
                'hex'
            ),
            coalesce(raw.fetched_at, CURRENT_TIMESTAMP)
        FROM raw_entries AS raw
        WHERE raw.source_entry_id IS NULL
        ORDER BY raw.id
        """
    )
    op.execute(
        """
        INSERT INTO source_entry_identities (
            id,
            source_id,
            current_revision_no,
            projection_pending,
            created_at
        )
        SELECT
            identity_id,
            source_id,
            1,
            false,
            created_at
        FROM source_entry_backfill_map
        ORDER BY raw_entry_id
        """
    )
    op.execute(
        """
        INSERT INTO source_entry_keys (
            source_entry_id,
            source_id,
            identity_kind,
            identity_key,
            created_at
        )
        SELECT
            identity_id,
            source_id,
            'legacy',
            identity_key,
            created_at
        FROM source_entry_backfill_map
        ORDER BY raw_entry_id
        """
    )
    op.execute(
        """
        UPDATE raw_entries AS raw
        SET source_entry_id = mapping.identity_id,
            revision_no = 1
        FROM source_entry_backfill_map AS mapping
        WHERE raw.id = mapping.raw_entry_id
          AND raw.source_entry_id IS NULL
        """
    )


def _validate_backfill_result() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE source_entry_id IS NULL
                   OR revision_no IS NULL
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_null_result: 回填后仍存在空 identity 或 revision';
            END IF;
        END
        $reader$
        """
    )
    _validate_assigned_rows()


def _validate_assigned_rows() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE source_entry_id IS NOT NULL
                GROUP BY source_entry_id, revision_no
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_duplicate_revision: 同一 Source Entry Identity 存在重复 revision_no';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE source_entry_id IS NOT NULL
                GROUP BY source_entry_id
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_shared_identity: identity 未保持一对一';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries AS raw
                JOIN source_entry_identities AS identity
                  ON identity.id = raw.source_entry_id
                 AND identity.source_id = raw.source_id
                WHERE raw.source_entry_id IS NOT NULL
                  AND identity.current_revision_no <> raw.revision_no
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_current_mismatch: current_revision_no 与 Raw Entry 不一致';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries AS raw
                WHERE raw.source_entry_id IS NOT NULL
                  AND (
                      SELECT count(*)
                      FROM source_entry_keys AS entry_key
                      WHERE entry_key.source_entry_id = raw.source_entry_id
                        AND entry_key.source_id = raw.source_id
                        AND entry_key.identity_kind = 'legacy'
                  ) <> 1
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_legacy_key_count: identity 必须恰好拥有一个 legacy key';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries AS raw
                WHERE raw.source_entry_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM source_entry_keys AS entry_key
                      WHERE entry_key.source_entry_id = raw.source_entry_id
                        AND entry_key.source_id = raw.source_id
                        AND entry_key.identity_kind = 'legacy'
                        AND entry_key.identity_key =
                            'legacy:' || encode(
                                sha256(convert_to(raw.external_id, 'UTF8')),
                                'hex'
                            )
                  )
            ) THEN
                RAISE EXCEPTION
                    'source_entry_backfill_legacy_key_mismatch: legacy key 与 external ID 不匹配';
            END IF;
        END
        $reader$
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Source Entry legacy backfill 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
