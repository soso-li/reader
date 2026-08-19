"""Require persisted fingerprints to match the canonical Raw Entry evidence."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0010_raw_fingerprint_canonical"
down_revision: str = "0009_raw_fingerprint_format"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CANONICAL_PAYLOAD_FINGERPRINT_SQL = r"""
encode(
    sha256(
        convert_to(
            '{"author":' ||
            to_json(
                normalize(
                    replace(replace(coalesce(author, ''), E'\r\n', E'\n'), E'\r', E'\n'),
                    NFC
                )
            )::text ||
            ',"published_at":' ||
            to_json(
                CASE
                    WHEN published_at IS NULL THEN ''
                    ELSE to_char(
                        published_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                    )
                END
            )::text ||
            ',"raw_content":' ||
            to_json(
                normalize(
                    replace(replace(coalesce(raw_content, ''), E'\r\n', E'\n'), E'\r', E'\n'),
                    NFC
                )
            )::text ||
            ',"raw_summary":' ||
            to_json(
                normalize(
                    replace(replace(coalesce(raw_summary, ''), E'\r\n', E'\n'), E'\r', E'\n'),
                    NFC
                )
            )::text ||
            ',"title":' ||
            to_json(
                normalize(
                    replace(replace(coalesce(title, ''), E'\r\n', E'\n'), E'\r', E'\n'),
                    NFC
                )
            )::text ||
            ',"url":' ||
            to_json(
                normalize(
                    replace(replace(coalesce(url, ''), E'\r\n', E'\n'), E'\r', E'\n'),
                    NFC
                )
            )::text ||
            '}',
            'UTF8'
        )
    ),
    'hex'
)
""".strip()


def upgrade() -> None:
    op.execute(
        sa.text(
            f"""
            DO $reader$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM raw_entries
                    WHERE payload_fingerprint <> (
                        {CANONICAL_PAYLOAD_FINGERPRINT_SQL}
                    )
                ) THEN
                    RAISE EXCEPTION
                        'raw_fingerprint_canonical_mismatch: fingerprint 与原始证据不一致';
                END IF;
            END
            $reader$
            """
        )
    )
    op.create_check_constraint(
        "ck_raw_payload_fingerprint_canonical",
        "raw_entries",
        f"payload_fingerprint = ({CANONICAL_PAYLOAD_FINGERPRINT_SQL})",
    )


def downgrade() -> None:
    raise RuntimeError(
        "Raw Entry canonical fingerprint contract 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
