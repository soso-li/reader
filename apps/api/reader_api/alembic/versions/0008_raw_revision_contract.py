"""Backfill canonical payload fingerprints and close the revision contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
import re
import unicodedata

from alembic import op
import sqlalchemy as sa


revision: str = "0008_raw_revision_contract"
down_revision: str = "0007_normal_article_ingest"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def upgrade() -> None:
    if op.get_context().as_sql:
        _validate_offline_compatible_rows()
        _make_fingerprint_required()
        return

    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            """
            SELECT id, source_entry_id, revision_no, external_id, title, url,
                   author, published_at, raw_summary, raw_content,
                   payload_fingerprint
            FROM raw_entries
            ORDER BY source_entry_id, revision_no, id
            """
        )
    ).mappings().all()

    seen: dict[tuple[int, str], int] = {}
    updates: list[dict[str, object]] = []
    for row in rows:
        fingerprint = row["payload_fingerprint"] or _payload_fingerprint(row)
        if not isinstance(fingerprint, str) or not SHA256_HEX_RE.fullmatch(
            fingerprint
        ):
            raise RuntimeError(
                "raw_revision_invalid_fingerprint: "
                f"raw_entry={row['id']} fingerprint={fingerprint!r}"
            )
        key = (int(row["source_entry_id"]), fingerprint)
        duplicate_id = seen.get(key)
        if duplicate_id is not None:
            raise RuntimeError(
                "raw_revision_duplicate_canonical_fingerprint: "
                f"identity={key[0]} raw_entries={duplicate_id},{row['id']}"
            )
        seen[key] = int(row["id"])
        if row["payload_fingerprint"] is None:
            updates.append(
                {"raw_entry_id": int(row["id"]), "fingerprint": fingerprint}
            )

    if updates:
        connection.execute(
            sa.text(
                """
                UPDATE raw_entries
                SET payload_fingerprint = :fingerprint
                WHERE id = :raw_entry_id
                  AND payload_fingerprint IS NULL
                """
            ),
            updates,
        )
    _make_fingerprint_required()


def _make_fingerprint_required() -> None:
    op.alter_column(
        "raw_entries",
        "payload_fingerprint",
        existing_type=sa.String(length=64),
        nullable=False,
    )


def _validate_offline_compatible_rows() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE payload_fingerprint IS NULL
            ) THEN
                RAISE EXCEPTION
                    'raw_revision_offline_backfill_unsupported: 历史空 fingerprint 必须使用在线迁移入口补算';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries
                WHERE payload_fingerprint !~ '^[0-9a-f]{64}$'
            ) THEN
                RAISE EXCEPTION
                    'raw_revision_invalid_fingerprint: fingerprint 必须为小写 SHA-256 hex';
            END IF;

            IF EXISTS (
                SELECT 1
                FROM raw_entries
                GROUP BY source_entry_id, payload_fingerprint
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'raw_revision_duplicate_canonical_fingerprint: 同一 Source Entry 存在重复 canonical fingerprint';
            END IF;
        END
        $reader$
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Raw Entry revision contract 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )


def _payload_fingerprint(row: Mapping[str, object]) -> str:
    canonical_payload = json.dumps(
        {
            "author": _normalize_payload_text(row["author"]),
            "published_at": _canonical_utc_timestamp(row["published_at"]),
            "raw_content": _normalize_payload_text(row["raw_content"]),
            "raw_summary": _normalize_payload_text(row["raw_summary"]),
            "title": _normalize_payload_text(row["title"]),
            "url": _normalize_payload_text(row["url"]),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _normalize_payload_text(value: object) -> str:
    normalized = "" if value is None else str(value)
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    return unicodedata.normalize("NFC", normalized)


def _canonical_utc_timestamp(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, datetime):
        raise RuntimeError(
            "raw_revision_invalid_published_at: " f"value={value!r}"
        )
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
