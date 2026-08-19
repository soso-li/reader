"""Merge the supported legacy schema upgrade paths."""

from __future__ import annotations

from collections.abc import Sequence


revision: str = "0002_legacy_schema_head"
down_revision: tuple[str, str] = (
    "0001_legacy_baseline",
    "0001_production_legacy",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "legacy schema head 不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
