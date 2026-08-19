"""Merge the parallel W5 reliability contracts."""

from __future__ import annotations


revision: str = "0062_w5_contract_merge"
down_revision: tuple[str, str] = (
    "0060_generation_cancel_retry",
    "0061_result_apply_contract",
)
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError("禁止原地 downgrade；请恢复迁移前备份")
