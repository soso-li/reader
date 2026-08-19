"""Merge the parallel privacy and generation admission revisions."""

from __future__ import annotations


revision: str = "0057_privacy_admission_merge"
down_revision: tuple[str, str] = (
    "0055_source_generation_privacy",
    "0056_generation_admission",
)
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError(
        "P0.4 隐私与 admission 合并契约不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
