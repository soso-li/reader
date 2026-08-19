"""Allow one source-provided external ID across Source Entry generations."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0007_normal_article_ingest"
down_revision: str = "0006_raw_revision_allocator"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if op.get_context().as_sql:
        op.drop_constraint(
            "uq_raw_source_external",
            "raw_entries",
            type_="unique",
        )
        return

    matching_constraints = [
        constraint
        for constraint in sa.inspect(op.get_bind()).get_unique_constraints(
            "raw_entries"
        )
        if constraint["column_names"] == ["source_id", "external_id"]
    ]
    if len(matching_constraints) != 1:
        raise RuntimeError(
            "raw_entries(source_id, external_id) 唯一约束数量异常："
            f"expected=1 actual={len(matching_constraints)}"
        )
    constraint_name = matching_constraints[0]["name"]
    if not constraint_name:
        raise RuntimeError(
            "raw_entries(source_id, external_id) 唯一约束缺少可删除的名称"
        )
    op.drop_constraint(
        constraint_name,
        "raw_entries",
        type_="unique",
    )


def downgrade() -> None:
    raise RuntimeError(
        "normal article 不可变版本迁移不支持原地 downgrade；生产回滚必须恢复迁移前备份"
    )
