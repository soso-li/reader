"""Keep set-based ambiguous topology audits off nested-loop plans."""

from __future__ import annotations

from alembic import op


revision: str = "0065_ambiguous_topology_plan"
down_revision: str | None = "0064_ambiguous_topology_set"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        "ALTER FUNCTION reader_expected_ambiguous_clusters(character varying) "
        "SET enable_nestloop = off"
    )
    op.execute(
        "ALTER FUNCTION "
        "reader_ambiguous_run_terminal_graph_status(character varying) "
        "SET enable_nestloop = off"
    )


def downgrade() -> None:
    raise RuntimeError(
        "ambiguous topology 审计计划修复不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
