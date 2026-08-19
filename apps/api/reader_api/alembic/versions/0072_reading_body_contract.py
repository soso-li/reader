"""Add the source selector and reading body contracts."""

from alembic import op
import sqlalchemy as sa


revision: str = "0072_reading_body_contract"
down_revision: str | None = "0071_source_fetch_validators"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    with op.batch_alter_table("sources") as batch:
        batch.add_column(sa.Column("article_selector", sa.Text(), nullable=True))
        batch.add_column(sa.Column("remove_selector", sa.Text(), nullable=True))

    with op.batch_alter_table("documents") as batch:
        batch.add_column(sa.Column("reading_html", sa.Text(), nullable=True))
        batch.add_column(
            sa.Column("body_source", sa.String(length=20), nullable=True)
        )
        batch.add_column(
            sa.Column("web_fetch_status", sa.String(length=20), nullable=True)
        )
        batch.create_check_constraint(
            "ck_document_reading_body_state",
            """
            (
                reading_html IS NULL
                AND body_source IS NULL
                AND web_fetch_status IS NULL
            )
            OR
            (
                reading_html IS NOT NULL
                AND body_source IS NOT NULL
                AND web_fetch_status IS NOT NULL
                AND (
                    (body_source = 'rss' AND web_fetch_status IN ('not_requested', 'failed'))
                    OR
                    (body_source = 'webpage' AND web_fetch_status = 'succeeded')
                )
            )
            """,
        )


def downgrade() -> None:
    raise RuntimeError(
        "正文数据合同不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
