"""Reject unauthenticated split history and force future xid8 provenance."""

from __future__ import annotations

from alembic import op


revision: str = "0038_split_xid_provenance"
down_revision: str | None = "0037_split_projection_audit"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        DO $reader$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM cluster_event_projections
                WHERE reconciliation_kind = 'split'
            ) OR EXISTS (
                SELECT 1
                FROM event_lineages
                WHERE relation_type = 'split_from'
            ) THEN
                RAISE EXCEPTION 'event_split_xid_provenance_upgrade_requires_empty_graph: 0038 以前的 split 图使用可覆盖的 xid8 DEFAULT，必须恢复或人工审计';
            END IF;
        END
        $reader$;

        CREATE FUNCTION reader_force_created_transaction_id()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            NEW.created_transaction_id := pg_current_xact_id();
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_00_event_revision_created_transaction
        BEFORE INSERT ON event_revisions
        FOR EACH ROW
        EXECUTE FUNCTION reader_force_created_transaction_id();

        CREATE TRIGGER trg_00_projection_created_transaction
        BEFORE INSERT ON cluster_event_projections
        FOR EACH ROW
        EXECUTE FUNCTION reader_force_created_transaction_id();

        CREATE TRIGGER trg_00_lineage_created_transaction
        BEFORE INSERT ON event_lineages
        FOR EACH ROW
        EXECUTE FUNCTION reader_force_created_transaction_id();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event split xid8 事务来源保护不支持原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
