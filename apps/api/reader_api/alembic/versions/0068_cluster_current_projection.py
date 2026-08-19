"""Add the rebuildable current Event projection for each live Cluster."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0068_cluster_current_projection"
down_revision: str | None = "0067_source_deletion_tombstone"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "cluster_current_event_projections",
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("projection_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["cluster_id"],
            ["clusters.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["projection_id"],
            ["cluster_event_projections.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("cluster_id"),
        sa.UniqueConstraint(
            "projection_id",
            name="uq_cluster_current_event_projection_projection",
        ),
    )
    op.execute(
        """
        INSERT INTO cluster_current_event_projections (cluster_id, projection_id)
        SELECT cluster.id, latest.projection_id
        FROM clusters AS cluster
        JOIN LATERAL (
            SELECT projection.id AS projection_id
            FROM cluster_event_projections AS projection
            WHERE projection.cluster_id = cluster.id
            ORDER BY projection.id DESC
            LIMIT 1
        ) AS latest ON true
        """
    )
    op.execute(
        """
        CREATE FUNCTION reader_guard_cluster_current_event_projection()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            latest_projection_id integer;
        BEGIN
            SELECT projection.id INTO latest_projection_id
            FROM cluster_event_projections AS projection
            WHERE projection.cluster_id = NEW.cluster_id
            ORDER BY projection.id DESC
            LIMIT 1;

            IF latest_projection_id IS DISTINCT FROM NEW.projection_id THEN
                RAISE EXCEPTION
                    'cluster_current_event_projection_not_latest: current pointer 必须引用同一 Cluster 的最新 projection';
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_cluster_current_event_projection_guard
        BEFORE INSERT OR UPDATE ON cluster_current_event_projections
        FOR EACH ROW
        EXECUTE FUNCTION reader_guard_cluster_current_event_projection();

        CREATE FUNCTION reader_advance_cluster_current_event_projection()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        BEGIN
            IF NEW.cluster_id IS NOT NULL THEN
                INSERT INTO cluster_current_event_projections (
                    cluster_id,
                    projection_id
                ) VALUES (
                    NEW.cluster_id,
                    NEW.id
                )
                ON CONFLICT (cluster_id) DO UPDATE
                SET projection_id = EXCLUDED.projection_id
                WHERE cluster_current_event_projections.projection_id
                      < EXCLUDED.projection_id;
            END IF;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_advance_cluster_current_event_projection
        AFTER INSERT ON cluster_event_projections
        FOR EACH ROW
        EXECUTE FUNCTION reader_advance_cluster_current_event_projection();
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Cluster current Event projection 不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
