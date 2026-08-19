"""Seal clustering membership snapshots before completion."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0019_snapshot_seals"
down_revision: str = "0018_run_snapshot_finalized"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_SNAPSHOT_AGGREGATE_SQL = """
SELECT
    count(*)::integer,
    encode(
        sha256(
            convert_to(
                COALESCE(
                    string_agg(
                        format(
                            '%s|%s|%s|%s',
                            cluster_anchor,
                            cluster_occurrence,
                            evidence_anchor,
                            evidence_occurrence
                        ),
                        E'\\n'
                        ORDER BY cluster_anchor, cluster_occurrence,
                                 evidence_anchor, evidence_occurrence
                    ),
                    ''
                ),
                'UTF8'
            )
        ),
        'hex'
    )
{into_clause}
FROM clustering_run_memberships
WHERE run_id = {run_id} AND snapshot_phase = {snapshot_phase};
"""


def upgrade() -> None:
    op.create_table(
        "clustering_run_snapshot_seals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("snapshot_phase", sa.String(length=16), nullable=False),
        sa.Column("snapshot_row_count", sa.Integer(), nullable=False),
        sa.Column("snapshot_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "sealed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "snapshot_phase IN ('before', 'after')",
            name="ck_clustering_run_snapshot_seal_phase",
        ),
        sa.CheckConstraint(
            "snapshot_row_count >= 0",
            name="ck_clustering_run_snapshot_seal_count",
        ),
        sa.CheckConstraint(
            "snapshot_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_clustering_run_snapshot_seal_fingerprint",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["clustering_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "snapshot_phase", name="uq_clustering_run_snapshot_seal"
        ),
    )
    op.execute(
        """
        INSERT INTO clustering_run_snapshot_seals (
            run_id, snapshot_phase, snapshot_row_count,
            snapshot_fingerprint, sealed_at
        )
        SELECT
            run.id,
            'after',
            count(membership.id)::integer,
            encode(
                sha256(
                    convert_to(
                        COALESCE(
                            string_agg(
                                format(
                                    '%s|%s|%s|%s',
                                    membership.cluster_anchor,
                                    membership.cluster_occurrence,
                                    membership.evidence_anchor,
                                    membership.evidence_occurrence
                                ),
                                E'\\n'
                                ORDER BY membership.cluster_anchor,
                                         membership.cluster_occurrence,
                                         membership.evidence_anchor,
                                         membership.evidence_occurrence
                            ) FILTER (WHERE membership.id IS NOT NULL),
                            ''
                        ),
                        'UTF8'
                    )
                ),
                'hex'
            ),
            run.completed_at
        FROM clustering_runs AS run
        LEFT JOIN clustering_run_memberships AS membership
          ON membership.run_id = run.id
         AND membership.snapshot_phase = 'after'
        WHERE run.status = 'completed'
        GROUP BY run.id, run.completed_at
        """
    )
    op.execute(
        """
        CREATE FUNCTION reader_clustering_snapshot_seal_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_status text;
            actual_count integer;
            actual_fingerprint text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                RAISE EXCEPTION 'clustering_snapshot_seal_immutable: 快照封印不可修改或删除';
            END IF;
            SELECT status INTO run_status
            FROM clustering_runs
            WHERE id = NEW.run_id
            FOR UPDATE;
            IF run_status IS DISTINCT FROM 'started' THEN
                RAISE EXCEPTION 'clustering_snapshot_seal_terminal: 只能封印 started run 的快照';
            END IF;
            """
        + _SNAPSHOT_AGGREGATE_SQL.format(
            run_id="NEW.run_id",
            snapshot_phase="NEW.snapshot_phase",
            into_clause="INTO actual_count, actual_fingerprint",
        )
        + """
            NEW.snapshot_row_count := actual_count;
            NEW.snapshot_fingerprint := actual_fingerprint;
            NEW.sealed_at := CURRENT_TIMESTAMP;
            RETURN NEW;
        END
        $reader$;

        CREATE TRIGGER trg_clustering_snapshot_seal
        BEFORE INSERT OR UPDATE OR DELETE ON clustering_run_snapshot_seals
        FOR EACH ROW EXECUTE FUNCTION reader_clustering_snapshot_seal_guard();
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_clustering_snapshot_immutable()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            run_status text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT status INTO run_status
                FROM clustering_runs
                WHERE id = NEW.run_id
                FOR UPDATE;
                IF run_status IS DISTINCT FROM 'started' THEN
                    RAISE EXCEPTION 'clustering_snapshot_terminal: 终态 Clustering Run 不可追加 scope 或 membership';
                END IF;
                IF TG_TABLE_NAME = 'clustering_run_memberships' THEN
                    IF EXISTS (
                        SELECT 1
                        FROM clustering_run_snapshot_seals
                        WHERE run_id = NEW.run_id
                          AND snapshot_phase = NEW.snapshot_phase
                    ) THEN
                        RAISE EXCEPTION 'clustering_snapshot_sealed: 已封印的 membership 快照不可追加';
                    END IF;
                END IF;
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'clustering_snapshot_immutable: Clustering Run scope 与 membership 快照不可修改或删除';
        END
        $reader$;
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_clustering_run_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            sealed_count integer;
            sealed_fingerprint text;
            actual_count integer;
            actual_fingerprint text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.status <> 'started' OR NEW.after_snapshot_finalized THEN
                    RAISE EXCEPTION 'clustering_run_insert_started: Clustering Run 必须从未完成快照的 started 状态创建';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'clustering_run_immutable: Clustering Run 不可删除';
            END IF;
            IF OLD.status <> 'started' THEN
                RAISE EXCEPTION 'clustering_run_terminal: 终态 Clustering Run 不可修改';
            END IF;
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.scope_type IS DISTINCT FROM OLD.scope_type
               OR NEW.scope_key IS DISTINCT FROM OLD.scope_key
               OR NEW.rule_version IS DISTINCT FROM OLD.rule_version
               OR NEW.started_at IS DISTINCT FROM OLD.started_at THEN
                RAISE EXCEPTION 'clustering_run_identity_immutable: 运行身份、scope、规则与开始时间不可修改';
            END IF;
            IF NEW.status NOT IN ('completed', 'failed') THEN
                RAISE EXCEPTION 'clustering_run_transition: started 只能进入 completed 或 failed';
            END IF;
            IF NEW.status = 'completed' AND NOT NEW.after_snapshot_finalized THEN
                RAISE EXCEPTION 'clustering_run_snapshot_incomplete: completed 必须声明 after 快照已完成';
            END IF;
            IF NEW.status = 'completed' THEN
                SELECT snapshot_row_count, snapshot_fingerprint
                INTO sealed_count, sealed_fingerprint
                FROM clustering_run_snapshot_seals
                WHERE run_id = NEW.id AND snapshot_phase = 'after';
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'clustering_run_snapshot_unsealed: completed 必须引用已封印的 after 快照';
                END IF;
                """
        + _SNAPSHOT_AGGREGATE_SQL.format(
            run_id="NEW.id",
            snapshot_phase="'after'",
            into_clause="INTO actual_count, actual_fingerprint",
        )
        + """
                IF sealed_count IS DISTINCT FROM actual_count
                   OR sealed_fingerprint IS DISTINCT FROM actual_fingerprint THEN
                    RAISE EXCEPTION 'clustering_run_snapshot_seal_mismatch: after 快照与封印不一致';
                END IF;
            END IF;
            RETURN NEW;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run snapshot seal 不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
