"""Require a verifiable before snapshot seal for failed runs."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op


revision: str = "0021_failed_before_seals"
down_revision: str = "0020_before_snapshot_seals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION reader_clustering_run_lifecycle_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $reader$
        DECLARE
            required_phase text;
            required_phases text[];
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
                required_phases := ARRAY['before', 'after'];
            ELSE
                required_phases := ARRAY['before'];
            END IF;
            FOREACH required_phase IN ARRAY required_phases LOOP
                SELECT snapshot_row_count, snapshot_fingerprint
                INTO sealed_count, sealed_fingerprint
                FROM clustering_run_snapshot_seals
                WHERE run_id = NEW.id AND snapshot_phase = required_phase;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'clustering_run_snapshot_unsealed: % 必须引用已封印的 % snapshot', NEW.status, required_phase;
                END IF;
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
                                        ORDER BY cluster_anchor,
                                                 cluster_occurrence,
                                                 evidence_anchor,
                                                 evidence_occurrence
                                    ),
                                    ''
                                ),
                                'UTF8'
                            )
                        ),
                        'hex'
                    )
                INTO actual_count, actual_fingerprint
                FROM clustering_run_memberships
                WHERE run_id = NEW.id
                  AND snapshot_phase = required_phase;
                IF sealed_count IS DISTINCT FROM actual_count
                   OR sealed_fingerprint IS DISTINCT FROM actual_fingerprint THEN
                    RAISE EXCEPTION 'clustering_run_snapshot_seal_mismatch: % snapshot 与封印不一致', required_phase;
                END IF;
            END LOOP;
            RETURN NEW;
        END
        $reader$;
        """
    )


def downgrade() -> None:
    raise RuntimeError(
        "Clustering Run failed before seal 不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
