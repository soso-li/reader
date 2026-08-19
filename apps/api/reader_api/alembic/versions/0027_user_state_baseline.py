"""Expand interaction state and preserve legacy UserState as a baseline."""

from __future__ import annotations

from collections.abc import Sequence
from hashlib import sha256

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


revision: str = "0027_user_state_baseline"
down_revision: str = "0026_legacy_cluster_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


MIGRATION_VERSION = "legacy-user-state-baseline-v1"
SEEN_READ_STATUSES = frozenset({"summary_seen", "original_opened"})
SHA256_CHECK = "{column} ~ '^[0-9a-f]{{64}}$'"
UUID_CHECK = (
    "id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    "[89ab][0-9a-f]{3}-[0-9a-f]{12}$'"
)


def _idempotency_key(user_state_id: int) -> str:
    return sha256(f"{MIGRATION_VERSION}:{user_state_id}".encode()).hexdigest()


def _create_expand_tables() -> None:
    op.create_table(
        "migration_baselines",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("migration_version", sa.String(length=120), nullable=False),
        sa.Column("legacy_user_state_id", sa.Integer(), nullable=False),
        sa.Column("legacy_object_type", sa.String(length=40), nullable=False),
        sa.Column("legacy_object_id", sa.Integer(), nullable=False),
        sa.Column("resolved_event_id", sa.Integer(), nullable=True),
        sa.Column("resolved_revision_id", sa.Integer(), nullable=True),
        sa.Column("read_status", sa.String(length=40), nullable=False),
        sa.Column("read_later", sa.Boolean(), nullable=False),
        sa.Column("starred", sa.Boolean(), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            SHA256_CHECK.format(column="idempotency_key"),
            name="ck_migration_baseline_idempotency_key_sha256",
        ),
        sa.CheckConstraint(
            f"migration_version = '{MIGRATION_VERSION}'",
            name="ck_migration_baseline_version",
        ),
        sa.CheckConstraint(
            "(legacy_object_type = 'cluster' "
            " AND resolved_event_id IS NOT NULL "
            " AND resolved_revision_id IS NOT NULL) OR "
            "(legacy_object_type <> 'cluster' "
            " AND resolved_event_id IS NULL "
            " AND resolved_revision_id IS NULL)",
            name="ck_migration_baseline_target_resolution",
        ),
        sa.ForeignKeyConstraint(
            ["resolved_event_id"],
            ["events.id"],
            name="fk_migration_baseline_event",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["resolved_event_id", "resolved_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_migration_baseline_event_revision",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key", name="uq_migration_baseline_idempotency_key"
        ),
        sa.UniqueConstraint(
            "legacy_user_state_id", name="uq_migration_baseline_legacy_user_state"
        ),
        sa.UniqueConstraint(
            "id", "resolved_event_id", name="uq_migration_baseline_event_reference"
        ),
    )
    op.create_index(
        "ix_migration_baselines_legacy_target",
        "migration_baselines",
        ["legacy_object_type", "legacy_object_id"],
    )
    op.create_table(
        "event_user_states",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("baseline_id", sa.Integer(), nullable=True),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.Column("seen_revision_id", sa.Integer(), nullable=True),
        sa.Column("read_status", sa.String(length=40), nullable=False),
        sa.Column("read_later", sa.Boolean(), nullable=False),
        sa.Column("starred", sa.Boolean(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["events.id"],
            name="fk_event_user_state_event",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["event_id", "seen_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_event_user_state_seen_revision",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["baseline_id", "event_id"],
            ["migration_baselines.id", "migration_baselines.resolved_event_id"],
            name="fk_event_user_state_baseline_event",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id", name="uq_event_user_state_event"),
        sa.UniqueConstraint("baseline_id", name="uq_event_user_state_baseline"),
    )
    op.create_table(
        "interaction_events",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("operation_id", sa.String(length=120), nullable=False),
        sa.Column("target_kind", sa.String(length=20), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=True),
        sa.Column("observed_revision_id", sa.Integer(), nullable=True),
        sa.Column("legacy_object_type", sa.String(length=40), nullable=True),
        sa.Column("legacy_object_id", sa.Integer(), nullable=True),
        sa.Column("action", sa.String(length=40), nullable=False),
        sa.Column("set_value", sa.JSON(), nullable=False),
        sa.Column(
            "payload", sa.JSON(), server_default=sa.text("'{}'::json"), nullable=False
        ),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(UUID_CHECK, name="ck_interaction_event_id_uuid"),
        sa.CheckConstraint(
            "operation_id <> ''", name="ck_interaction_event_operation_nonempty"
        ),
        sa.CheckConstraint(
            "action <> ''", name="ck_interaction_event_action_nonempty"
        ),
        sa.CheckConstraint(
            "target_kind IN ('event', 'legacy')",
            name="ck_interaction_event_target_kind",
        ),
        sa.CheckConstraint(
            "(target_kind = 'event' "
            " AND event_id IS NOT NULL "
            " AND observed_revision_id IS NOT NULL "
            " AND legacy_object_type IS NULL "
            " AND legacy_object_id IS NULL) OR "
            "(target_kind = 'legacy' "
            " AND event_id IS NULL "
            " AND observed_revision_id IS NULL "
            " AND legacy_object_type IS NOT NULL "
            " AND legacy_object_id IS NOT NULL)",
            name="ck_interaction_event_target_shape",
        ),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["events.id"],
            name="fk_interaction_event_event",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["event_id", "observed_revision_id"],
            ["event_revisions.event_id", "event_revisions.id"],
            name="fk_interaction_event_observed_revision",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("operation_id", name="uq_interaction_event_operation"),
    )
    op.create_index(
        "ix_interaction_events_event_time",
        "interaction_events",
        ["event_id", "occurred_at"],
    )
    op.create_index(
        "ix_interaction_events_legacy_target",
        "interaction_events",
        ["legacy_object_type", "legacy_object_id", "occurred_at"],
    )
    op.execute(
        """
        CREATE TRIGGER trg_migration_baseline_immutable
        BEFORE UPDATE OR DELETE ON migration_baselines
        FOR EACH ROW EXECUTE FUNCTION reader_event_immutable_row();

        CREATE TRIGGER trg_interaction_event_immutable
        BEFORE UPDATE OR DELETE ON interaction_events
        FOR EACH ROW EXECUTE FUNCTION reader_event_immutable_row();
        """
    )


def _legacy_states(connection) -> list[dict[str, object]]:
    unresolved = connection.execute(
        text(
            "SELECT state.id AS legacy_user_state_id, state.object_id AS cluster_id, "
            "       count(DISTINCT mapping.event_id) AS event_count "
            "FROM user_states state "
            "LEFT JOIN cluster_event_projections mapping "
            "  ON mapping.cluster_id_snapshot = state.object_id "
            "WHERE state.object_type = 'cluster' "
            "GROUP BY state.id, state.object_id "
            "HAVING count(DISTINCT mapping.event_id) <> 1 "
            "ORDER BY state.id"
        )
    ).mappings().first()
    if unresolved is not None:
        raise RuntimeError(
            "legacy_user_state_baseline_unresolvable: "
            "Cluster UserState 必须唯一解析到一个 Event，"
            f"user_state_id={unresolved['legacy_user_state_id']}, "
            f"cluster_id={unresolved['cluster_id']}, "
            f"event_count={unresolved['event_count']}"
        )
    rows = connection.execute(
        text(
            "SELECT state.id AS legacy_user_state_id, "
            "       state.object_type AS legacy_object_type, "
            "       state.object_id AS legacy_object_id, "
            "       state.read_status, state.read_later, state.starred, "
            "       state.updated_at AS source_updated_at, "
            "       projection.event_id AS resolved_event_id, "
            "       projection.event_revision_id AS resolved_revision_id "
            "FROM user_states state "
            "LEFT JOIN LATERAL ("
            "    SELECT mapping.event_id, mapping.event_revision_id "
            "    FROM cluster_event_projections mapping "
            "    WHERE mapping.cluster_id_snapshot = state.object_id "
            "    ORDER BY mapping.id DESC LIMIT 1"
            ") projection ON state.object_type = 'cluster' "
            "ORDER BY state.id"
        )
    ).mappings()
    prepared = [dict(row) for row in rows]
    resolved_events: dict[int, int] = {}
    for row in prepared:
        user_state_id = int(row["legacy_user_state_id"])
        if row["source_updated_at"] is None:
            raise RuntimeError(
                "legacy_user_state_baseline_unresolvable: "
                f"UserState 缺少 updated_at，user_state_id={user_state_id}"
            )
        if row["legacy_object_type"] != "cluster":
            continue
        if row["resolved_event_id"] is None or row["resolved_revision_id"] is None:
            raise RuntimeError(
                "legacy_user_state_baseline_unresolvable: "
                f"Cluster UserState 缺少 Event mapping，user_state_id={user_state_id}, "
                f"cluster_id={row['legacy_object_id']}"
            )
        event_id = int(row["resolved_event_id"])
        previous_state_id = resolved_events.setdefault(event_id, user_state_id)
        if previous_state_id != user_state_id:
            raise RuntimeError(
                "legacy_user_state_baseline_unresolvable: "
                f"多个 Cluster UserState 指向同一 Event，event_id={event_id}, "
                f"user_state_ids={previous_state_id},{user_state_id}"
            )
    return prepared


def _insert_baselines(connection, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    connection.execute(
        text(
            "INSERT INTO migration_baselines "
            "(idempotency_key, migration_version, legacy_user_state_id, "
            " legacy_object_type, legacy_object_id, resolved_event_id, "
            " resolved_revision_id, read_status, read_later, starred, "
            " source_updated_at) "
            "VALUES (:idempotency_key, :migration_version, "
            " :legacy_user_state_id, :legacy_object_type, :legacy_object_id, "
            " :resolved_event_id, :resolved_revision_id, :read_status, "
            " :read_later, :starred, :source_updated_at)"
        ),
        [
            {
                **row,
                "idempotency_key": _idempotency_key(
                    int(row["legacy_user_state_id"])
                ),
                "migration_version": MIGRATION_VERSION,
            }
            for row in rows
        ],
    )
    baseline_ids = {
        int(row["legacy_user_state_id"]): int(row["id"])
        for row in connection.execute(
            text("SELECT id, legacy_user_state_id FROM migration_baselines")
        ).mappings()
    }
    cluster_rows = [row for row in rows if row["legacy_object_type"] == "cluster"]
    if cluster_rows:
        connection.execute(
            text(
                "INSERT INTO event_user_states "
                "(baseline_id, event_id, seen_revision_id, read_status, "
                " read_later, starred, updated_at) "
                "VALUES (:baseline_id, :event_id, :seen_revision_id, "
                " :read_status, :read_later, :starred, :updated_at)"
            ),
            [
                {
                    "baseline_id": baseline_ids[int(row["legacy_user_state_id"])],
                    "event_id": row["resolved_event_id"],
                    "seen_revision_id": (
                        row["resolved_revision_id"]
                        if row["read_status"] in SEEN_READ_STATUSES
                        else None
                    ),
                    "read_status": row["read_status"],
                    "read_later": row["read_later"],
                    "starred": row["starred"],
                    "updated_at": row["source_updated_at"],
                }
                for row in cluster_rows
            ],
        )


def upgrade() -> None:
    if op.get_context().as_sql:
        op.execute(
            """
            DO $reader$
            BEGIN
                IF EXISTS (SELECT 1 FROM user_states) THEN
                    RAISE EXCEPTION
                        'legacy_user_state_offline_backfill_unsupported: '
                        'legacy UserState 必须使用在线 migration 入口建立 baseline';
                END IF;
            END
            $reader$
            """
        )
    _create_expand_tables()
    if op.get_context().as_sql:
        return
    connection = op.get_bind()
    _insert_baselines(connection, _legacy_states(connection))


def downgrade() -> None:
    raise RuntimeError(
        "Migration Baseline 与 Interaction Event expand 不可原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
