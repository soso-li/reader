"""Migrate remaining Cluster state and contract Event authority."""

from __future__ import annotations

from hashlib import sha256

from alembic import op
from sqlalchemy import text


revision: str = "0047_event_authority_contract"
down_revision: str | None = "0046_event_read_indexes"
branch_labels: str | None = None
depends_on: str | None = None

MIGRATION_VERSION = "legacy-user-state-baseline-v1"
SEEN_READ_STATUSES = {"summary_seen", "original_opened"}


def _state_tuple(row: dict[str, object], prefix: str) -> tuple[object, ...]:
    return (
        row[f"{prefix}read_status"],
        bool(row[f"{prefix}read_later"]),
        bool(row[f"{prefix}starred"]),
        row[f"{prefix}updated_at"],
    )


def upgrade() -> None:
    if op.get_context().as_sql:
        op.execute(
            """
            DO $reader$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM user_states WHERE object_type = 'cluster'
                ) THEN
                    RAISE EXCEPTION
                        'event_authority_offline_migration_unsupported: '
                        'Cluster UserState 必须使用在线 migration 入口迁移';
                END IF;
            END
            $reader$
            """
        )
        op.create_check_constraint(
            "ck_user_state_event_authority",
            "user_states",
            "object_type <> 'cluster'",
        )
        return

    connection = op.get_bind()
    connection.execute(
        text(
            "LOCK TABLE user_states, migration_baselines, event_user_states, "
            "cluster_event_projections, events IN ACCESS EXCLUSIVE MODE"
        )
    )
    rows = [
        dict(row)
        for row in connection.execute(
            text(
                "SELECT state.id AS user_state_id, "
                "       state.object_id AS cluster_id, "
                "       state.read_status AS state_read_status, "
                "       state.read_later AS state_read_later, "
                "       state.starred AS state_starred, "
                "       state.updated_at AS state_updated_at, "
                "       projection.event_id, projection.event_revision_id, "
                "       projection.event_status, "
                "       baseline.id AS baseline_id, "
                "       baseline.resolved_event_id AS baseline_event_id, "
                "       baseline.resolved_revision_id AS baseline_revision_id, "
                "       baseline.read_status AS baseline_read_status, "
                "       baseline.read_later AS baseline_read_later, "
                "       baseline.starred AS baseline_starred, "
                "       baseline.source_updated_at AS baseline_updated_at, "
                "       event_state.id AS event_state_id, "
                "       event_state.baseline_id AS event_state_baseline_id, "
                "       event_state.read_status AS event_state_read_status, "
                "       event_state.read_later AS event_state_read_later, "
                "       event_state.starred AS event_state_starred, "
                "       event_state.updated_at AS event_state_updated_at "
                "FROM user_states state "
                "LEFT JOIN LATERAL ("
                "    SELECT mapping.event_id, mapping.event_revision_id, "
                "           event.status AS event_status "
                "    FROM cluster_event_projections mapping "
                "    JOIN events event ON event.id = mapping.event_id "
                "    WHERE mapping.cluster_id = state.object_id "
                "    ORDER BY mapping.id DESC LIMIT 1"
                ") projection ON TRUE "
                "LEFT JOIN migration_baselines baseline "
                "  ON baseline.legacy_user_state_id = state.id "
                "LEFT JOIN event_user_states event_state "
                "  ON event_state.event_id = projection.event_id "
                "WHERE state.object_type = 'cluster' "
                "ORDER BY state.id"
            )
        ).mappings()
    ]

    state_by_event: dict[int, int] = {}
    for row in rows:
        state_id = int(row["user_state_id"])
        cluster_id = int(row["cluster_id"])
        if (
            row["event_id"] is None
            or row["event_revision_id"] is None
            or row["event_status"] != "active"
        ):
            raise RuntimeError(
                "event_authority_state_unresolvable: "
                f"user_state_id={state_id}, cluster_id={cluster_id}"
            )
        if row["state_updated_at"] is None:
            raise RuntimeError(
                "event_authority_state_unresolvable: "
                f"UserState 缺少 updated_at，user_state_id={state_id}"
            )
        event_id = int(row["event_id"])
        previous_state_id = state_by_event.setdefault(event_id, state_id)
        if previous_state_id != state_id:
            raise RuntimeError(
                "event_authority_state_ambiguous: "
                f"event_id={event_id}, user_state_ids={previous_state_id},{state_id}"
            )

        baseline_id = row["baseline_id"]
        if baseline_id is not None:
            if int(row["baseline_event_id"]) != event_id:
                raise RuntimeError(
                    "event_authority_baseline_mismatch: "
                    f"user_state_id={state_id}, event_id={event_id}"
                )
            if row["event_state_id"] is None and _state_tuple(
                row, "state_"
            ) != _state_tuple(row, "baseline_"):
                raise RuntimeError(
                    "event_authority_baseline_state_mismatch: "
                    f"user_state_id={state_id}, event_id={event_id}"
                )

        if row["event_state_id"] is not None:
            if _state_tuple(row, "state_") != _state_tuple(row, "event_state_"):
                raise RuntimeError(
                    "event_authority_state_mismatch: "
                    f"user_state_id={state_id}, event_id={event_id}"
                )
            if baseline_id is not None and row["event_state_baseline_id"] != baseline_id:
                raise RuntimeError(
                    "event_authority_baseline_link_mismatch: "
                    f"user_state_id={state_id}, event_id={event_id}"
                )
            continue

        if baseline_id is None:
            baseline_id = connection.execute(
                text(
                    "INSERT INTO migration_baselines "
                    "(idempotency_key, migration_version, legacy_user_state_id, "
                    " legacy_object_type, legacy_object_id, resolved_event_id, "
                    " resolved_revision_id, read_status, read_later, starred, "
                    " source_updated_at) "
                    "VALUES (:idempotency_key, :migration_version, :state_id, "
                    " 'cluster', :cluster_id, :event_id, :revision_id, "
                    " :read_status, :read_later, :starred, :updated_at) "
                    "RETURNING id"
                ),
                {
                    "idempotency_key": sha256(
                        f"{MIGRATION_VERSION}:{state_id}".encode()
                    ).hexdigest(),
                    "migration_version": MIGRATION_VERSION,
                    "state_id": state_id,
                    "cluster_id": cluster_id,
                    "event_id": event_id,
                    "revision_id": int(row["event_revision_id"]),
                    "read_status": row["state_read_status"],
                    "read_later": row["state_read_later"],
                    "starred": row["state_starred"],
                    "updated_at": row["state_updated_at"],
                },
            ).scalar_one()

        connection.execute(
            text(
                "INSERT INTO event_user_states "
                "(baseline_id, event_id, seen_revision_id, read_status, "
                " read_later, starred, updated_at) "
                "VALUES (:baseline_id, :event_id, :seen_revision_id, "
                " :read_status, :read_later, :starred, :updated_at)"
            ),
            {
                "baseline_id": int(baseline_id),
                "event_id": event_id,
                "seen_revision_id": (
                    int(
                        row["baseline_revision_id"]
                        if row["baseline_revision_id"] is not None
                        else row["event_revision_id"]
                    )
                    if row["state_read_status"] in SEEN_READ_STATUSES
                    else None
                ),
                "read_status": row["state_read_status"],
                "read_later": row["state_read_later"],
                "starred": row["state_starred"],
                "updated_at": row["state_updated_at"],
            },
        )

    connection.execute(text("DELETE FROM user_states WHERE object_type = 'cluster'"))
    op.create_check_constraint(
        "ck_user_state_event_authority",
        "user_states",
        "object_type <> 'cluster'",
    )


def downgrade() -> None:
    raise RuntimeError(
        "Event 权威切换不支持原地 downgrade；回滚必须恢复迁移前备份"
    )
