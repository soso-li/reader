"""Backfill every legacy Cluster into one stable Event revision."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from hashlib import sha256
import json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid5

from alembic import op
from sqlalchemy import text


revision: str = "0026_legacy_cluster_events"
down_revision: str = "0025_event_evidence_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


RULE_VERSION = "legacy-cluster-backfill-v1"
SCOPE_TYPE = "legacy-cluster-backfill"
EVIDENCE_TYPE = "article"
EVIDENCE_ROLE = "material"
WHOLE_ENTRY_FRAGMENT_FINGERPRINT = sha256(
    b"event-evidence-whole-entry-v1"
).hexdigest()
KEY_PRIORITY = {"guid": 0, "url": 1, "fallback": 2, "legacy": 3}


def _sha256_text(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _canonical_url(value: str) -> str:
    if not value:
        return ""
    parsed = urlsplit(value.strip())
    query = [
        (key, item_value)
        for key, item_value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
        and key.lower() not in {"fbclid", "gclid"}
    ]
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip("/"),
            urlencode(query),
            "",
        )
    )


def _stable_uuid(kind: str, material: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"reader:{kind}:{material}"))


def _evidence_anchor(row: Mapping[str, object]) -> str:
    projection = json.dumps(
        [
            "content-item-evidence-v1",
            row["source_entry_id"],
            row["raw_revision_no"],
            row["item_content_hash"] or "",
            row["item_canonical_url"] or "",
            row["item_title"] or "",
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return (
        f"source-entry:{row['source_entry_id']}:"
        f"revision:{row['raw_revision_no']}:"
        f"item:{_sha256_text(projection)}"
    )


def _cluster_anchor(evidence_anchors: tuple[str, ...]) -> str:
    canonical = json.dumps(
        sorted(evidence_anchors), ensure_ascii=False, separators=(",", ":")
    )
    return _sha256_text(f"cluster-membership-v1:{canonical}")


def _fragment_fingerprint(row: Mapping[str, object]) -> str:
    if row["document_type"] == "normal_article":
        return WHOLE_ENTRY_FRAGMENT_FINGERPRINT
    return _sha256_json(
        ["legacy-content-fragment-v2", row["item_content_hash"] or ""]
    )


def _load_stable_keys(connection, source_entry_ids: set[int]) -> dict[int, str]:
    if not source_entry_ids:
        return {}
    rows = connection.execute(
        text(
            "SELECT source_entry_id, identity_kind, identity_key "
            "FROM source_entry_keys "
            "WHERE source_entry_id = ANY(:source_entry_ids) "
            "ORDER BY source_entry_id, identity_kind, identity_key"
        ),
        {"source_entry_ids": sorted(source_entry_ids)},
    ).mappings()
    grouped: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[int(row["source_entry_id"])].append(
            (str(row["identity_kind"]), str(row["identity_key"]))
        )
    stable: dict[int, str] = {}
    for source_entry_id in source_entry_ids:
        keys = grouped.get(source_entry_id, [])
        if not keys:
            raise RuntimeError(
                "legacy_cluster_evidence_unlocatable: "
                f"Source Entry 缺少稳定 identity key，source_entry_id={source_entry_id}"
            )
        identity_kind, identity_key = min(
            keys,
            key=lambda value: (KEY_PRIORITY.get(value[0], 99), value[1]),
        )
        stable[source_entry_id] = f"{identity_kind}|{identity_key}"
    return stable


def _legacy_rows(connection) -> list[dict[str, object]]:
    rows = connection.execute(
        text(
            "SELECT cluster.id AS cluster_id, cluster.title AS cluster_title, "
            "       cluster.generated_title AS cluster_generated_title, "
            "       cluster.first_seen_at AS cluster_first_seen_at, "
            "       cluster_item.id AS cluster_item_id, "
            "       item.id AS item_id, item.source_id AS item_source_id, "
            "       item.title AS item_title, item.summary AS item_summary, "
            "       item.content_text AS item_content_text, item.url AS item_url, "
            "       item.canonical_url AS item_canonical_url, "
            "       item.published_at AS item_published_at, "
            "       item.content_hash AS item_content_hash, "
            "       document.id AS document_id, "
            "       document.document_type AS document_type, "
            "       raw.id AS raw_entry_id, raw.source_id AS raw_source_id, "
            "       raw.source_entry_id AS source_entry_id, "
            "       raw.revision_no AS raw_revision_no, "
            "       raw.payload_fingerprint AS raw_payload_fingerprint, "
            "       raw.external_id AS raw_external_id, "
            "       raw.title AS raw_title, raw.url AS raw_url, "
            "       raw.author AS raw_author, raw.published_at AS raw_published_at, "
            "       source.id AS source_id, source.url AS source_url, "
            "       source.media_type AS source_media_type "
            "FROM clusters cluster "
            "LEFT JOIN cluster_items cluster_item "
            "  ON cluster_item.cluster_id = cluster.id "
            "LEFT JOIN content_items item ON item.id = cluster_item.content_item_id "
            "LEFT JOIN documents document ON document.id = item.document_id "
            "LEFT JOIN raw_entries raw ON raw.id = document.raw_entry_id "
            "LEFT JOIN sources source ON source.id = item.source_id "
            "ORDER BY cluster.id, item.id, cluster_item.id"
        )
    ).mappings().all()
    prepared = [dict(row) for row in rows]
    for row in prepared:
        cluster_id = int(row["cluster_id"])
        required = (
            "cluster_item_id",
            "item_id",
            "item_source_id",
            "document_id",
            "document_type",
            "raw_entry_id",
            "raw_source_id",
            "source_entry_id",
            "raw_revision_no",
            "raw_payload_fingerprint",
            "raw_external_id",
            "source_id",
            "source_url",
            "source_media_type",
        )
        missing = [field for field in required if row[field] is None]
        if missing:
            raise RuntimeError(
                "legacy_cluster_evidence_unlocatable: "
                f"cluster_id={cluster_id} 缺少 {','.join(missing)}"
            )
        if row["source_media_type"] != "article":
            raise RuntimeError(
                "legacy_cluster_evidence_unlocatable: "
                f"cluster_id={cluster_id} 的 media_type="
                f"{row['source_media_type']} 不属于 legacy 文章证据"
            )
        if not (
            int(row["item_source_id"])
            == int(row["raw_source_id"])
            == int(row["source_id"])
        ):
            raise RuntimeError(
                "legacy_cluster_evidence_unlocatable: "
                f"cluster_id={cluster_id} 的 ContentItem/RawEntry/Source 不一致"
            )
    return prepared


def _ensure_empty_event_graph(connection) -> None:
    existing = connection.execute(
        text(
            "SELECT "
            "  (SELECT count(*) FROM events) + "
            "  (SELECT count(*) FROM event_revisions) + "
            "  (SELECT count(*) FROM event_evidence) + "
            "  (SELECT count(*) FROM event_evidence_versions) + "
            "  (SELECT count(*) FROM event_revision_evidence) + "
            "  (SELECT count(*) FROM cluster_event_projections)"
        )
    ).scalar_one()
    if int(existing) != 0:
        raise RuntimeError(
            "legacy_cluster_backfill_requires_empty_graph: "
            "#21 不能重解释已有 Event 历史；请从 P0.1 恢复副本重新迁移"
        )


def _insert_snapshot_run(
    connection,
    *,
    rows: list[dict[str, object]],
    cluster_positions: dict[int, tuple[str, int]],
    migration_time: datetime,
) -> str:
    item_anchors = {
        int(row["item_id"]): str(row["evidence_anchor"]) for row in rows
    }
    scope_occurrences: Counter[str] = Counter()
    scope_rows: list[tuple[str, int]] = []
    for _item_id, anchor in sorted(item_anchors.items()):
        scope_occurrences[anchor] += 1
        scope_rows.append((anchor, scope_occurrences[anchor]))

    cluster_evidence: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        cluster_evidence[int(row["cluster_id"])].append(
            str(row["evidence_anchor"])
        )
    membership_rows: list[tuple[str, int, str, int]] = []
    for cluster_id, anchors in sorted(cluster_evidence.items()):
        cluster_anchor, cluster_occurrence = cluster_positions[cluster_id]
        evidence_occurrences: Counter[str] = Counter()
        for evidence_anchor in sorted(anchors):
            evidence_occurrences[evidence_anchor] += 1
            membership_rows.append(
                (
                    cluster_anchor,
                    cluster_occurrence,
                    evidence_anchor,
                    evidence_occurrences[evidence_anchor],
                )
            )

    scope_key = _sha256_json(
        [
            "legacy-cluster-backfill-scope-v1",
            scope_rows,
            sorted(cluster_positions.items()),
        ]
    )
    run_id = _stable_uuid("legacy-cluster-backfill-run-v1", scope_key)
    connection.execute(
        text(
            "INSERT INTO clustering_runs "
            "(id, scope_type, scope_key, rule_version, status, failure_info, "
            " started_at, after_snapshot_finalized) "
            "VALUES (:id, :scope_type, :scope_key, :rule_version, 'started', '', "
            " :started_at, false)"
        ),
        {
            "id": run_id,
            "scope_type": SCOPE_TYPE,
            "scope_key": scope_key,
            "rule_version": RULE_VERSION,
            "started_at": migration_time,
        },
    )
    if scope_rows:
        connection.execute(
            text(
                "INSERT INTO clustering_run_scope_evidence "
                "(run_id, evidence_anchor, evidence_occurrence) "
                "VALUES (:run_id, :evidence_anchor, :evidence_occurrence)"
            ),
            [
                {
                    "run_id": run_id,
                    "evidence_anchor": evidence_anchor,
                    "evidence_occurrence": evidence_occurrence,
                }
                for evidence_anchor, evidence_occurrence in scope_rows
            ],
        )
    connection.execute(
        text(
            "INSERT INTO clustering_run_memberships "
            "(run_id, snapshot_phase, cluster_anchor, cluster_occurrence, "
            " evidence_anchor, evidence_occurrence) "
            "VALUES (:run_id, :snapshot_phase, :cluster_anchor, "
            " :cluster_occurrence, :evidence_anchor, :evidence_occurrence)"
        ),
        [
            {
                "run_id": run_id,
                "snapshot_phase": snapshot_phase,
                "cluster_anchor": cluster_anchor,
                "cluster_occurrence": cluster_occurrence,
                "evidence_anchor": evidence_anchor,
                "evidence_occurrence": evidence_occurrence,
            }
            for snapshot_phase in ("before", "after")
            for (
                cluster_anchor,
                cluster_occurrence,
                evidence_anchor,
                evidence_occurrence,
            ) in membership_rows
        ],
    )
    connection.execute(
        text(
            "INSERT INTO clustering_run_snapshot_seals "
            "(run_id, snapshot_phase, snapshot_row_count, "
            " snapshot_fingerprint, sealed_at) "
            "VALUES (:run_id, :snapshot_phase, 0, :empty_fingerprint, :sealed_at)"
        ),
        [
            {
                "run_id": run_id,
                "snapshot_phase": snapshot_phase,
                "empty_fingerprint": _sha256_text(""),
                "sealed_at": migration_time,
            }
            for snapshot_phase in ("before", "after")
        ],
    )
    connection.execute(
        text(
            "UPDATE clustering_runs "
            "SET status = 'completed', completed_at = :completed_at, "
            "    after_snapshot_finalized = true "
            "WHERE id = :run_id"
        ),
        {"run_id": run_id, "completed_at": migration_time},
    )
    return run_id


def upgrade() -> None:
    if op.get_context().as_sql:
        op.execute(
            """
            DO $reader$
            BEGIN
                IF EXISTS (SELECT 1 FROM clusters) THEN
                    RAISE EXCEPTION
                        'legacy_cluster_offline_backfill_unsupported: '
                        'legacy Cluster 必须使用在线 migration 入口生成稳定 Event fingerprint';
                END IF;
                IF EXISTS (SELECT 1 FROM events)
                   OR EXISTS (SELECT 1 FROM event_revisions)
                   OR EXISTS (SELECT 1 FROM event_evidence)
                   OR EXISTS (SELECT 1 FROM event_evidence_versions)
                   OR EXISTS (SELECT 1 FROM event_revision_evidence)
                   OR EXISTS (SELECT 1 FROM cluster_event_projections) THEN
                    RAISE EXCEPTION
                        'legacy_cluster_backfill_requires_empty_graph: '
                        '#21 不能重解释已有 Event 历史';
                END IF;
            END
            $reader$
            """
        )
        return

    connection = op.get_bind()
    _ensure_empty_event_graph(connection)
    rows = _legacy_rows(connection)
    if not rows:
        return

    stable_keys = _load_stable_keys(
        connection, {int(row["source_entry_id"]) for row in rows}
    )
    for row in rows:
        row["evidence_anchor"] = _evidence_anchor(row)
        fragment_fingerprint = _fragment_fingerprint(row)
        row["fragment_fingerprint"] = fragment_fingerprint
        row["identity_fingerprint"] = _sha256_json(
            [
                "event-evidence-identity-v2",
                _canonical_url(str(row["source_url"])),
                stable_keys[int(row["source_entry_id"])],
                fragment_fingerprint,
            ]
        )
        content_snapshot = (
            str(row["item_content_text"] or row["item_summary"] or "")
        )
        row["content_snapshot"] = content_snapshot
        row["version_fingerprint"] = _sha256_json(
            [
                "event-evidence-version-v4",
                _canonical_url(str(row["source_url"])),
                str(row["raw_external_id"]),
                str(row["raw_payload_fingerprint"]),
                str(row["item_content_hash"] or ""),
                str(row["item_title"] or ""),
                str(row["item_canonical_url"] or row["item_url"] or ""),
                content_snapshot,
                fragment_fingerprint,
            ]
        )

    cluster_evidence_anchors: dict[int, tuple[str, ...]] = {}
    grouped_anchors: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        grouped_anchors[int(row["cluster_id"])].append(
            str(row["evidence_anchor"])
        )
    for cluster_id, anchors in grouped_anchors.items():
        cluster_evidence_anchors[cluster_id] = tuple(sorted(anchors))

    identical_clusters: dict[tuple[str, ...], list[int]] = defaultdict(list)
    for cluster_id, anchors in cluster_evidence_anchors.items():
        identical_clusters[anchors].append(cluster_id)
    cluster_positions: dict[int, tuple[str, int]] = {}
    for anchors, cluster_ids in sorted(identical_clusters.items()):
        cluster_anchor = _cluster_anchor(anchors)
        for occurrence, cluster_id in enumerate(sorted(cluster_ids), start=1):
            cluster_positions[cluster_id] = (cluster_anchor, occurrence)

    migration_time = datetime.now(timezone.utc)
    run_id = _insert_snapshot_run(
        connection,
        rows=rows,
        cluster_positions=cluster_positions,
        migration_time=migration_time,
    )

    evidence_payloads: dict[tuple[int, str], dict[str, object]] = {}
    for row in sorted(
        rows,
        key=lambda item: (
            int(item["source_entry_id"]),
            str(item["fragment_fingerprint"]),
            int(item["item_id"]),
        ),
    ):
        evidence_key = (
            int(row["source_entry_id"]),
            str(row["fragment_fingerprint"]),
        )
        evidence_payloads.setdefault(
            evidence_key,
            {
                "uid": _stable_uuid(
                    "legacy-event-evidence-v1", str(row["identity_fingerprint"])
                ),
                "identity_fingerprint": row["identity_fingerprint"],
                "source_entry_id": row["source_entry_id"],
                "fragment_fingerprint": row["fragment_fingerprint"],
                "created_at": migration_time,
            },
        )
    connection.execute(
        text(
            "INSERT INTO event_evidence "
            "(uid, identity_fingerprint, source_entry_id, "
            " fragment_fingerprint, created_at) "
            "VALUES (:uid, :identity_fingerprint, :source_entry_id, "
            " :fragment_fingerprint, :created_at)"
        ),
        list(evidence_payloads.values()),
    )
    evidence_ids = {
        (int(row["source_entry_id"]), str(row["fragment_fingerprint"])): int(
            row["id"]
        )
        for row in connection.execute(
            text(
                "SELECT id, source_entry_id, fragment_fingerprint "
                "FROM event_evidence"
            )
        ).mappings()
    }

    version_payloads: dict[tuple[int, str], dict[str, object]] = {}
    row_version_keys: dict[int, tuple[int, str]] = {}
    for row in sorted(rows, key=lambda item: int(item["item_id"])):
        evidence_key = (
            int(row["source_entry_id"]),
            str(row["fragment_fingerprint"]),
        )
        evidence_id = evidence_ids[evidence_key]
        version_key = (evidence_id, str(row["version_fingerprint"]))
        version_payloads.setdefault(
            version_key,
            {
                "uid": _stable_uuid(
                    "legacy-event-evidence-version-v1",
                    f"{row['identity_fingerprint']}:{row['version_fingerprint']}",
                ),
                "evidence_id": evidence_id,
                "version_fingerprint": row["version_fingerprint"],
                "raw_entry_id": row["raw_entry_id"],
                "source_entry_id": row["source_entry_id"],
                "source_id": row["source_id"],
                "raw_revision_no": row["raw_revision_no"],
                "legacy_content_item_id": row["item_id"],
                "legacy_content_item_id_snapshot": row["item_id"],
                "fragment_fingerprint": row["fragment_fingerprint"],
                "title_snapshot": row["item_title"] or row["raw_title"] or "",
                "url_snapshot": row["item_url"] or row["raw_url"] or "",
                "author_snapshot": row["raw_author"] or "",
                "published_at_snapshot": row["item_published_at"]
                or row["raw_published_at"],
                "content_snapshot": row["content_snapshot"],
                "created_at": migration_time,
            },
        )
        row_version_keys[int(row["cluster_item_id"])] = version_key
    connection.execute(
        text(
            "INSERT INTO event_evidence_versions "
            "(uid, evidence_id, version_fingerprint, raw_entry_id, "
            " source_entry_id, source_id, raw_revision_no, "
            " legacy_content_item_id, legacy_content_item_id_snapshot, "
            " fragment_fingerprint, title_snapshot, url_snapshot, "
            " author_snapshot, published_at_snapshot, content_snapshot, "
            " created_at) "
            "VALUES (:uid, :evidence_id, :version_fingerprint, "
            " :raw_entry_id, :source_entry_id, :source_id, "
            " :raw_revision_no, :legacy_content_item_id, "
            " :legacy_content_item_id_snapshot, :fragment_fingerprint, "
            " :title_snapshot, :url_snapshot, :author_snapshot, "
            " :published_at_snapshot, :content_snapshot, :created_at)"
        ),
        list(version_payloads.values()),
    )
    version_ids = {
        (int(row["evidence_id"]), str(row["version_fingerprint"])): int(
            row["id"]
        )
        for row in connection.execute(
            text(
                "SELECT id, evidence_id, version_fingerprint "
                "FROM event_evidence_versions"
            )
        ).mappings()
    }
    row_version_ids = {
        cluster_item_id: version_ids[version_key]
        for cluster_item_id, version_key in row_version_keys.items()
    }

    cluster_rows: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        cluster_rows[int(row["cluster_id"])].append(row)
    cluster_specs: dict[int, dict[str, object]] = {}
    for cluster_id, member_rows in sorted(cluster_rows.items()):
        cluster_anchor, cluster_occurrence = cluster_positions[cluster_id]
        evidence_versions: dict[str, int] = {}
        for row in member_rows:
            fingerprint = str(row["version_fingerprint"])
            evidence_versions.setdefault(
                fingerprint, row_version_ids[int(row["cluster_item_id"])]
            )
        fingerprint_rows = sorted(
            f"{EVIDENCE_TYPE}|{EVIDENCE_ROLE}|{version_fingerprint}"
            for version_fingerprint in evidence_versions
        )
        revision_fingerprint = _sha256_text("\n".join(fingerprint_rows))
        event_uid = _stable_uuid(
            "legacy-cluster-event-v1",
            f"{cluster_anchor}:{cluster_occurrence}",
        )
        representative = min(
            member_rows, key=lambda item: int(item["cluster_item_id"])
        )
        cluster_specs[cluster_id] = {
            "event_uid": event_uid,
            "revision_uid": _stable_uuid(
                "legacy-cluster-event-revision-v1",
                f"{event_uid}:{revision_fingerprint}",
            ),
            "evidence_fingerprint": revision_fingerprint,
            "title_snapshot": str(
                representative["cluster_generated_title"] or ""
            ).strip()
            or str(representative["cluster_title"] or ""),
            "event_time_snapshot": representative["cluster_first_seen_at"],
            "cluster_anchor": cluster_anchor,
            "cluster_occurrence": cluster_occurrence,
            "evidence_version_ids": tuple(evidence_versions.values()),
        }

    connection.execute(
        text(
            "INSERT INTO events (uid, status, created_at) "
            "VALUES (:uid, 'active', :created_at)"
        ),
        [
            {
                "uid": spec["event_uid"],
                "created_at": migration_time,
            }
            for spec in cluster_specs.values()
        ],
    )
    event_ids = {
        str(row["uid"]): int(row["id"])
        for row in connection.execute(
            text("SELECT id, uid FROM events")
        ).mappings()
    }
    connection.execute(
        text(
            "INSERT INTO event_revisions "
            "(uid, event_id, revision_no, evidence_fingerprint, "
            " title_snapshot, event_time_snapshot, created_at) "
            "VALUES (:uid, :event_id, 1, :evidence_fingerprint, "
            " :title_snapshot, :event_time_snapshot, :created_at)"
        ),
        [
            {
                "uid": spec["revision_uid"],
                "event_id": event_ids[str(spec["event_uid"])],
                "evidence_fingerprint": spec["evidence_fingerprint"],
                "title_snapshot": spec["title_snapshot"],
                "event_time_snapshot": spec["event_time_snapshot"],
                "created_at": migration_time,
            }
            for spec in cluster_specs.values()
        ],
    )
    revision_ids = {
        str(row["uid"]): int(row["id"])
        for row in connection.execute(
            text("SELECT id, uid FROM event_revisions")
        ).mappings()
    }
    connection.execute(
        text(
            "INSERT INTO event_revision_evidence "
            "(revision_id, evidence_version_id, evidence_type, role, created_at) "
            "VALUES (:revision_id, :evidence_version_id, :evidence_type, "
            " :role, :created_at)"
        ),
        [
            {
                "revision_id": revision_ids[str(spec["revision_uid"])],
                "evidence_version_id": evidence_version_id,
                "evidence_type": EVIDENCE_TYPE,
                "role": EVIDENCE_ROLE,
                "created_at": migration_time,
            }
            for spec in cluster_specs.values()
            for evidence_version_id in spec["evidence_version_ids"]
        ],
    )
    connection.execute(
        text(
            "UPDATE events SET current_revision_id = :revision_id "
            "WHERE id = :event_id"
        ),
        [
            {
                "event_id": event_ids[str(spec["event_uid"])],
                "revision_id": revision_ids[str(spec["revision_uid"])],
            }
            for spec in cluster_specs.values()
        ],
    )
    connection.execute(
        text(
            "INSERT INTO cluster_event_projections "
            "(cluster_id, cluster_id_snapshot, clustering_run_id, "
            " cluster_anchor, cluster_occurrence, event_id, "
            " event_revision_id, projected_at) "
            "VALUES (:cluster_id, :cluster_id_snapshot, :clustering_run_id, "
            " :cluster_anchor, :cluster_occurrence, :event_id, "
            " :event_revision_id, :projected_at)"
        ),
        [
            {
                "cluster_id": cluster_id,
                "cluster_id_snapshot": cluster_id,
                "clustering_run_id": run_id,
                "cluster_anchor": spec["cluster_anchor"],
                "cluster_occurrence": spec["cluster_occurrence"],
                "event_id": event_ids[str(spec["event_uid"])],
                "event_revision_id": revision_ids[str(spec["revision_uid"])],
                "projected_at": migration_time,
            }
            for cluster_id, spec in cluster_specs.items()
        ],
    )


def downgrade() -> None:
    raise RuntimeError(
        "legacy Cluster Event/Revision/Evidence 回填不可原地 downgrade；"
        "回滚必须恢复迁移前备份"
    )
