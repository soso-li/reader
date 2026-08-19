from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping, TypeVar

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection


ColumnMap = Mapping[str, str]


@dataclass(frozen=True, order=True)
class IndexState:
    is_valid: bool
    is_ready: bool


@dataclass(frozen=True, order=True)
class KeyConstraint:
    columns: tuple[str, ...]
    is_deferrable: bool = False
    initially: str = "IMMEDIATE"
    index_state: IndexState = IndexState(is_valid=True, is_ready=True)


PrimaryKeyConstraint = KeyConstraint
UniqueConstraint = KeyConstraint
UniqueConstraintCollection = tuple[KeyConstraint, ...]


@dataclass(frozen=True, order=True)
class ForeignKey:
    constrained_columns: tuple[str, ...]
    referred_table: str
    referred_columns: tuple[str, ...]
    on_delete: str = "NO ACTION"
    on_update: str = "NO ACTION"
    is_deferrable: bool = False
    initially: str = "IMMEDIATE"
    is_validated: bool = True
    match_type: str = "SIMPLE"
    referred_schema_is_current: bool = True


ForeignKeyCollection = tuple[ForeignKey, ...]
ConstraintT = TypeVar("ConstraintT", KeyConstraint, ForeignKey)


@dataclass(frozen=True, order=True)
class RestrictedConstraint:
    kind: str
    definition: str
    is_validated: bool


@dataclass(frozen=True)
class ForeignKeyCatalogState:
    is_validated: bool
    match_type: str
    referred_schema_is_current: bool


@dataclass(frozen=True)
class SchemaSnapshot:
    tables: Mapping[str, ColumnMap]
    column_nullability: Mapping[str, Mapping[str, bool]]
    column_defaults: Mapping[str, Mapping[str, str | None]]
    identity_columns: frozenset[tuple[str, str]]
    serial_sequences: Mapping[str, Mapping[str, str | None]]
    primary_keys: Mapping[str, PrimaryKeyConstraint]
    unique_constraints: Mapping[str, UniqueConstraintCollection]
    foreign_keys: Mapping[str, ForeignKeyCollection]
    restricted_constraints: Mapping[str, tuple[RestrictedConstraint, ...]]
    standalone_unique_indexes: Mapping[str, tuple[str, ...]]
    extensions: frozenset[str]
    indexes: Mapping[str, str]
    index_states: Mapping[str, IndexState]


def _columns(**columns: str) -> ColumnMap:
    return MappingProxyType(columns)


EXPECTED_TABLES: Mapping[str, ColumnMap] = MappingProxyType(
    {
        "app_settings": _columns(
            key="character varying(80)",
            value="text",
            updated_at="timestamp with time zone",
        ),
        "clusters": _columns(
            id="integer",
            cluster_key="character varying(80)",
            title="text",
            generated_title="text",
            generated_summary="text",
            generated_content="text",
            citations="text",
            model_version="character varying(120)",
            prompt_version="character varying(120)",
            first_seen_at="timestamp with time zone",
            last_seen_at="timestamp with time zone",
            created_at="timestamp with time zone",
        ),
        "folders": _columns(
            id="integer",
            name="character varying(240)",
            created_at="timestamp with time zone",
        ),
        "llm_tasks": _columns(
            id="integer",
            task_type="character varying(80)",
            provider="character varying(80)",
            object_type="character varying(40)",
            object_id="integer",
            status="character varying(40)",
            prompt_version="character varying(120)",
            model_version="character varying(120)",
            result_json="text",
            created_at="timestamp with time zone",
            updated_at="timestamp with time zone",
        ),
        "topic_groups": _columns(
            id="integer",
            name="character varying(240)",
            query="text",
            description="text",
            created_at="timestamp with time zone",
            updated_at="timestamp with time zone",
        ),
        "user_states": _columns(
            id="integer",
            object_type="character varying(40)",
            object_id="integer",
            read_status="character varying(40)",
            read_later="boolean",
            starred="boolean",
            updated_at="timestamp with time zone",
        ),
        "sources": _columns(
            id="integer",
            folder_id="integer",
            name="character varying(320)",
            url="text",
            site_url="text",
            media_type="character varying(32)",
            status="character varying(20)",
            enabled="boolean",
            fetch_full_content="boolean",
            feed_trust_score="double precision",
            last_fetched_at="timestamp with time zone",
            last_error="text",
            created_at="timestamp with time zone",
            status_changed_at="timestamp with time zone",
        ),
        "feed_metrics": _columns(
            id="integer",
            source_id="integer",
            fetched_count="integer",
            read_count="integer",
            opened_count="integer",
            starred_count="integer",
            read_later_count="integer",
            cluster_count="integer",
            duplicate_count="integer",
            updated_at="timestamp with time zone",
        ),
        "raw_entries": _columns(
            id="integer",
            source_id="integer",
            external_id="text",
            title="text",
            url="text",
            author="text",
            published_at="timestamp with time zone",
            fetched_at="timestamp with time zone",
            raw_summary="text",
            raw_content="text",
            content_hash="character varying(64)",
        ),
        "documents": _columns(
            id="integer",
            raw_entry_id="integer",
            document_type="character varying(32)",
            title="text",
            summary="text",
            content_text="text",
            digest_score="double precision",
            created_at="timestamp with time zone",
        ),
        "content_items": _columns(
            id="integer",
            document_id="integer",
            source_id="integer",
            title="text",
            summary="text",
            content_text="text",
            url="text",
            published_at="timestamp with time zone",
            content_hash="character varying(64)",
            canonical_url="text",
            normalized_title="text",
            lsh_signature="text",
            media_url="text",
            media_kind="character varying(32)",
            media_duration="integer",
            embedding_vector="halfvec(2560)",
            embedding_model="character varying(120)",
            cluster_score="double precision",
            created_at="timestamp with time zone",
        ),
        "cluster_items": _columns(
            id="integer",
            cluster_id="integer",
            content_item_id="integer",
            duplicate_score="double precision",
            created_at="timestamp with time zone",
        ),
        "content_embeddings": _columns(
            id="integer",
            content_item_id="integer",
            representation="character varying(40)",
            model="character varying(120)",
            vector="halfvec(2560)",
            created_at="timestamp with time zone",
        ),
    }
)

EXPECTED_NULLABLE_COLUMNS = frozenset(
    {
        ("clusters", "generated_content"),
        ("clusters", "first_seen_at"),
        ("clusters", "last_seen_at"),
        ("sources", "folder_id"),
        ("sources", "last_fetched_at"),
        ("sources", "status_changed_at"),
        ("raw_entries", "published_at"),
        ("content_items", "published_at"),
        ("content_items", "media_url"),
        ("content_items", "media_kind"),
        ("content_items", "embedding_vector"),
        ("content_items", "embedding_model"),
        ("content_embeddings", "vector"),
    }
)

STRICT_LEGACY_NOT_NULL_COLUMNS = frozenset(
    {
        ("clusters", "generated_content"),
        ("content_items", "media_url"),
        ("content_items", "media_kind"),
        ("content_items", "embedding_model"),
    }
)
PRODUCTION_LEGACY_NOT_NULL_COLUMNS = frozenset(
    {
        ("clusters", "generated_content"),
        ("content_items", "embedding_model"),
    }
)


def _column_nullability_contract(
    not_null_columns: frozenset[tuple[str, str]],
) -> Mapping[str, Mapping[str, bool]]:
    return MappingProxyType(
        {
            table: MappingProxyType(
                {
                    column: (
                        (table, column) in EXPECTED_NULLABLE_COLUMNS
                        and (table, column) not in not_null_columns
                    )
                    for column in columns
                }
            )
            for table, columns in EXPECTED_TABLES.items()
        }
    )


EXPECTED_COLUMN_NULLABILITY = _column_nullability_contract(frozenset())
STRICT_LEGACY_COLUMN_NULLABILITY = _column_nullability_contract(
    STRICT_LEGACY_NOT_NULL_COLUMNS
)
PRODUCTION_LEGACY_COLUMN_NULLABILITY = _column_nullability_contract(
    PRODUCTION_LEGACY_NOT_NULL_COLUMNS
)

EXPECTED_PRIMARY_KEYS: Mapping[str, PrimaryKeyConstraint] = MappingProxyType(
    {
        table: PrimaryKeyConstraint(
            ("key",) if table == "app_settings" else ("id",)
        )
        for table in EXPECTED_TABLES
    }
)

EXPECTED_AUTOINCREMENT_COLUMNS = frozenset(
    (table, "id") for table in EXPECTED_TABLES if table != "app_settings"
)

def _unique_constraints(*columns: tuple[str, ...]) -> UniqueConstraintCollection:
    return tuple(sorted(UniqueConstraint(value) for value in columns))


EXPECTED_UNIQUES: Mapping[str, UniqueConstraintCollection] = MappingProxyType(
    {
        "folders": _unique_constraints(("name",)),
        "sources": _unique_constraints(("url",)),
        "raw_entries": _unique_constraints(("source_id", "external_id")),
        "documents": _unique_constraints(("raw_entry_id",)),
        "content_embeddings": _unique_constraints(
            ("content_item_id", "representation", "model")
        ),
        "clusters": _unique_constraints(("cluster_key",)),
        "cluster_items": _unique_constraints(("cluster_id", "content_item_id")),
        "topic_groups": _unique_constraints(("name",)),
        "user_states": _unique_constraints(("object_type", "object_id")),
    }
)

def _foreign_keys(*constraints: ForeignKey) -> ForeignKeyCollection:
    return tuple(sorted(constraints))


EXPECTED_FOREIGN_KEYS: Mapping[str, ForeignKeyCollection] = MappingProxyType(
    {
        "sources": _foreign_keys(ForeignKey(("folder_id",), "folders", ("id",))),
        "raw_entries": _foreign_keys(
            ForeignKey(("source_id",), "sources", ("id",))
        ),
        "documents": _foreign_keys(
            ForeignKey(("raw_entry_id",), "raw_entries", ("id",))
        ),
        "content_items": _foreign_keys(
            ForeignKey(("document_id",), "documents", ("id",)),
            ForeignKey(("source_id",), "sources", ("id",)),
        ),
        "content_embeddings": _foreign_keys(
            ForeignKey(("content_item_id",), "content_items", ("id",))
        ),
        "cluster_items": _foreign_keys(
            ForeignKey(("cluster_id",), "clusters", ("id",)),
            ForeignKey(("content_item_id",), "content_items", ("id",)),
        ),
        "feed_metrics": _foreign_keys(
            ForeignKey(("source_id",), "sources", ("id",))
        ),
    }
)

# `EXPECTED_LEGACY_SCHEMA` deliberately remains the pre-Alembic baseline used
# for safe stamping. These are the separately asserted constraints introduced
# by 0069 after that baseline has been upgraded.
EXPLICIT_FOLDER_MEDIA_TYPES = frozenset(
    {"article", "social", "image", "video", "podcast", "notification"}
)
EXPLICIT_FOLDER_UNIQUES = _unique_constraints(
    ("media_type", "name"),
    ("id", "media_type"),
)
EXPLICIT_SOURCE_FOLDER_FOREIGN_KEY = ForeignKey(
    ("folder_id", "media_type"),
    "folders",
    ("id", "media_type"),
)


def _constraint_multiplicity_errors(
    table: str,
    label: str,
    expected: tuple[ConstraintT, ...],
    actual: tuple[ConstraintT, ...],
) -> list[str]:
    errors: list[str] = []
    expected_constraints = Counter(expected)
    actual_constraints = Counter(actual)
    for constraint, count in sorted(
        (expected_constraints - actual_constraints).items()
    ):
        errors.append(f"{table} 缺少{label} {constraint} × {count}")
    for constraint, count in sorted(
        (actual_constraints - expected_constraints).items()
    ):
        errors.append(f"{table} 存在额外{label} {constraint} × {count}")
    return errors

EXPECTED_INDEX_DEFINITIONS: Mapping[str, str] = MappingProxyType(
    {
        "ix_content_items_fts": (
            "CREATE INDEX ix_content_items_fts ON content_items USING GIN ("
            "to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(summary,'') "
            "|| ' ' || coalesce(content_text,'')))"
        ),
        "ix_sources_name_fts": (
            "CREATE INDEX ix_sources_name_fts ON sources USING GIN ("
            "to_tsvector('simple', coalesce(name,'')))"
        ),
        "ix_clusters_fts": (
            "CREATE INDEX ix_clusters_fts ON clusters USING GIN ("
            "to_tsvector('simple', coalesce(title,'') || ' ' || "
            "coalesce(generated_title,'') || ' ' || coalesce(generated_summary,'') "
            "|| ' ' || coalesce(generated_content,'')))"
        ),
        "ix_content_items_embedding_hnsw": (
            "CREATE INDEX ix_content_items_embedding_hnsw ON content_items "
            "USING HNSW (embedding_vector halfvec_cosine_ops) "
            "WHERE embedding_vector IS NOT NULL"
        ),
        "ix_content_embeddings_zh_hnsw": (
            "CREATE INDEX ix_content_embeddings_zh_hnsw ON content_embeddings "
            "USING HNSW (vector halfvec_cosine_ops) "
            "WHERE vector IS NOT NULL AND representation = 'zh_canonical'"
        ),
    }
)

EXPECTED_LEGACY_SCHEMA = SchemaSnapshot(
    tables=EXPECTED_TABLES,
    column_nullability=EXPECTED_COLUMN_NULLABILITY,
    column_defaults=MappingProxyType(
        {
            table: MappingProxyType(
                {
                    column: (
                        f"nextval('{table}_id_seq'::regclass)"
                        if (table, column) in EXPECTED_AUTOINCREMENT_COLUMNS
                        else None
                    )
                    for column in columns
                }
            )
            for table, columns in EXPECTED_TABLES.items()
        }
    ),
    identity_columns=frozenset(),
    serial_sequences=MappingProxyType(
        {
            table: MappingProxyType(
                {
                    column: (
                        f"public.{table}_id_seq"
                        if (table, column) in EXPECTED_AUTOINCREMENT_COLUMNS
                        else None
                    )
                    for column in columns
                }
            )
            for table, columns in EXPECTED_TABLES.items()
        }
    ),
    primary_keys=EXPECTED_PRIMARY_KEYS,
    unique_constraints=EXPECTED_UNIQUES,
    foreign_keys=EXPECTED_FOREIGN_KEYS,
    restricted_constraints=MappingProxyType({}),
    standalone_unique_indexes=MappingProxyType({}),
    extensions=frozenset({"vector"}),
    indexes=EXPECTED_INDEX_DEFINITIONS,
    index_states=MappingProxyType(
        {
            name: IndexState(is_valid=True, is_ready=True)
            for name in EXPECTED_INDEX_DEFINITIONS
        }
    ),
)

ALLOWED_AUXILIARY_TABLES = frozenset({"alembic_version"})
ALEMBIC_VERSION_COLUMNS = MappingProxyType(
    {"version_num": "character varying(32)"}
)


def normalize_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


SINGLE_QUOTED_SQL_LITERAL = re.compile(r"'(?:''|[^'])*'")


def canonicalize_index_syntax(value: str) -> str:
    normalized = value.lower()
    normalized = re.sub(r"\bpublic\.", "", normalized)
    normalized = re.sub(
        r"::(?:pg_catalog\.)?(?:regconfig|text|character varying)\b",
        "",
        normalized,
    )
    return re.sub(r"[\s();]", "", normalized)


def canonicalize_index_definition(value: str) -> str:
    parts: list[str] = []
    position = 0
    for literal in SINGLE_QUOTED_SQL_LITERAL.finditer(value):
        parts.append(canonicalize_index_syntax(value[position : literal.start()]))
        parts.append(literal.group(0))
        position = literal.end()
    parts.append(canonicalize_index_syntax(value[position:]))
    return "".join(parts)


NEXTVAL_DEFAULT = re.compile(
    r"^nextval\('(?P<sequence>[^']+)'::regclass\)$",
    re.IGNORECASE,
)


def normalize_sequence_name(value: str) -> str:
    normalized = value.strip().lower().replace('"', "")
    return normalized.removeprefix("public.")


def column_is_autoincrementing(
    snapshot: SchemaSnapshot,
    table: str,
    column: str,
) -> bool:
    sequence = snapshot.serial_sequences.get(table, {}).get(column)
    if not sequence:
        return False
    if (table, column) in snapshot.identity_columns:
        return True
    default = snapshot.column_defaults.get(table, {}).get(column)
    if not default:
        return False
    match = NEXTVAL_DEFAULT.fullmatch(normalize_sql(default))
    return bool(
        match
        and normalize_sequence_name(match.group("sequence"))
        == normalize_sequence_name(sequence)
    )


def compare_legacy_schema(
    snapshot: SchemaSnapshot,
    *,
    expected_column_nullability: Mapping[str, Mapping[str, bool]] = (
        EXPECTED_COLUMN_NULLABILITY
    ),
    allowed_auxiliary_tables: frozenset[str] = ALLOWED_AUXILIARY_TABLES,
) -> list[str]:
    errors: list[str] = []
    unexpected_tables = (
        set(snapshot.tables) - set(EXPECTED_TABLES) - allowed_auxiliary_tables
    )
    for table in sorted(unexpected_tables):
        errors.append(f"存在额外表 {table}")

    for table, expected_columns in EXPECTED_TABLES.items():
        actual_columns = snapshot.tables.get(table)
        if actual_columns is None:
            errors.append(f"缺少表 {table}")
            continue
        for column, expected_type in expected_columns.items():
            actual_type = actual_columns.get(column)
            if actual_type is None:
                errors.append(f"{table}.{column} 缺少列")
            elif normalize_sql(actual_type) != normalize_sql(expected_type):
                errors.append(
                    f"{table}.{column} 类型应为 {expected_type}，实际为 {actual_type}"
                )
            expected_nullable = expected_column_nullability[table][column]
            actual_nullable = snapshot.column_nullability.get(table, {}).get(column)
            if actual_type is not None and actual_nullable is None:
                errors.append(f"{table}.{column} 无法核对 nullability")
            elif actual_nullable is not None and actual_nullable != expected_nullable:
                requirement = "可为空" if expected_nullable else "不可为空"
                errors.append(f"{table}.{column} 应为{requirement}")

        unexpected_columns = set(actual_columns) - set(expected_columns)
        for column in sorted(unexpected_columns):
            errors.append(f"{table} 存在额外列 {column}")

        expected_pk = EXPECTED_PRIMARY_KEYS[table]
        actual_pk = snapshot.primary_keys.get(table)
        if actual_pk != expected_pk:
            errors.append(
                f"{table} 主键应为 {expected_pk}，实际为 {actual_pk}"
            )

        if (table, "id") in EXPECTED_AUTOINCREMENT_COLUMNS and not column_is_autoincrementing(
            snapshot,
            table,
            "id",
        ):
            errors.append(f"{table}.id 缺少有效的自增默认、identity 或关联序列")

    alembic_columns = snapshot.tables.get("alembic_version")
    if alembic_columns is not None:
        version_type = alembic_columns.get("version_num")
        expected_type = ALEMBIC_VERSION_COLUMNS["version_num"]
        if version_type is None:
            errors.append("alembic_version.version_num 缺少列")
        elif normalize_sql(version_type) != normalize_sql(expected_type):
            errors.append(
                "alembic_version.version_num "
                f"类型应为 {expected_type}，实际为 {version_type}"
            )
        version_nullable = snapshot.column_nullability.get("alembic_version", {}).get(
            "version_num"
        )
        if version_type is not None and version_nullable is None:
            errors.append("alembic_version.version_num 无法核对 nullability")
        elif version_nullable is True:
            errors.append("alembic_version.version_num 应为不可为空")
        for column in sorted(set(alembic_columns) - set(ALEMBIC_VERSION_COLUMNS)):
            errors.append(f"alembic_version 存在额外列 {column}")
        expected_pk = PrimaryKeyConstraint(("version_num",))
        actual_pk = snapshot.primary_keys.get("alembic_version")
        if actual_pk != expected_pk:
            errors.append(
                "alembic_version "
                f"主键应为 {expected_pk}，实际为 {actual_pk}"
            )

    constraint_tables = list(EXPECTED_TABLES)
    if "alembic_version" in snapshot.tables:
        constraint_tables.append("alembic_version")

    for table in constraint_tables:
        errors.extend(
            _constraint_multiplicity_errors(
                table,
                "唯一约束",
                EXPECTED_UNIQUES.get(table, ()),
                snapshot.unique_constraints.get(table, ()),
            )
        )

    for table in constraint_tables:
        errors.extend(
            _constraint_multiplicity_errors(
                table,
                "外键",
                EXPECTED_FOREIGN_KEYS.get(table, ()),
                snapshot.foreign_keys.get(table, ()),
            )
        )

    for table in constraint_tables:
        for constraint in snapshot.restricted_constraints.get(table, ()):
            errors.append(
                f"{table} 存在额外{constraint.kind}约束 "
                f"{constraint.definition}；validated={constraint.is_validated}"
            )

    for table in constraint_tables:
        for index_name in snapshot.standalone_unique_indexes.get(table, ()):
            errors.append(f"{table} 存在额外独立 UNIQUE 索引 {index_name}")

    if "vector" not in snapshot.extensions:
        errors.append("缺少 vector 扩展")

    for name, expected_definition in EXPECTED_INDEX_DEFINITIONS.items():
        actual_definition = snapshot.indexes.get(name, "")
        if not actual_definition:
            errors.append(f"缺少索引 {name}")
            continue
        if canonicalize_index_definition(actual_definition) != canonicalize_index_definition(
            expected_definition
        ):
            errors.append(f"索引 {name} 定义不兼容；应为 {expected_definition}")
        state = snapshot.index_states.get(name)
        if state is None:
            errors.append(f"索引 {name} 无法核对有效与就绪状态")
            continue
        if not state.is_valid:
            errors.append(f"索引 {name} 无效")
        if not state.is_ready:
            errors.append(f"索引 {name} 未就绪")
    return errors


def compare_strict_legacy_schema(snapshot: SchemaSnapshot) -> list[str]:
    return compare_legacy_schema(
        snapshot,
        expected_column_nullability=STRICT_LEGACY_COLUMN_NULLABILITY,
    )


def compare_production_legacy_schema(snapshot: SchemaSnapshot) -> list[str]:
    return compare_legacy_schema(
        snapshot,
        expected_column_nullability=PRODUCTION_LEGACY_COLUMN_NULLABILITY,
    )


def read_postgres_schema(connection: Connection) -> SchemaSnapshot:
    schema = connection.scalar(text("SELECT current_schema()")) or "public"
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names(schema=schema))
    tables: dict[str, dict[str, str]] = {name: {} for name in table_names}
    column_nullability: dict[str, dict[str, bool]] = {
        name: {} for name in table_names
    }
    column_defaults: dict[str, dict[str, str | None]] = {
        name: {} for name in table_names
    }
    serial_sequences: dict[str, dict[str, str | None]] = {
        name: {} for name in table_names
    }
    identity_columns: set[tuple[str, str]] = set()
    rows = connection.execute(
        text(
            """
            SELECT c.relname AS table_name,
                   a.attname AS column_name,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
                   NOT a.attnotnull AS is_nullable,
                   pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expression,
                   a.attidentity AS identity_kind,
                   pg_catalog.pg_get_serial_sequence(
                       pg_catalog.format('%I.%I', n.nspname, c.relname),
                       a.attname
                   ) AS serial_sequence
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef d
              ON d.adrelid = a.attrelid
             AND d.adnum = a.attnum
            WHERE n.nspname = :schema
              AND c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
            """
        ),
        {"schema": schema},
    )
    for row in rows.mappings():
        tables.setdefault(row["table_name"], {})[row["column_name"]] = row["formatted_type"]
        column_nullability.setdefault(row["table_name"], {})[row["column_name"]] = bool(
            row["is_nullable"]
        )
        column_defaults.setdefault(row["table_name"], {})[row["column_name"]] = row[
            "default_expression"
        ]
        serial_sequences.setdefault(row["table_name"], {})[row["column_name"]] = row[
            "serial_sequence"
        ]
        if row["identity_kind"]:
            identity_columns.add((row["table_name"], row["column_name"]))

    primary_keys: dict[str, PrimaryKeyConstraint] = {}
    unique_constraint_lists: dict[str, list[UniqueConstraint]] = {
        table: [] for table in table_names
    }
    for row in connection.execute(
        text(
            """
            SELECT table_class.relname AS table_name,
                   constraint_meta.contype AS constraint_type,
                   constraint_meta.condeferrable AS is_deferrable,
                   constraint_meta.condeferred AS is_initially_deferred,
                   index_meta.indisvalid AS index_is_valid,
                   index_meta.indisready AS index_is_ready,
                   ARRAY(
                     SELECT column_meta.attname
                     FROM unnest(constraint_meta.conkey)
                          WITH ORDINALITY AS constrained_column(attnum, position)
                     JOIN pg_catalog.pg_attribute column_meta
                       ON column_meta.attrelid = constraint_meta.conrelid
                      AND column_meta.attnum = constrained_column.attnum
                     ORDER BY constrained_column.position
                   ) AS column_names
            FROM pg_catalog.pg_constraint constraint_meta
            JOIN pg_catalog.pg_class table_class
              ON table_class.oid = constraint_meta.conrelid
            JOIN pg_catalog.pg_namespace table_namespace
              ON table_namespace.oid = table_class.relnamespace
            JOIN pg_catalog.pg_index index_meta
              ON index_meta.indexrelid = constraint_meta.conindid
            WHERE table_namespace.nspname = :schema
              AND constraint_meta.contype IN ('p', 'u')
            """
        ),
        {"schema": schema},
    ).mappings():
        constraint = KeyConstraint(
            columns=tuple(row["column_names"] or ()),
            is_deferrable=bool(row["is_deferrable"]),
            initially=(
                "DEFERRED" if row["is_initially_deferred"] else "IMMEDIATE"
            ),
            index_state=IndexState(
                is_valid=bool(row["index_is_valid"]),
                is_ready=bool(row["index_is_ready"]),
            ),
        )
        if row["constraint_type"] == "p":
            primary_keys[row["table_name"]] = constraint
        else:
            unique_constraint_lists[row["table_name"]].append(constraint)
    unique_constraints: dict[str, UniqueConstraintCollection] = {
        table: tuple(sorted(constraints))
        for table, constraints in unique_constraint_lists.items()
    }
    restricted_constraint_lists: dict[str, list[RestrictedConstraint]] = {
        table: [] for table in table_names
    }
    for row in connection.execute(
        text(
            """
            SELECT table_class.relname AS table_name,
                   CASE constraint_meta.contype
                     WHEN 'c' THEN 'CHECK'
                     ELSE 'EXCLUDE'
                   END AS constraint_kind,
                   pg_catalog.pg_get_constraintdef(
                     constraint_meta.oid,
                     true
                   ) AS definition,
                   constraint_meta.convalidated AS is_validated
            FROM pg_catalog.pg_constraint constraint_meta
            JOIN pg_catalog.pg_class table_class
              ON table_class.oid = constraint_meta.conrelid
            JOIN pg_catalog.pg_namespace table_namespace
              ON table_namespace.oid = table_class.relnamespace
            WHERE table_namespace.nspname = :schema
              AND constraint_meta.contype IN ('c', 'x')
            """
        ),
        {"schema": schema},
    ).mappings():
        restricted_constraint_lists[row["table_name"]].append(
            RestrictedConstraint(
                kind=str(row["constraint_kind"]),
                definition=str(row["definition"]),
                is_validated=bool(row["is_validated"]),
            )
        )
    restricted_constraints = {
        table: tuple(sorted(constraints))
        for table, constraints in restricted_constraint_lists.items()
    }
    foreign_keys: dict[str, ForeignKeyCollection] = {}
    foreign_key_catalog = {
        (row["table_name"], row["constraint_name"]): ForeignKeyCatalogState(
            is_validated=bool(row["is_validated"]),
            match_type=str(row["match_type"]),
            referred_schema_is_current=str(row["referred_schema"]) == schema,
        )
        for row in connection.execute(
            text(
                """
                SELECT table_class.relname AS table_name,
                       constraint_meta.conname AS constraint_name,
                       constraint_meta.convalidated AS is_validated,
                       CASE constraint_meta.confmatchtype
                         WHEN 'f' THEN 'FULL'
                         WHEN 'p' THEN 'PARTIAL'
                         ELSE 'SIMPLE'
                       END AS match_type,
                       referred_namespace.nspname AS referred_schema
                FROM pg_catalog.pg_constraint constraint_meta
                JOIN pg_catalog.pg_class table_class
                  ON table_class.oid = constraint_meta.conrelid
                JOIN pg_catalog.pg_namespace table_namespace
                  ON table_namespace.oid = table_class.relnamespace
                JOIN pg_catalog.pg_class referred_class
                  ON referred_class.oid = constraint_meta.confrelid
                JOIN pg_catalog.pg_namespace referred_namespace
                  ON referred_namespace.oid = referred_class.relnamespace
                WHERE table_namespace.nspname = :schema
                  AND constraint_meta.contype = 'f'
                """
            ),
            {"schema": schema},
        ).mappings()
    }
    for table in table_names:
        table_foreign_keys: list[ForeignKey] = []
        for constraint in inspector.get_foreign_keys(table, schema=schema):
            catalog_state = foreign_key_catalog.get(
                (table, str(constraint.get("name") or "")),
                ForeignKeyCatalogState(
                    is_validated=False,
                    match_type="UNKNOWN",
                    referred_schema_is_current=False,
                ),
            )
            table_foreign_keys.append(
                ForeignKey(
                    constrained_columns=tuple(
                        constraint.get("constrained_columns") or ()
                    ),
                    referred_table=str(constraint.get("referred_table") or ""),
                    referred_columns=tuple(
                        constraint.get("referred_columns") or ()
                    ),
                    on_delete=str(
                        (constraint.get("options") or {}).get("ondelete")
                        or "NO ACTION"
                    ).upper(),
                    on_update=str(
                        (constraint.get("options") or {}).get("onupdate")
                        or "NO ACTION"
                    ).upper(),
                    is_deferrable=bool(
                        (constraint.get("options") or {}).get(
                            "deferrable",
                            False,
                        )
                    ),
                    initially=str(
                        (constraint.get("options") or {}).get("initially")
                        or "IMMEDIATE"
                    ).upper(),
                    is_validated=catalog_state.is_validated,
                    match_type=catalog_state.match_type,
                    referred_schema_is_current=(
                        catalog_state.referred_schema_is_current
                    ),
                )
            )
        foreign_keys[table] = tuple(sorted(table_foreign_keys))

    extensions = frozenset(
        connection.scalars(text("SELECT extname FROM pg_catalog.pg_extension")).all()
    )
    index_rows = connection.execute(
        text(
            """
            SELECT table_class.relname AS table_name,
                   index_class.relname AS indexname,
                   pg_catalog.pg_get_indexdef(index_meta.indexrelid) AS indexdef,
                   index_meta.indisvalid AS is_valid,
                   index_meta.indisready AS is_ready,
                   index_meta.indisunique AS is_unique,
                   EXISTS (
                     SELECT 1
                     FROM pg_catalog.pg_constraint constraint_meta
                     WHERE constraint_meta.conindid = index_meta.indexrelid
                       AND constraint_meta.contype IN ('p', 'u')
                   ) AS has_key_constraint_owner
            FROM pg_catalog.pg_index index_meta
            JOIN pg_catalog.pg_class index_class
              ON index_class.oid = index_meta.indexrelid
            JOIN pg_catalog.pg_class table_class
              ON table_class.oid = index_meta.indrelid
            JOIN pg_catalog.pg_namespace table_namespace
              ON table_namespace.oid = table_class.relnamespace
            WHERE table_namespace.nspname = :schema
            """
        ),
        {"schema": schema},
    ).mappings()
    indexes: dict[str, str] = {}
    index_states: dict[str, IndexState] = {}
    standalone_unique_index_lists: dict[str, list[str]] = {
        table: [] for table in table_names
    }
    for row in index_rows:
        indexes[row["indexname"]] = row["indexdef"]
        index_states[row["indexname"]] = IndexState(
            is_valid=bool(row["is_valid"]),
            is_ready=bool(row["is_ready"]),
        )
        if row["is_unique"] and not row["has_key_constraint_owner"]:
            standalone_unique_index_lists[row["table_name"]].append(
                row["indexname"]
            )
    standalone_unique_indexes = {
        table: tuple(sorted(index_names))
        for table, index_names in standalone_unique_index_lists.items()
    }
    return SchemaSnapshot(
        tables=MappingProxyType(
            {name: MappingProxyType(columns) for name, columns in tables.items()}
        ),
        column_nullability=MappingProxyType(
            {
                name: MappingProxyType(columns)
                for name, columns in column_nullability.items()
            }
        ),
        column_defaults=MappingProxyType(
            {
                name: MappingProxyType(columns)
                for name, columns in column_defaults.items()
            }
        ),
        identity_columns=frozenset(identity_columns),
        serial_sequences=MappingProxyType(
            {
                name: MappingProxyType(columns)
                for name, columns in serial_sequences.items()
            }
        ),
        primary_keys=MappingProxyType(primary_keys),
        unique_constraints=MappingProxyType(unique_constraints),
        foreign_keys=MappingProxyType(foreign_keys),
        restricted_constraints=MappingProxyType(restricted_constraints),
        standalone_unique_indexes=MappingProxyType(standalone_unique_indexes),
        extensions=extensions,
        indexes=MappingProxyType(indexes),
        index_states=MappingProxyType(index_states),
    )
