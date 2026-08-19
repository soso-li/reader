from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import re
from collections.abc import Iterator

from sqlalchemy import delete, exists, func, insert, literal, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.elements import ColumnElement

from .models import (
    ContentItem,
    DELETED_SOURCE_STATUS,
    FilterMatch,
    FilterRule,
    Source,
)


FILTER_MATCH_TYPES = {"literal", "regex"}
FILTER_PATTERN_MAX_LENGTH = 500
FILTER_PREVIEW_LIMIT = 20
FILTER_STATEMENT_TIMEOUT = "5s"


class FilterRuleError(ValueError):
    pass


class FilterRuleExecutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class FilterPreview:
    count: int
    items: tuple[ContentItem, ...]


def validate_filter_rule(match_type: str, pattern: str) -> str:
    value = pattern.strip()
    if match_type not in FILTER_MATCH_TYPES:
        raise FilterRuleError("不支持的过滤匹配方式")
    if not value:
        raise FilterRuleError("过滤表达式不能为空")
    if len(value) > FILTER_PATTERN_MAX_LENGTH:
        raise FilterRuleError(f"过滤表达式不能超过 {FILTER_PATTERN_MAX_LENGTH} 个字符")
    if match_type == "regex":
        try:
            re.compile(value)
        except re.error as exc:
            raise FilterRuleError(f"正则表达式无效：{exc.msg}") from None
    return value


def active_filter_match_exists(
    content_item_id: ColumnElement[int] = ContentItem.id,
) -> ColumnElement[bool]:
    return exists(
        select(literal(1))
        .select_from(FilterMatch)
        .join(FilterRule, FilterRule.id == FilterMatch.rule_id)
        .where(
            FilterMatch.content_item_id == content_item_id,
            FilterRule.enabled.is_(True),
        )
    )


def unfiltered_content_clause(
    content_item_id: ColumnElement[int] = ContentItem.id,
) -> ColumnElement[bool]:
    return ~active_filter_match_exists(content_item_id)


def preview_filter_rule(
    session: Session,
    *,
    source_id: int | None,
    match_type: str,
    pattern: str,
    limit: int = FILTER_PREVIEW_LIMIT,
) -> FilterPreview:
    value = validate_filter_rule(match_type, pattern)
    stmt = (
        select(ContentItem, func.count().over().label("match_count"))
        .options(joinedload(ContentItem.source))
        .where(
            _rule_scope_clause(source_id),
            _rule_match_clause(match_type, value),
        )
    )
    stmt = stmt.order_by(
        ContentItem.published_at.desc().nullslast(), ContentItem.id.desc()
    ).limit(max(1, min(limit, FILTER_PREVIEW_LIMIT)))
    try:
        with _filter_statement_timeout(session):
            rows = session.execute(stmt).all()
    except DBAPIError as exc:
        raise _execution_error(exc) from None
    return FilterPreview(
        count=int(rows[0].match_count) if rows else 0,
        items=tuple(row[0] for row in rows),
    )


def rebuild_filter_rule_matches(session: Session, rule: FilterRule) -> int:
    value = validate_filter_rule(rule.match_type, rule.pattern)
    matching_ids = select(literal(rule.id), ContentItem.id).where(
        _rule_scope_clause(rule.source_id),
        _rule_match_clause(rule.match_type, value),
    )
    try:
        with _filter_statement_timeout(session):
            session.execute(delete(FilterMatch).where(FilterMatch.rule_id == rule.id))
            session.execute(
                insert(FilterMatch).from_select(
                    [FilterMatch.rule_id, FilterMatch.content_item_id], matching_ids
                )
            )
            return int(
                session.scalar(
                    select(func.count()).select_from(FilterMatch).where(
                        FilterMatch.rule_id == rule.id
                    )
                )
                or 0
            )
    except DBAPIError as exc:
        raise _execution_error(exc) from None


def refresh_filter_matches_for_items(
    session: Session, item_ids: list[int] | tuple[int, ...] | set[int]
) -> None:
    ids = sorted({int(item_id) for item_id in item_ids if item_id})
    if not ids:
        return
    try:
        with _filter_statement_timeout(session):
            rules = session.scalars(
                select(FilterRule)
                .where(FilterRule.enabled.is_(True))
                .order_by(FilterRule.id)
            ).all()
            session.execute(
                delete(FilterMatch).where(FilterMatch.content_item_id.in_(ids))
            )
            for rule in rules:
                value = validate_filter_rule(rule.match_type, rule.pattern)
                matching_ids = select(literal(rule.id), ContentItem.id).where(
                    ContentItem.id.in_(ids),
                    _rule_scope_clause(rule.source_id),
                    _rule_match_clause(rule.match_type, value),
                )
                session.execute(
                    insert(FilterMatch).from_select(
                        [FilterMatch.rule_id, FilterMatch.content_item_id], matching_ids
                    )
                )
    except DBAPIError as exc:
        raise _execution_error(exc) from None


def active_filter_labels_for_items(
    session: Session, item_ids: list[int] | tuple[int, ...] | set[int]
) -> dict[int, list[str]]:
    ids = sorted({int(item_id) for item_id in item_ids if item_id})
    if not ids:
        return {}
    rows = session.execute(
        select(FilterMatch.content_item_id, FilterRule.match_type, FilterRule.pattern)
        .join(FilterRule, FilterRule.id == FilterMatch.rule_id)
        .where(
            FilterMatch.content_item_id.in_(ids),
            FilterRule.enabled.is_(True),
        )
        .order_by(FilterMatch.content_item_id, FilterRule.id)
    ).all()
    labels: dict[int, list[str]] = {}
    for item_id, match_type, pattern in rows:
        prefix = "正则" if match_type == "regex" else "关键词"
        labels.setdefault(item_id, []).append(f"{prefix}：{pattern}")
    return labels


def _rule_scope_clause(source_id: int | None) -> ColumnElement[bool]:
    live_source = ContentItem.source.has(Source.status != DELETED_SOURCE_STATUS)
    return (
        live_source
        if source_id is None
        else live_source & (ContentItem.source_id == source_id)
    )


def _rule_match_clause(match_type: str, pattern: str) -> ColumnElement[bool]:
    searchable = func.coalesce(ContentItem.title, "") + "\n" + func.coalesce(
        ContentItem.summary, ""
    ) + "\n" + func.coalesce(ContentItem.content_text, "")
    if match_type == "regex":
        return searchable.regexp_match(pattern, flags="i")
    return func.lower(searchable).contains(pattern.lower(), autoescape=True)


@contextmanager
def _filter_statement_timeout(session: Session) -> Iterator[None]:
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        yield
        return
    previous = str(session.scalar(text("SELECT current_setting('statement_timeout')")))
    session.execute(
        text("SELECT set_config('statement_timeout', :value, true)"),
        {"value": FILTER_STATEMENT_TIMEOUT},
    )
    try:
        yield
    except Exception:
        # PostgreSQL clears SET LOCAL on rollback; a statement timeout leaves the
        # transaction aborted, so restoring it here would hide the original error.
        raise
    else:
        session.execute(
            text("SELECT set_config('statement_timeout', :value, true)"),
            {"value": previous},
        )


def _execution_error(exc: DBAPIError) -> FilterRuleExecutionError:
    message = str(getattr(exc, "orig", exc)).lower()
    if "statement timeout" in message or "canceling statement" in message:
        return FilterRuleExecutionError(
            "匹配超过 5 秒，请缩小来源范围或简化过滤表达式"
        )
    return FilterRuleExecutionError("过滤表达式无法执行，请检查语法")
