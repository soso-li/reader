from sqlalchemy import select
from sqlalchemy.orm import Session

from .article_fetch import _article_url
from .models import DELETED_SOURCE_STATUS, Source


def clean_source_url(value: str) -> str:
    url = value.strip()
    if not url:
        raise ValueError("RSS URL 不能为空")
    try:
        return _article_url(url)[3]
    except (UnicodeError, ValueError):
        raise ValueError("RSS URL 只支持有效且不含用户名或密码的 http 或 https 地址") from None


def source_by_url(session: Session, url: str, exclude_id: int | None = None) -> Source | None:
    # ponytail: O(n) over personal subscriptions; add a normalized_url column if source count makes this hot.
    for source in session.scalars(select(Source)).all():
        if source.status == DELETED_SOURCE_STATUS:
            continue
        if exclude_id is not None and source.id == exclude_id:
            continue
        try:
            if clean_source_url(source.url) == url:
                source.url = url
                return source
        except ValueError:
            continue
    return None
