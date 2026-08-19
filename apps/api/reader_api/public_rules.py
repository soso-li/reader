from __future__ import annotations

import json
import logging
import re
import tarfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO
from pathlib import PurePosixPath
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from sqlalchemy import select
from sqlalchemy.orm import Session

from .article_fetch import (
    FiveFiltersRule,
    FiveFiltersRules,
    fivefilters_rules_from_payload,
    load_fivefilters_rules,
)
from .article_selectors import validate_article_selector
from .fivefilters_compile import compile_rule_text
from .models import (
    AppSetting,
    DELETED_SOURCE_STATUS,
    RawEntry,
    Source,
    SourceEntryIdentity,
)


ACTIVE_RULES_KEY = "fivefilters_rules_snapshot"
LATEST_COMMIT_URL = (
    "https://api.github.com/repos/fivefilters/ftr-site-config/commits/master"
)
ARCHIVE_URL = "https://codeload.github.com/fivefilters/ftr-site-config/tar.gz/"
MAX_COMMIT_BYTES = 256 * 1024
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024
MAX_RULE_BYTES = 512 * 1024
MAX_UNCOMPRESSED_BYTES = 32 * 1024 * 1024
MAX_RULE_FILES = 5000
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
logger = logging.getLogger(__name__)


class PublicRuleUpdateError(RuntimeError):
    pass


@dataclass(frozen=True)
class ActivePublicRules:
    rules: FiveFiltersRules
    commit: str
    activated_at: datetime | None
    bundled: bool


@dataclass(frozen=True)
class CandidatePublicRules:
    commit: str
    payload: dict[str, object]
    rules: FiveFiltersRules


@dataclass(frozen=True)
class PublicRuleExtractionPreview:
    hostname: str
    title: str
    reading_html: str
    rss_characters: int
    webpage_characters: int
    method: str
    version: str
    adopted_webpage: bool
    matched_elements: int
    removed_elements: int
    diagnostics: tuple[str, ...]
    passed: bool


@dataclass(frozen=True)
class PublicRuleCheck:
    current: ActivePublicRules
    candidate: CandidatePublicRules
    rules_count: int
    skipped_count: int
    subscribed_domains: int
    covered_subscribed_domains: int
    changed_subscribed_domains: int
    tested_subscribed_domains: int
    invalid_subscribed_domains: tuple[str, ...]
    failed_subscribed_domains: tuple[str, ...]
    preview: PublicRuleExtractionPreview | None

    @property
    def passed(self) -> bool:
        return not (
            self.invalid_subscribed_domains
            or self.failed_subscribed_domains
        )

    @property
    def can_activate(self) -> bool:
        return self.passed and self.current.commit != self.candidate.commit


def active_public_rules(session: Session) -> ActivePublicRules:
    row = session.get(AppSetting, ACTIVE_RULES_KEY)
    if row is not None:
        try:
            return _active_rules_from_json(row.value)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            logger.exception("已保存的公共规则快照无效，回退内置版本")
    rules = load_fivefilters_rules()
    return ActivePublicRules(
        rules=rules,
        commit=_commit_from_version(rules.version),
        activated_at=None,
        bundled=True,
    )


@lru_cache(maxsize=4)
def _active_rules_from_json(value: str) -> ActivePublicRules:
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("公共规则快照格式无效")
    rules = fivefilters_rules_from_payload(payload)
    return ActivePublicRules(
        rules=rules,
        commit=_commit_from_version(rules.version),
        activated_at=(
            datetime.fromisoformat(str(payload["activated_at"]))
            if payload.get("activated_at")
            else None
        ),
        bundled=False,
    )


def _commit_from_version(version: str) -> str:
    prefix = "fivefilters@"
    return version[len(prefix) :] if version.startswith(prefix) else version


def check_public_rule_update(
    session: Session,
    previewer: Callable[
        [RawEntry, str, FiveFiltersRules],
        PublicRuleExtractionPreview,
    ],
) -> PublicRuleCheck:
    return _check_candidate(session, _download_candidate(), previewer)


def activate_public_rule_update(
    session: Session,
    commit: str,
    previewer: Callable[
        [RawEntry, str, FiveFiltersRules],
        PublicRuleExtractionPreview,
    ],
) -> ActivePublicRules:
    candidate = _download_candidate(commit)
    check = _check_candidate(session, candidate, previewer)
    if not check.passed:
        raise PublicRuleUpdateError("候选规则未通过已订阅域名测试")
    payload = dict(candidate.payload)
    payload["activated_at"] = datetime.now(timezone.utc).isoformat()
    row = session.get(AppSetting, ACTIVE_RULES_KEY) or AppSetting(
        key=ACTIVE_RULES_KEY
    )
    row.value = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    row.updated_at = datetime.now(timezone.utc)
    session.add(row)
    session.commit()
    return active_public_rules(session)


def _download_candidate(commit: str | None = None) -> CandidatePublicRules:
    resolved_commit = commit or _latest_commit()
    if COMMIT_RE.fullmatch(resolved_commit) is None:
        raise PublicRuleUpdateError("上游 commit 格式无效")
    archive = _read_url(f"{ARCHIVE_URL}{resolved_commit}", MAX_ARCHIVE_BYTES)
    payload = _compile_archive(archive, resolved_commit)
    return CandidatePublicRules(
        commit=resolved_commit,
        payload=payload,
        rules=fivefilters_rules_from_payload(payload),
    )


def _latest_commit() -> str:
    try:
        payload = json.loads(
            _read_url(LATEST_COMMIT_URL, MAX_COMMIT_BYTES).decode()
        )
        commit = str(payload["sha"]).lower()
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise PublicRuleUpdateError("无法读取上游 commit") from exc
    if COMMIT_RE.fullmatch(commit) is None:
        raise PublicRuleUpdateError("上游 commit 格式无效")
    return commit


def _read_url(url: str, max_bytes: int) -> bytes:
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "Reader/0.1 (+personal RSS reader)",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            declared = response.headers.get("Content-Length")
            if declared and int(declared) > max_bytes:
                raise PublicRuleUpdateError("公共规则下载超过大小上限")
            body = response.read(max_bytes + 1)
    except PublicRuleUpdateError:
        raise
    except Exception as exc:
        raise PublicRuleUpdateError("公共规则下载失败") from exc
    if len(body) > max_bytes:
        raise PublicRuleUpdateError("公共规则下载超过大小上限")
    return body


def _compile_archive(
    archive_bytes: bytes,
    commit: str,
) -> dict[str, object]:
    rules: dict[str, dict[str, list[str]]] = {}
    skipped: dict[str, str] = {}
    total_bytes = 0
    file_count = 0
    try:
        with tarfile.open(fileobj=BytesIO(archive_bytes), mode="r:gz") as archive:
            for member in archive:
                path = PurePosixPath(member.name)
                if (
                    not member.isfile()
                    or len(path.parts) != 2
                    or not path.name.endswith(".txt")
                ):
                    continue
                file_count += 1
                total_bytes += member.size
                if (
                    file_count > MAX_RULE_FILES
                    or member.size > MAX_RULE_BYTES
                    or total_bytes > MAX_UNCOMPRESSED_BYTES
                ):
                    raise PublicRuleUpdateError("公共规则归档超过安全上限")
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise PublicRuleUpdateError("公共规则归档读取失败")
                text = extracted.read(MAX_RULE_BYTES + 1).decode("utf-8-sig")
                hostname = path.name.removesuffix(".txt").lower()
                rule, reason = compile_rule_text(text)
                if rule is not None:
                    rules[hostname] = rule
                elif reason:
                    skipped[hostname] = reason
    except PublicRuleUpdateError:
        raise
    except (tarfile.TarError, UnicodeDecodeError, OSError) as exc:
        raise PublicRuleUpdateError("公共规则归档无效") from exc
    if not rules:
        raise PublicRuleUpdateError("公共规则归档没有可用规则")
    return {
        "version": f"fivefilters@{commit}",
        "rules": rules,
        "skipped": skipped,
    }


def _check_candidate(
    session: Session,
    candidate: CandidatePublicRules,
    previewer: Callable[
        [RawEntry, str, FiveFiltersRules],
        PublicRuleExtractionPreview,
    ],
) -> PublicRuleCheck:
    current = active_public_rules(session)
    samples = _subscribed_samples(session)
    covered = changed = 0
    invalid: list[str] = []
    tested = 0
    failed: list[str] = []
    preview: PublicRuleExtractionPreview | None = None
    for hostname, entry in samples.items():
        candidate_rule = candidate.rules.match(hostname)
        if candidate_rule is not None:
            covered += 1
            if not _rule_is_valid(candidate_rule):
                invalid.append(hostname)
        rule_changed = (
            candidate_rule != current.rules.match(hostname)
            or candidate.rules.skipped_reason(hostname)
            != current.rules.skipped_reason(hostname)
        )
        if rule_changed:
            changed += 1
            if hostname not in invalid:
                result = previewer(entry, hostname, candidate.rules)
                tested += 1
                preview = preview or result
                if not result.passed:
                    failed.append(result.hostname)
    return PublicRuleCheck(
        current=current,
        candidate=candidate,
        rules_count=len(candidate.rules.rules),
        skipped_count=len(candidate.rules.skipped),
        subscribed_domains=len(samples),
        covered_subscribed_domains=covered,
        changed_subscribed_domains=changed,
        tested_subscribed_domains=tested,
        invalid_subscribed_domains=tuple(invalid),
        failed_subscribed_domains=tuple(failed),
        preview=preview,
    )


def _subscribed_samples(session: Session) -> dict[str, RawEntry]:
    entries = session.scalars(
        select(RawEntry)
        .join(Source, Source.id == RawEntry.source_id)
        .join(
            SourceEntryIdentity,
            SourceEntryIdentity.id == RawEntry.source_entry_id,
        )
        .where(
            Source.status != DELETED_SOURCE_STATUS,
            RawEntry.url != "",
            RawEntry.revision_no
            == SourceEntryIdentity.current_revision_no,
        )
        .order_by(
            RawEntry.published_at.is_(None),
            RawEntry.published_at.desc(),
            RawEntry.fetched_at.desc(),
            RawEntry.id.desc(),
        )
    )
    samples: dict[str, RawEntry] = {}
    for entry in entries:
        try:
            hostname = (
                (urlsplit(entry.url).hostname or "")
                .encode("idna")
                .decode("ascii")
                .rstrip(".")
                .lower()
            )
        except UnicodeError:
            continue
        if hostname:
            samples.setdefault(hostname, entry)
    return dict(sorted(samples.items()))


def _rule_is_valid(rule: FiveFiltersRule) -> bool:
    try:
        for expression in (*rule.body, *rule.strip):
            validate_article_selector(f"xpath:{expression}")
    except ValueError:
        return False
    return True
