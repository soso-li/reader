from __future__ import annotations

import http.client
import ipaddress
import json
import queue
import re
import socket
import ssl
import threading
import time
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from email.message import Message
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import TypeVar
from urllib.parse import SplitResult, urljoin, urlsplit, urlunsplit

from lxml import etree, html as lxml_html

from .article_selectors import select_article_elements
from .digest import normalize_space, strip_html


TOTAL_FETCH_SECONDS = 15.0
MAX_REDIRECTS = 5
MAX_TRANSIENT_ATTEMPTS = 3
MAX_HTML_BYTES = 5 * 1024 * 1024
MIN_ARTICLE_CHARACTERS = 320
HTML_CONTENT_TYPES = frozenset({"text/html", "application/xhtml+xml"})
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_MIHOMO_FAKE_IP_NETWORK = ipaddress.ip_network("198.18.0.0/15")
REMOVED_FOR_QUALITY = (
    "script",
    "style",
    "noscript",
    "template",
    "form",
    "iframe",
    "object",
    "embed",
)
BLOCKED_PAGE_RE = re.compile(
    r"(?:"
    r"\bcaptcha\b|verify\s+you\s+are\s+human|"
    r"\bpage\s+not\s+found\b|\baccess\s+denied\b|\bforbidden\b|"
    r"\b(?:4\d\d|5\d\d)\s+(?:internal\s+server\s+error|service\s+unavailable|error)\b|"
    r"\b(?:internal\s+server\s+error|bad\s+gateway|gateway\s+timeout|service\s+unavailable)\b|"
    r"subscribe\s+to\s+continue|subscriber[- ]only|"
    r"登录|登入|验证码|人机验证|付费阅读|订阅后|会员专享|页面不存在|访问被拒绝"
    r")",
    re.I,
)
BLOCKED_LEAD_RE = re.compile(
    r"(?:"
    r"\bsign\s+in\s+to\s+continue\b|\blog\s+in\s+to\s+continue\b|"
    r"\bcaptcha\b|verify\s+you\s+are\s+human|"
    r"\bpage\s+not\s+found\b|\baccess\s+denied\b|\bforbidden\b|"
    r"\b(?:4\d\d|5\d\d)\s+(?:internal\s+server\s+error|service\s+unavailable|error)\b|"
    r"\b(?:internal\s+server\s+error|bad\s+gateway|gateway\s+timeout|service\s+unavailable)\b|"
    r"subscribe\s+to\s+continue|subscriber[- ]only|"
    r"人机验证|付费阅读|订阅后|会员专享|页面不存在|访问被拒绝"
    r")",
    re.I,
)
BLOCKED_MARKER_RE = re.compile(
    r"(?:captcha|paywall|login|sign[-_]?in(?![a-z])|access[-_]?denied)",
    re.I,
)
BLOCKED_MARKER_TAGS = frozenset({"article", "aside", "div", "main", "section"})
UNSAFE_URL_CHARACTER_RE = re.compile(r"[\x00-\x20\x7f]")
FIVEFILTERS_PATH = Path(__file__).with_name("fivefilters_rules.json")
_HostnameValue = TypeVar("_HostnameValue")


@dataclass(frozen=True)
class FiveFiltersRule:
    body: tuple[str, ...]
    strip: tuple[str, ...]
    strip_id_or_class: tuple[str, ...]


@dataclass(frozen=True)
class FiveFiltersRules:
    version: str
    rules: Mapping[str, FiveFiltersRule]
    skipped: Mapping[str, str]

    def match(self, hostname: str) -> FiveFiltersRule | None:
        return _hostname_match(self.rules, hostname)

    def skipped_reason(self, hostname: str) -> str:
        return _hostname_match(self.skipped, hostname) or ""


@dataclass(frozen=True)
class PageFetchResult:
    succeeded: bool
    html: str
    final_url: str
    diagnostic: str
    attempts: int
    redirects: int


@dataclass(frozen=True)
class ArticleFetchResult:
    content_html: str
    content_text: str
    body_source: str
    web_fetch_status: str
    method: str
    version: str
    final_url: str
    diagnostics: tuple[str, ...]
    matched_elements: int = 0
    removed_elements: int = 0
    candidate_characters: int = 0

    @classmethod
    def rss_failed(
        cls,
        rss_text: str,
        *,
        final_url: str = "",
        diagnostics: tuple[str, ...] = (),
        matched_elements: int = 0,
        removed_elements: int = 0,
        candidate_characters: int = 0,
    ) -> ArticleFetchResult:
        return cls(
            content_html="",
            content_text=rss_text,
            body_source="rss",
            web_fetch_status="failed",
            method="rss",
            version="rss-v1",
            final_url=final_url,
            diagnostics=diagnostics,
            matched_elements=matched_elements,
            removed_elements=removed_elements,
            candidate_characters=candidate_characters,
        )


class _FetchFailure(Exception):
    pass


class _ConnectionResponse:
    def __init__(
        self,
        connection: http.client.HTTPConnection,
        response: http.client.HTTPResponse,
    ) -> None:
        self.connection = connection
        self.response = response
        self.status = response.status
        self.headers = response.headers

    def read(self, size: int) -> bytes:
        return self.response.read(size)

    def set_timeout(self, seconds: float) -> None:
        if self.connection.sock is not None:
            self.connection.sock.settimeout(seconds)

    def close(self) -> None:
        self.response.close()
        self.connection.close()


@lru_cache(maxsize=1)
def load_fivefilters_rules() -> FiveFiltersRules:
    return fivefilters_rules_from_payload(
        json.loads(FIVEFILTERS_PATH.read_text(encoding="utf-8"))
    )


def fivefilters_rules_from_payload(payload: Mapping[str, object]) -> FiveFiltersRules:
    rule_payload = payload["rules"]
    skipped_payload = payload["skipped"]
    if not isinstance(rule_payload, Mapping) or not isinstance(
        skipped_payload, Mapping
    ):
        raise ValueError("公共规则快照格式无效")
    return FiveFiltersRules(
        version=str(payload["version"]),
        rules={
            hostname: FiveFiltersRule(
                body=tuple(values["body"]),
                strip=tuple(values["strip"]),
                strip_id_or_class=tuple(values["strip_id_or_class"]),
            )
            for hostname, values in rule_payload.items()
            if isinstance(hostname, str) and isinstance(values, Mapping)
        },
        skipped={
            str(hostname): str(reason)
            for hostname, reason in skipped_payload.items()
        },
    )


def fetch_html(
    url: str,
    *,
    resolver: Callable[[str, int, float], tuple[str, ...]] | None = None,
    request: Callable[..., object] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> PageFetchResult:
    resolver = resolver or resolve_addresses
    request = request or _request_once
    deadline = clock() + TOTAL_FETCH_SECONDS
    current_url = url
    attempts = 0
    redirects = 0
    transient_attempts = 0

    while True:
        try:
            parsed, hostname, port, normalized_url = _article_url(current_url)
        except (ValueError, UnicodeError):
            return _page_failure(current_url, "invalid_url", attempts, redirects)
        remaining = deadline - clock()
        if remaining <= 0:
            return _page_failure(
                normalized_url,
                "time_budget_exhausted",
                attempts,
                redirects,
            )
        try:
            addresses = (
                (hostname,)
                if _is_ip_literal(hostname)
                else resolver(hostname, port, remaining)
            )
            public_addresses = _validated_public_addresses(addresses)
        except TimeoutError:
            return _page_failure(
                normalized_url,
                "time_budget_exhausted",
                attempts,
                redirects,
            )
        except ValueError:
            return _page_failure(
                normalized_url,
                "ssrf_blocked",
                attempts,
                redirects,
            )
        except OSError:
            return _page_failure(
                normalized_url,
                "dns_failed",
                attempts,
                redirects,
            )

        remaining = deadline - clock()
        if remaining <= 0:
            return _page_failure(
                normalized_url,
                "time_budget_exhausted",
                attempts,
                redirects,
            )
        attempts += 1
        ssl_context = ssl.create_default_context() if parsed.scheme == "https" else None
        try:
            response = request(
                url=normalized_url,
                address=public_addresses[0],
                timeout=remaining,
                ssl_context=ssl_context,
            )
        except ssl.SSLCertVerificationError:
            return _page_failure(
                normalized_url,
                "tls_verification_failed",
                attempts,
                redirects,
            )
        except ssl.SSLError:
            return _page_failure(
                normalized_url,
                "tls_failed",
                attempts,
                redirects,
            )
        except (TimeoutError, ConnectionError, OSError):
            transient_attempts += 1
            diagnostic = _transient_failure_diagnostic(
                transient_attempts, deadline, clock
            )
            if diagnostic:
                return _page_failure(
                    normalized_url,
                    diagnostic,
                    attempts,
                    redirects,
                )
            continue

        try:
            status = int(getattr(response, "status"))
            headers = getattr(response, "headers")
            if status in REDIRECT_STATUSES:
                location = _header(headers, "Location")
                if not location:
                    return _page_failure(
                        normalized_url,
                        "redirect_missing_location",
                        attempts,
                        redirects,
                    )
                if redirects >= MAX_REDIRECTS:
                    return _page_failure(
                        normalized_url,
                        "too_many_redirects",
                        attempts,
                        redirects,
                    )
                redirects += 1
                current_url = urljoin(normalized_url, location)
                continue
            if status in TRANSIENT_STATUSES:
                transient_attempts += 1
                diagnostic = _transient_failure_diagnostic(
                    transient_attempts, deadline, clock
                )
                if diagnostic:
                    return _page_failure(
                        normalized_url,
                        diagnostic,
                        attempts,
                        redirects,
                    )
                continue
            if status != 200:
                return _page_failure(
                    normalized_url,
                    f"http_status_{status}",
                    attempts,
                    redirects,
                )
            content_type = _header(headers, "Content-Type")
            if _media_type(content_type) not in HTML_CONTENT_TYPES:
                return _page_failure(
                    normalized_url,
                    "content_type_not_html",
                    attempts,
                    redirects,
                )
            try:
                body = _read_limited_body(
                    response,
                    _header(headers, "Content-Encoding"),
                    deadline,
                    clock,
                )
                html = _decode_html(body, content_type)
            except _FetchFailure as exc:
                if str(exc) == "transient_read_failed":
                    transient_attempts += 1
                    diagnostic = _transient_failure_diagnostic(
                        transient_attempts, deadline, clock
                    )
                    if diagnostic:
                        return _page_failure(
                            normalized_url,
                            diagnostic,
                            attempts,
                            redirects,
                        )
                    continue
                return _page_failure(
                    normalized_url,
                    str(exc),
                    attempts,
                    redirects,
                )
            return PageFetchResult(
                succeeded=True,
                html=html,
                final_url=normalized_url,
                diagnostic="",
                attempts=attempts,
                redirects=redirects,
            )
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()


def fetch_article(
    url: str,
    rss_text: str,
    *,
    article_selector: str | None = None,
    remove_selector: str | None = None,
    page_fetcher: Callable[[str], PageFetchResult] = fetch_html,
    rules: FiveFiltersRules | None = None,
    trafilatura_extractor: Callable[[str, str], str] | None = None,
) -> ArticleFetchResult:
    page = page_fetcher(url)
    if not page.succeeded:
        return ArticleFetchResult.rss_failed(
            rss_text,
            final_url=page.final_url,
            diagnostics=(page.diagnostic,),
        )
    try:
        document = lxml_html.fromstring(page.html, base_url=page.final_url)
    except (ValueError, etree.ParserError):
        return ArticleFetchResult.rss_failed(
            rss_text,
            final_url=page.final_url,
            diagnostics=("html_parse_failed",),
        )

    if article_selector:
        try:
            removed = _remove_selected(document, remove_selector)
            selected = _deduplicate_nested(
                select_article_elements(document, article_selector)
            )
        except ValueError:
            return ArticleFetchResult.rss_failed(
                rss_text,
                final_url=page.final_url,
                diagnostics=("manual_selector_invalid",),
            )
        if not selected:
            return ArticleFetchResult.rss_failed(
                rss_text,
                final_url=page.final_url,
                diagnostics=("manual_no_match",),
                removed_elements=removed,
            )
        fragment = _serialize_elements(selected)
        candidate_characters = _candidate_characters(
            fragment,
            page.final_url,
        )
        accepted = _accepted_article(
            fragment,
            page.final_url,
            document,
        )
        if accepted is None:
            return ArticleFetchResult.rss_failed(
                rss_text,
                final_url=page.final_url,
                diagnostics=("manual_quality_rejected",),
                matched_elements=len(selected),
                removed_elements=removed,
                candidate_characters=candidate_characters,
            )
        return _webpage_result(
            accepted,
            method="manual",
            version="manual-selector-v1",
            final_url=page.final_url,
            matched_elements=len(selected),
            removed_elements=removed,
        )

    working_html = page.html
    remove_only_count = 0
    if remove_selector:
        try:
            remove_only_count = _remove_selected(document, remove_selector)
        except ValueError:
            return ArticleFetchResult.rss_failed(
                rss_text,
                final_url=page.final_url,
                diagnostics=("manual_selector_invalid",),
            )
        working_html = lxml_html.tostring(
            document,
            encoding="unicode",
            method="html",
        )

    rules = rules or load_fivefilters_rules()
    diagnostics: list[str] = []
    candidate_characters = 0
    matched_elements = 0
    removed_elements = 0
    hostname = (
        (urlsplit(page.final_url).hostname or "")
        .encode("idna")
        .decode("ascii")
        .rstrip(".")
        .lower()
    )
    public_rule = rules.match(hostname)
    if public_rule is not None:
        fragment, matched, removed, diagnostic = _extract_fivefilters(
            working_html,
            page.final_url,
            public_rule,
        )
        removed += remove_only_count
        if fragment:
            matched_elements = matched
            removed_elements = removed
            candidate_characters = _candidate_characters(
                fragment,
                page.final_url,
            )
            public_document = lxml_html.fromstring(
                working_html,
                base_url=page.final_url,
            )
            accepted = _accepted_article(
                fragment,
                page.final_url,
                public_document,
            )
            if accepted is not None:
                return _webpage_result(
                    accepted,
                    method="fivefilters",
                    version=rules.version,
                    final_url=page.final_url,
                    diagnostics=tuple(diagnostics),
                    matched_elements=matched,
                    removed_elements=removed,
                )
            diagnostic = "fivefilters_quality_rejected"
        diagnostics.append(diagnostic)
    elif rules.skipped_reason(hostname):
        diagnostics.append("fivefilters_hard_dependency")

    extractor = trafilatura_extractor or _extract_with_trafilatura
    extracted = extractor(working_html, page.final_url)
    if extracted:
        candidate_characters = max(
            candidate_characters,
            _candidate_characters(extracted, page.final_url),
        )
    accepted = (
        _accepted_article(extracted, page.final_url, document) if extracted else None
    )
    if accepted is not None:
        return _webpage_result(
            accepted,
            method="trafilatura",
            version=_trafilatura_version(),
            final_url=page.final_url,
            diagnostics=tuple(diagnostics),
            removed_elements=remove_only_count,
        )
    diagnostics.append("quality_rejected" if extracted else "trafilatura_no_match")
    return ArticleFetchResult.rss_failed(
        rss_text,
        final_url=page.final_url,
        diagnostics=tuple(diagnostics),
        matched_elements=matched_elements,
        removed_elements=max(removed_elements, remove_only_count),
        candidate_characters=candidate_characters,
    )


def resolve_addresses(hostname: str, port: int, timeout: float) -> tuple[str, ...]:
    result: queue.Queue[object] = queue.Queue(maxsize=1)

    def resolve() -> None:
        try:
            result.put(
                socket.getaddrinfo(
                    hostname,
                    port,
                    type=socket.SOCK_STREAM,
                )
            )
        except BaseException as exc:
            result.put(exc)

    thread = threading.Thread(target=resolve, daemon=True)
    thread.start()
    thread.join(max(timeout, 0))
    if thread.is_alive():
        raise TimeoutError
    outcome = result.get_nowait()
    if isinstance(outcome, BaseException):
        raise outcome
    addresses = tuple(
        dict.fromkeys(
            str(sockaddr[0]) for _family, _type, _proto, _canonname, sockaddr in outcome
        )
    )
    if not addresses:
        raise OSError("DNS 未返回地址")
    return addresses


def _article_url(
    url: str,
) -> tuple[SplitResult, str, int, str]:
    if (
        not url
        or len(url) > 8192
        or url != url.strip()
        or UNSAFE_URL_CHARACTER_RE.search(url)
    ):
        raise ValueError("URL 无效")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("URL 无效")
    hostname = parsed.hostname.encode("idna").decode("ascii").rstrip(".").lower()
    if not hostname:
        raise ValueError("URL 无效")
    parsed_port = parsed.port
    port = (
        parsed_port
        if parsed_port is not None
        else (443 if parsed.scheme == "https" else 80)
    )
    if port < 1:
        raise ValueError("URL 端口无效")
    host_for_url = f"[{hostname}]" if ":" in hostname else hostname
    default_port = (parsed.scheme == "https" and port == 443) or (
        parsed.scheme == "http" and port == 80
    )
    authority = host_for_url if default_port else f"{host_for_url}:{port}"
    normalized_url = urlunsplit(
        (
            parsed.scheme,
            authority,
            parsed.path or "/",
            parsed.query,
            "",
        )
    )
    return parsed, hostname, port, normalized_url


def _is_ip_literal(hostname: str) -> bool:
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return True


def _validated_public_addresses(addresses: tuple[str, ...]) -> tuple[str, ...]:
    if not addresses:
        raise ValueError("DNS 未返回地址")
    validated: list[str] = []
    for value in addresses:
        address = ipaddress.ip_address(value)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        is_mihomo_fake_ip = (
            isinstance(address, ipaddress.IPv4Address)
            and address in _MIHOMO_FAKE_IP_NETWORK
        )
        if address.is_multicast or (
            not address.is_global and not is_mihomo_fake_ip
        ):
            raise ValueError("文章地址不是公共地址")
        validated.append(str(address))
    return tuple(dict.fromkeys(validated))


def _request_once(
    *,
    url: str,
    address: str,
    timeout: float,
    ssl_context: ssl.SSLContext | None,
    headers: Mapping[str, str] | None = None,
) -> _ConnectionResponse:
    parsed, hostname, port, _normalized_url = _article_url(url)
    deadline = time.monotonic() + timeout
    raw_socket = socket.create_connection((address, port), timeout=timeout)
    connection = http.client.HTTPConnection(hostname, port, timeout=timeout)
    try:
        raw_socket.settimeout(_remaining_timeout(deadline))
        connection.sock = (
            ssl_context.wrap_socket(raw_socket, server_hostname=hostname)
            if ssl_context is not None
            else raw_socket
        )
        connection.sock.settimeout(_remaining_timeout(deadline))
        target = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        connection.request(
            "GET",
            target,
            headers=headers
            or {
                "User-Agent": "Reader/0.1 (+personal RSS reader)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "close",
            },
        )
        connection.sock.settimeout(_remaining_timeout(deadline))
        return _ConnectionResponse(connection, connection.getresponse())
    except BaseException:
        connection.close()
        raw_socket.close()
        raise


def _read_limited_body(
    response: object,
    content_encoding: str,
    deadline: float,
    clock: Callable[[], float],
) -> bytes:
    encoding = content_encoding.strip().lower()
    if encoding in {"", "identity"}:
        decompressor = None
    elif encoding == "gzip":
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    elif encoding == "deflate":
        decompressor = zlib.decompressobj()
    else:
        raise _FetchFailure("content_encoding_unsupported")

    body = bytearray()
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise _FetchFailure("time_budget_exhausted")
        set_timeout = getattr(response, "set_timeout", None)
        if callable(set_timeout):
            set_timeout(remaining)
        try:
            chunk = response.read(64 * 1024)  # type: ignore[attr-defined]
        except (TimeoutError, OSError, http.client.IncompleteRead) as exc:
            raise _FetchFailure("transient_read_failed") from exc
        if not chunk:
            break
        try:
            decoded = (
                chunk
                if decompressor is None
                else decompressor.decompress(
                    chunk,
                    MAX_HTML_BYTES + 1 - len(body),
                )
            )
        except zlib.error as exc:
            raise _FetchFailure("content_decode_failed") from exc
        body.extend(decoded)
        if len(body) > MAX_HTML_BYTES or (
            decompressor is not None and bool(decompressor.unconsumed_tail)
        ):
            raise _FetchFailure("body_too_large")
    if decompressor is not None:
        try:
            body.extend(decompressor.flush(MAX_HTML_BYTES + 1 - len(body)))
        except zlib.error as exc:
            raise _FetchFailure("content_decode_failed") from exc
        if not decompressor.eof or decompressor.unused_data:
            raise _FetchFailure("content_decode_failed")
    if len(body) > MAX_HTML_BYTES:
        raise _FetchFailure("body_too_large")
    return bytes(body)


def _decode_html(body: bytes, content_type: str) -> str:
    message = Message()
    message["content-type"] = content_type
    charset = message.get_content_charset() or "utf-8-sig"
    try:
        return body.decode(charset, "strict")
    except (LookupError, UnicodeDecodeError) as exc:
        raise _FetchFailure("charset_decode_failed") from exc


def _media_type(content_type: str) -> str:
    return content_type.partition(";")[0].strip().lower()


def _header(headers: object, name: str) -> str:
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return ""
    value = str(getter(name, "") or "").strip()
    return "" if "\r" in value or "\n" in value else value


def _page_failure(
    final_url: str,
    diagnostic: str,
    attempts: int,
    redirects: int,
) -> PageFetchResult:
    return PageFetchResult(
        succeeded=False,
        html="",
        final_url=final_url,
        diagnostic=diagnostic,
        attempts=attempts,
        redirects=redirects,
    )


def _transient_failure_diagnostic(
    transient_attempts: int,
    deadline: float,
    clock: Callable[[], float],
) -> str:
    if clock() >= deadline:
        return "time_budget_exhausted"
    if transient_attempts >= MAX_TRANSIENT_ATTEMPTS:
        return "transient_retries_exhausted"
    return ""


def _remaining_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError
    return remaining


def _hostname_match(
    values: Mapping[str, _HostnameValue],
    hostname: str,
) -> _HostnameValue | None:
    hostname = hostname.rstrip(".").lower()
    if hostname in values:
        return values[hostname]
    if hostname.startswith("www.") and hostname[4:] in values:
        return values[hostname[4:]]
    labels = hostname.split(".")
    for index in range(1, len(labels) - 1):
        wildcard = "." + ".".join(labels[index:])
        if hostname == f"www{wildcard}":
            continue
        if wildcard in values:
            return values[wildcard]
    return None


def _remove_selected(
    document: etree._Element,
    selector: str | None,
) -> int:
    if not selector:
        return 0
    matches = _deduplicate_nested(select_article_elements(document, selector))
    return _remove_elements(matches)


def _remove_elements(elements: list[etree._Element]) -> int:
    removed = 0
    for element in elements:
        parent = element.getparent()
        if parent is not None:
            tail = element.tail
            previous = element.getprevious()
            parent.remove(element)
            if tail:
                if previous is None:
                    parent.text = (parent.text or "") + tail
                else:
                    previous.tail = (previous.tail or "") + tail
            removed += 1
    return removed


def _deduplicate_nested(
    elements: list[etree._Element],
) -> list[etree._Element]:
    selected = set(elements)
    return [
        element
        for element in elements
        if not any(ancestor in selected for ancestor in element.iterancestors())
    ]


def _serialize_elements(elements: list[etree._Element]) -> str:
    return "".join(
        lxml_html.tostring(
            element,
            encoding="unicode",
            method="html",
        )
        for element in elements
    )


def _extract_fivefilters(
    html: str,
    final_url: str,
    rule: FiveFiltersRule,
) -> tuple[str, int, int, str]:
    try:
        document = lxml_html.fromstring(html, base_url=final_url)
        removed = 0
        for expression in rule.strip:
            matches = _deduplicate_nested(
                select_article_elements(document, f"xpath:{expression}")
            )
            removed += _remove_elements(matches)
        removed += _remove_id_or_class(
            document,
            rule.strip_id_or_class,
        )
        for expression in rule.body:
            selected = _deduplicate_nested(
                select_article_elements(document, f"xpath:{expression}")
            )
            if selected:
                return (
                    _serialize_elements(selected),
                    len(selected),
                    removed,
                    "",
                )
    except (ValueError, etree.ParserError):
        return "", 0, 0, "fivefilters_invalid_rule"
    return "", 0, removed, "fivefilters_no_match"


def _remove_id_or_class(
    document: etree._Element,
    needles: tuple[str, ...],
) -> int:
    if not needles:
        return 0
    matches = [
        element
        for element in document.iter()
        if isinstance(element.tag, str)
        and any(
            needle in (element.get("id") or "")
            or needle in (element.get("class") or "")
            for needle in needles
        )
    ]
    return _remove_elements(_deduplicate_nested(matches))


def _accepted_article(
    fragment: str,
    final_url: str,
    source_document: etree._Element,
) -> tuple[str, str] | None:
    if "<" not in fragment:
        fragment = f"<p>{escape(fragment)}</p>"
    try:
        document = lxml_html.fragment_fromstring(
            fragment,
            create_parent="div",
            base_url=final_url,
        )
    except (ValueError, etree.ParserError):
        return None
    for tag in REMOVED_FOR_QUALITY:
        _remove_elements(list(document.iter(tag)))
    safe_fragment = "".join(
        lxml_html.tostring(
            child,
            encoding="unicode",
            method="html",
        )
        for child in document
    )
    plain_text = normalize_space(document.text_content())
    if len(plain_text) < MIN_ARTICLE_CHARACTERS or _looks_blocked(
        source_document, document, plain_text
    ):
        return None
    return safe_fragment, strip_html(safe_fragment, final_url)


def _candidate_characters(fragment: str, final_url: str) -> int:
    return len(normalize_space(strip_html(fragment, final_url)))


def _looks_blocked(
    source_document: etree._Element,
    extracted_document: etree._Element,
    plain_text: str,
) -> bool:
    title = normalize_space(" ".join(source_document.xpath("//title//text()")))
    if BLOCKED_PAGE_RE.search(title):
        return True
    if extracted_document.xpath(
        "//input[translate(@type, 'PASSWORD', 'password')='password']"
    ):
        return True
    for element in extracted_document.iter():
        if element.tag not in BLOCKED_MARKER_TAGS:
            continue
        marker = f"{element.get('id') or ''} {element.get('class') or ''}"
        if BLOCKED_MARKER_RE.search(marker):
            return True
    # ponytail: this is the requested simple gate; calibrate only after labeled false positives appear.
    lead = plain_text[:800]
    return bool(BLOCKED_LEAD_RE.search(lead)) or (
        len(plain_text) < 1600 and bool(BLOCKED_PAGE_RE.search(lead))
    )


def _extract_with_trafilatura(html: str, url: str) -> str:
    try:
        import trafilatura

        return (
            trafilatura.extract(
                html,
                url=url,
                output_format="html",
                include_tables=True,
                include_images=True,
                include_formatting=True,
                include_links=True,
                favor_precision=True,
            )
            or ""
        )
    except Exception:
        return ""


def _trafilatura_version() -> str:
    try:
        import trafilatura

        return f"trafilatura@{trafilatura.__version__}"
    except Exception:
        return "trafilatura"


def _webpage_result(
    accepted: tuple[str, str],
    *,
    method: str,
    version: str,
    final_url: str,
    diagnostics: tuple[str, ...] = (),
    matched_elements: int = 0,
    removed_elements: int = 0,
) -> ArticleFetchResult:
    return ArticleFetchResult(
        content_html=accepted[0],
        content_text=accepted[1],
        body_source="webpage",
        web_fetch_status="succeeded",
        method=method,
        version=version,
        final_url=final_url,
        diagnostics=diagnostics,
        matched_elements=matched_elements,
        removed_elements=removed_elements,
        candidate_characters=len(normalize_space(accepted[1])),
    )
