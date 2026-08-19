from __future__ import annotations

import http.client
import ssl
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.parse import urljoin

from .article_fetch import (
    MAX_REDIRECTS,
    REDIRECT_STATUSES,
    _article_url,
    _header,
    _is_ip_literal,
    _request_once,
    _validated_public_addresses,
    resolve_addresses,
)


@dataclass(frozen=True, slots=True)
class PublicFetchResult:
    succeeded: bool
    body: bytes
    content_type: str
    final_url: str
    diagnostic: str


def fetch_public_bytes(
    url: str,
    *,
    max_bytes: int,
    trusted_origin: str = "",
    timeout_seconds: float = 10.0,
    headers: Mapping[str, str] | None = None,
    resolver: Callable[[str, int, float], tuple[str, ...]] | None = None,
    request: Callable[..., object] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> PublicFetchResult:
    resolver = resolver or resolve_addresses
    request = request or _request_once
    trusted_origin_parts: tuple[str, str, int] | None = None
    if trusted_origin:
        try:
            trusted_parsed, trusted_hostname, trusted_port, _ = _article_url(
                trusted_origin
            )
            trusted_origin_parts = (
                trusted_parsed.scheme,
                trusted_hostname,
                trusted_port,
            )
        except (ValueError, UnicodeError):
            pass
    deadline = clock() + timeout_seconds
    current_url = url
    redirects = 0

    while True:
        try:
            parsed, hostname, port, normalized_url = _article_url(current_url)
        except (ValueError, UnicodeError):
            return _failure(current_url, "invalid_url")
        remaining = deadline - clock()
        if remaining <= 0:
            return _failure(normalized_url, "time_budget_exhausted")
        try:
            addresses = (
                (hostname,)
                if _is_ip_literal(hostname)
                else resolver(hostname, port, remaining)
            )
            validated_addresses = (
                addresses
                if trusted_origin_parts == (parsed.scheme, hostname, port)
                else _validated_public_addresses(addresses)
            )
        except ValueError:
            return _failure(normalized_url, "ssrf_blocked")
        except TimeoutError:
            return _failure(normalized_url, "time_budget_exhausted")
        except OSError:
            return _failure(normalized_url, "dns_failed")

        remaining = deadline - clock()
        if remaining <= 0:
            return _failure(normalized_url, "time_budget_exhausted")
        try:
            response = request(
                url=normalized_url,
                address=validated_addresses[0],
                timeout=remaining,
                ssl_context=(
                    ssl.create_default_context()
                    if parsed.scheme == "https"
                    else None
                ),
                headers=headers
                or {
                    "User-Agent": "Reader/0.1 (+personal RSS reader)",
                    "Accept": "*/*",
                    "Accept-Encoding": "identity",
                    "Connection": "close",
                },
            )
        except (ssl.SSLCertVerificationError, ssl.SSLError):
            return _failure(normalized_url, "tls_failed")
        except (TimeoutError, ConnectionError, OSError):
            return _failure(normalized_url, "request_failed")

        try:
            status = int(getattr(response, "status"))
            response_headers = getattr(response, "headers")
            if status in REDIRECT_STATUSES:
                location = _header(response_headers, "Location")
                if not location:
                    return _failure(normalized_url, "redirect_missing_location")
                if redirects >= MAX_REDIRECTS:
                    return _failure(normalized_url, "too_many_redirects")
                redirects += 1
                current_url = urljoin(normalized_url, location)
                continue
            if status != 200:
                return _failure(normalized_url, f"http_status_{status}")
            if _header(response_headers, "Content-Encoding").lower() not in {
                "",
                "identity",
            }:
                return _failure(normalized_url, "content_encoding_unsupported")
            try:
                declared_length = max(
                    int(_header(response_headers, "Content-Length") or 0),
                    0,
                )
            except ValueError:
                declared_length = 0
            if declared_length > max_bytes:
                return _failure(normalized_url, "body_too_large")

            body = bytearray()
            while True:
                remaining = deadline - clock()
                if remaining <= 0:
                    return _failure(normalized_url, "time_budget_exhausted")
                set_timeout = getattr(response, "set_timeout", None)
                if callable(set_timeout):
                    set_timeout(remaining)
                chunk = response.read(64 * 1024)  # type: ignore[attr-defined]
                if not chunk:
                    break
                body.extend(chunk)
                if len(body) > max_bytes:
                    return _failure(normalized_url, "body_too_large")
            return PublicFetchResult(
                succeeded=True,
                body=bytes(body),
                content_type=_header(response_headers, "Content-Type")
                .partition(";")[0]
                .strip()
                .lower(),
                final_url=normalized_url,
                diagnostic="",
            )
        except (TimeoutError, OSError, http.client.IncompleteRead):
            return _failure(normalized_url, "request_failed")
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()


def _failure(url: str, diagnostic: str) -> PublicFetchResult:
    return PublicFetchResult(False, b"", "", url, diagnostic)
