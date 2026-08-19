from __future__ import annotations

import gzip
import http.client
import json
import socket
import ssl
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from reader_api.article_fetch import (
    MAX_HTML_BYTES,
    ArticleFetchResult,
    FiveFiltersRule,
    FiveFiltersRules,
    PageFetchResult,
    fetch_article,
    fetch_html,
    load_fivefilters_rules,
)


PUBLIC_IP = "93.184.216.34"


class FakeResponse:
    def __init__(
        self,
        body: bytes = b"",
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        chunk_size: int = 64 * 1024,
    ) -> None:
        self.status = status
        self.headers = headers or {"Content-Type": "text/html; charset=utf-8"}
        self._body = body
        self._offset = 0
        self._chunk_size = chunk_size
        self.closed = False

    def read(self, size: int) -> bytes:
        if self._offset >= len(self._body):
            return b""
        end = min(self._offset + min(size, self._chunk_size), len(self._body))
        chunk = self._body[self._offset : end]
        self._offset = end
        return chunk

    def set_timeout(self, _seconds: float) -> None:
        return None

    def close(self) -> None:
        self.closed = True


def public_resolver(_host: str, _port: int, _timeout: float) -> tuple[str, ...]:
    return (PUBLIC_IP,)


def response_request(response: FakeResponse) -> Callable[..., FakeResponse]:
    def request(**_: object) -> FakeResponse:
        return response

    return request


@pytest.mark.parametrize(
    ("url", "addresses"),
    [
        ("ftp://example.com/article", (PUBLIC_IP,)),
        ("https://user:secret@example.com/article", (PUBLIC_IP,)),
        ("https://example.com/article with space", (PUBLIC_IP,)),
        ("https://example.com/" + "a" * 8192, (PUBLIC_IP,)),
        ("https://example.com:0/article", (PUBLIC_IP,)),
        ("https://./article", (PUBLIC_IP,)),
        ("https://127.0.0.1/article", ("127.0.0.1",)),
        ("https://example.com/article", ("10.0.0.8",)),
        ("https://example.com/article", ("fe80::1",)),
        ("https://example.com/article", ("::ffff:127.0.0.1",)),
        ("https://example.com/article", ("224.0.0.1",)),
        ("https://example.com/article", ("ff02::1",)),
        ("https://example.com/article", (PUBLIC_IP, "169.254.169.254")),
    ],
)
def test_fetch_html_rejects_non_public_article_urls(
    url: str,
    addresses: tuple[str, ...],
) -> None:
    called = False

    def request(**_: object) -> FakeResponse:
        nonlocal called
        called = True
        return FakeResponse()

    result = fetch_html(
        url,
        resolver=lambda *_: addresses,
        request=request,
    )

    assert result.succeeded is False
    assert result.diagnostic in {"invalid_url", "ssrf_blocked"}
    assert called is False


def test_fetch_html_accepts_mihomo_fake_ip_allowlist() -> None:
    requested_addresses: list[str] = []

    def request(**kwargs: object) -> FakeResponse:
        requested_addresses.append(str(kwargs["address"]))
        return FakeResponse(b"<html><body>ok</body></html>")

    hostname_result = fetch_html(
        "https://example.com/article",
        resolver=lambda *_: ("198.18.2.216",),
        request=request,
    )
    literal_result = fetch_html(
        "https://198.18.2.216/article",
        request=request,
    )

    assert hostname_result.succeeded is True
    assert literal_result.succeeded is True
    assert requested_addresses == ["198.18.2.216", "198.18.2.216"]


def test_fetch_html_pins_the_checked_address_and_blocks_rebinding() -> None:
    resolutions = iter(((PUBLIC_IP,), ("127.0.0.1",)))
    requested_addresses: list[str] = []

    def request(**kwargs: object) -> FakeResponse:
        requested_addresses.append(str(kwargs["address"]))
        raise TimeoutError

    result = fetch_html(
        "https://example.com/article",
        resolver=lambda *_: next(resolutions),
        request=request,
    )

    assert requested_addresses == [PUBLIC_IP]
    assert result.diagnostic == "ssrf_blocked"


def test_fetch_html_distinguishes_dns_failure_from_unsafe_dns_answer() -> None:
    def resolver(*_: object) -> tuple[str, ...]:
        raise socket.gaierror

    result = fetch_html(
        "https://example.com/article",
        resolver=resolver,
        request=response_request(FakeResponse()),
    )

    assert result.diagnostic == "dns_failed"


def test_fetch_html_revalidates_redirects_and_stops_before_private_target() -> None:
    responses = iter(
        [
            FakeResponse(
                status=302,
                headers={"Location": "http://private.example/article"},
            )
        ]
    )

    def resolver(host: str, _port: int, _timeout: float) -> tuple[str, ...]:
        return (PUBLIC_IP,) if host == "example.com" else ("192.168.1.20",)

    result = fetch_html(
        "https://example.com/article",
        resolver=resolver,
        request=lambda **_: next(responses),
    )

    assert result.succeeded is False
    assert result.redirects == 1
    assert result.diagnostic == "ssrf_blocked"


def test_fetch_html_limits_redirects_to_five() -> None:
    calls = 0

    def request(**_: object) -> FakeResponse:
        nonlocal calls
        calls += 1
        return FakeResponse(
            status=302,
            headers={"Location": f"https://example.com/article/{calls}"},
        )

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=request,
    )

    assert calls == 6
    assert result.redirects == 5
    assert result.diagnostic == "too_many_redirects"


def test_fetch_html_uses_verified_tls_and_accepts_xhtml() -> None:
    contexts: list[ssl.SSLContext] = []

    def request(**kwargs: object) -> FakeResponse:
        contexts.append(kwargs["ssl_context"])  # type: ignore[arg-type]
        return FakeResponse(
            b"<html><body>ok</body></html>",
            headers={
                "Content-Type": "application/xhtml+xml; charset=utf-8",
            },
        )

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=request,
    )

    assert result.succeeded is True
    assert result.html == "<html><body>ok</body></html>"
    assert contexts[0].verify_mode == ssl.CERT_REQUIRED
    assert contexts[0].check_hostname is True


def test_fetch_html_never_retries_with_disabled_certificate_verification() -> None:
    calls = 0

    def request(**_: object) -> FakeResponse:
        nonlocal calls
        calls += 1
        raise ssl.SSLCertVerificationError(1, "certificate verify failed")

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=request,
    )

    assert calls == 1
    assert result.diagnostic == "tls_verification_failed"


def test_fetch_html_rejects_wrong_content_type_and_invalid_charset() -> None:
    wrong_type = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=response_request(
            FakeResponse(b"%PDF", headers={"Content-Type": "application/pdf"})
        ),
    )
    wrong_charset = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=response_request(
            FakeResponse(
                b"\xff",
                headers={"Content-Type": "text/html; charset=utf-8"},
            )
        ),
    )

    assert wrong_type.diagnostic == "content_type_not_html"
    assert wrong_charset.diagnostic == "charset_decode_failed"


@pytest.mark.parametrize(
    ("body", "headers"),
    [
        (
            b"x" * (MAX_HTML_BYTES + 1),
            {"Content-Type": "text/html; charset=utf-8"},
        ),
        (
            gzip.compress(b"x" * (MAX_HTML_BYTES + 1)),
            {
                "Content-Type": "text/html; charset=utf-8",
                "Content-Encoding": "gzip",
            },
        ),
    ],
)
def test_fetch_html_limits_decompressed_html(
    body: bytes,
    headers: dict[str, str],
) -> None:
    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=response_request(FakeResponse(body, headers=headers)),
    )

    assert result.diagnostic == "body_too_large"


def test_fetch_html_rejects_truncated_compressed_body() -> None:
    compressed = gzip.compress(
        ("<html><body>" + ("article " * 100) + "</body></html>").encode()
    )

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=response_request(
            FakeResponse(
                compressed[:-5],
                headers={
                    "Content-Type": "text/html; charset=utf-8",
                    "Content-Encoding": "gzip",
                },
            )
        ),
    )

    assert result.diagnostic == "content_decode_failed"


def test_fetch_html_retries_transient_failures_at_most_three_times() -> None:
    calls = 0

    def request(**_: object) -> FakeResponse:
        nonlocal calls
        calls += 1
        return FakeResponse(status=503)

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=request,
    )

    assert calls == 3
    assert result.attempts == 3
    assert result.diagnostic == "transient_retries_exhausted"


@pytest.mark.parametrize(
    "error",
    [ConnectionResetError(), http.client.IncompleteRead(b"partial")],
)
def test_fetch_html_retries_transient_stream_failures(error: BaseException) -> None:
    calls = 0

    class BrokenResponse(FakeResponse):
        def read(self, _size: int) -> bytes:
            raise error

    def request(**_: object) -> FakeResponse:
        nonlocal calls
        calls += 1
        return BrokenResponse()

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=request,
    )

    assert calls == 3
    assert result.diagnostic == "transient_retries_exhausted"


def test_fetch_html_keeps_all_attempts_inside_one_total_budget() -> None:
    now = [0.0]
    calls = 0

    def request(**_: object) -> FakeResponse:
        nonlocal calls
        calls += 1
        now[0] = 15.1
        raise TimeoutError

    result = fetch_html(
        "https://example.com/article",
        resolver=public_resolver,
        request=request,
        clock=lambda: now[0],
    )

    assert calls == 1
    assert result.diagnostic == "time_budget_exhausted"


def fetched_page(
    html: str, url: str = "https://example.com/article"
) -> PageFetchResult:
    return PageFetchResult(
        succeeded=True,
        html=html,
        final_url=url,
        diagnostic="",
        attempts=1,
        redirects=0,
    )


def test_manual_selector_merges_in_document_order_and_deduplicates_children() -> None:
    text = "正文内容 " * 90
    page = fetched_page(
        "<html><body>"
        f'<article id="parent">{text}<div class="part">子元素内容</div></article>'
        f'<section class="part">{text}第二部分</section>'
        "</body></html>"
    )

    result = fetch_article(
        "https://example.com/article",
        "RSS 摘要",
        article_selector="css:article, .part",
        page_fetcher=lambda _url: page,
        rules=FiveFiltersRules("test", {}, {}),
    )

    assert result.body_source == "webpage"
    assert result.method == "manual"
    assert result.matched_elements == 2
    assert result.content_text.count("子元素内容") == 1
    assert result.content_text.index("子元素内容") < result.content_text.index(
        "第二部分"
    )


def test_manual_selector_zero_match_falls_directly_back_to_rss() -> None:
    page = fetched_page(
        f"<html><body><article>{'网页正文 ' * 100}</article></body></html>"
    )

    result = fetch_article(
        "https://example.com/article",
        "RSS 摘要",
        article_selector="css:.missing",
        page_fetcher=lambda _url: page,
        rules=FiveFiltersRules("test", {}, {}),
    )

    assert result == ArticleFetchResult.rss_failed(
        "RSS 摘要",
        final_url=page.final_url,
        diagnostics=("manual_no_match",),
    )


def test_fivefilters_rule_runs_before_trafilatura_and_strips_noise() -> None:
    page = fetched_page(
        "<html><body>"
        f'<main id="story"><div class="ad">广告</div>{"公共规则正文 " * 80}</main>'
        "</body></html>"
    )
    rules = FiveFiltersRules(
        "fivefilters@test",
        {
            "example.com": FiveFiltersRule(
                body=("//main[@id='story']",),
                strip=("//div[@class='ad']",),
                strip_id_or_class=(),
            )
        },
        {},
    )

    result = fetch_article(
        "https://www.example.com/article",
        "RSS 摘要",
        page_fetcher=lambda _url: page,
        rules=rules,
    )

    assert result.body_source == "webpage"
    assert result.method == "fivefilters"
    assert result.version == "fivefilters@test"
    assert result.removed_elements == 1
    assert "广告" not in result.content_text


def test_fivefilters_rule_matches_redirect_target() -> None:
    page = fetched_page(
        "<html><body>" f"<main>{'目标站公共规则正文 ' * 80}</main>" "</body></html>",
        "https://target.example/article",
    )
    rules = FiveFiltersRules(
        "fivefilters@test",
        {
            "target.example": FiveFiltersRule(
                body=("//main",),
                strip=(),
                strip_id_or_class=(),
            )
        },
        {},
    )

    result = fetch_article(
        "https://short.example/go",
        "RSS 摘要",
        page_fetcher=lambda _url: page,
        rules=rules,
    )

    assert result.method == "fivefilters"
    assert result.final_url == "https://target.example/article"


def test_failed_or_hard_dependency_public_rule_falls_back_to_trafilatura() -> None:
    page = fetched_page("<html><body><main>unimportant</main></body></html>")
    rules = FiveFiltersRules(
        "fivefilters@test",
        {
            "example.com": FiveFiltersRule(
                body=("//article[@id='missing']",),
                strip=(),
                strip_id_or_class=(),
            )
        },
        {"blocked.example.com": "hard_dependency"},
    )
    extracted = "<article>" + ("Trafilatura 正文 " * 80) + "</article>"

    failed_rule = fetch_article(
        "https://example.com/article",
        "RSS 摘要",
        page_fetcher=lambda _url: page,
        rules=rules,
        trafilatura_extractor=lambda *_: extracted,
    )
    skipped_rule = fetch_article(
        "https://blocked.example.com/article",
        "RSS 摘要",
        page_fetcher=lambda _url: fetched_page(
            page.html,
            "https://blocked.example.com/article",
        ),
        rules=rules,
        trafilatura_extractor=lambda *_: extracted,
    )

    assert failed_rule.method == skipped_rule.method == "trafilatura"
    assert failed_rule.diagnostics == ("fivefilters_no_match",)
    assert skipped_rule.diagnostics == ("fivefilters_hard_dependency",)


def test_public_rule_hostname_matching_uses_domain_boundaries() -> None:
    exact = FiveFiltersRule(("//article",), (), ())
    wildcard = FiveFiltersRule(("//main",), (), ())
    rules = FiveFiltersRules(
        "test",
        {
            "example.com": exact,
            ".example.com": wildcard,
        },
        {},
    )

    assert rules.match("example.com") == exact
    assert rules.match("www.example.com") == exact
    assert rules.match("news.example.com") == wildcard
    assert rules.match("evil-example.com") is None


def test_bundled_public_rules_pin_version_and_skip_hard_dependencies() -> None:
    rules = load_fivefilters_rules()

    assert rules.version == ("fivefilters@1ff664684d02df2dc40c2a82389eeabefbc0c2a1")
    assert rules.match("news.about.com") is not None
    assert rules.match("zhihu.com") is None
    assert rules.skipped_reason("zhihu.com") == "hard_dependency"


def test_fivefilters_compiler_keeps_only_safe_rules(tmp_path: Path) -> None:
    source = tmp_path / "site_config"
    source.mkdir()
    (source / "safe.example.txt").write_text(
        "title: //h1\n"
        "body: //article\n"
        "strip: //aside\n"
        "strip_id_or_class: ad\n",
        encoding="utf-8",
    )
    (source / "blocked.example.txt").write_text(
        "body: //article\nhttp_header(user-agent): Example\n",
        encoding="utf-8",
    )
    output = tmp_path / "rules.json"
    script = (
        Path(__file__).resolve().parents[3] / "scripts/compile-fivefilters-rules.py"
    )

    subprocess.run(
        [
            sys.executable,
            str(script),
            str(source),
            str(output),
            "--commit",
            "abc123",
        ],
        check=True,
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["version"] == "fivefilters@abc123"
    assert payload["rules"] == {
        "safe.example": {
            "body": ["//article"],
            "strip": ["//aside"],
            "strip_id_or_class": ["ad"],
        }
    }
    assert payload["skipped"] == {"blocked.example": "hard_dependency"}


@pytest.mark.parametrize(
    "html",
    [
        "<html><head><title>Sign in</title></head><body>"
        + ("Sign in to continue. " * 40)
        + "</body></html>",
        "<html><body><div id='captcha'>"
        + ("Verify you are human. " * 40)
        + "</div></body></html>",
        "<html><head><title>404 Page not found</title></head><body>"
        + ("Page not found. " * 40)
        + "</body></html>",
        "<html><body><main>"
        + ("503 Service Unavailable. Please try again later. " * 80)
        + "</main></body></html>",
        "<html><body><main>"
        + ("Internal Server Error. Please try again later. " * 80)
        + "</main></body></html>",
        "<html><body><main>"
        + ("Bad Gateway. Please try again later. " * 80)
        + "</main></body></html>",
        "<html><body><main>"
        + ("Gateway Timeout. Please try again later. " * 80)
        + "</main></body></html>",
        "<html><body><main>"
        + ("Service Unavailable. Please try again later. " * 80)
        + "</main></body></html>",
        "<html><body><main>" + ("请输入验证码。" * 50) + "</main></body></html>",
    ],
)
def test_http_200_quality_failures_record_rss_failed(html: str) -> None:
    result = fetch_article(
        "https://example.com/article",
        "RSS 摘要",
        page_fetcher=lambda _url: fetched_page(html),
        rules=FiveFiltersRules("test", {}, {}),
        trafilatura_extractor=lambda page, _url: page,
    )

    assert result.body_source == "rss"
    assert result.web_fetch_status == "failed"
    assert result.diagnostics[-1] == "quality_rejected"


def test_webpage_can_be_shorter_than_rss_when_quality_passes() -> None:
    result = fetch_article(
        "https://example.com/article",
        "RSS 正文 " * 300,
        page_fetcher=lambda _url: fetched_page(
            "<article>" + ("网页正文 " * 90) + "</article>"
        ),
        rules=FiveFiltersRules("test", {}, {}),
        trafilatura_extractor=lambda page, _url: page,
    )

    assert len(result.content_text) < len("RSS 正文 " * 300)
    assert result.body_source == "webpage"
    assert result.web_fetch_status == "succeeded"


def test_long_article_that_mentions_captcha_is_not_rejected() -> None:
    article = (
        "<article><h2 id='why-login-was-broken'>登录为何失败</h2>"
        "<div id='signing-off'><p class='paywall'>"
        "本文比较远程工具的验证码处理。</p></div><p>"
        + ("这是正常的产品评测正文，不是验证页面。 " * 100)
        + "</p></article>"
    )
    page = (
        "<html><head><title>Now you can log in with a selfie</title></head>"
        f"<body>{article}</body></html>"
    )

    result = fetch_article(
        "https://example.com/article",
        "RSS 摘要",
        page_fetcher=lambda _url: fetched_page(page),
        rules=FiveFiltersRules("test", {}, {}),
        trafilatura_extractor=lambda *_: article,
    )

    assert result.body_source == "webpage"
    assert result.web_fetch_status == "succeeded"


def test_login_link_outside_extracted_article_does_not_fail_quality() -> None:
    html = (
        "<html><body><nav><a class='login'>Sign in</a></nav>"
        "<article>"
        + ("Real article body with useful reporting. " * 30)
        + "</article></body></html>"
    )
    result = fetch_article(
        "https://example.com/article",
        "RSS 摘要",
        page_fetcher=lambda _url: fetched_page(html),
        rules=FiveFiltersRules("test", {}, {}),
        trafilatura_extractor=lambda *_: (
            "<article>"
            + ("Real article body with useful reporting. " * 30)
            + "</article>"
        ),
    )

    assert result.body_source == "webpage"
