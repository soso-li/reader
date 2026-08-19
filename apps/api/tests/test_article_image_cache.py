import fcntl
import os
import ssl
import threading
import time

from lxml.html import fragment_fromstring

from reader_api.article_image_cache import (
    ArticleImageCache,
    DownloadBudget,
    DownloadedImage,
    MAX_IMAGE_BYTES,
    cache_key_for_url,
    download_image,
    prepare_reading_images,
)


def test_cache_file_locks_never_block_filesystem_workers(
    monkeypatch,
    tmp_path,
) -> None:
    operations: list[int] = []
    blocked_once = True

    def fake_flock(_file_descriptor: int, operation: int) -> None:
        nonlocal blocked_once
        operations.append(operation)
        if operation & fcntl.LOCK_EX and blocked_once:
            blocked_once = False
            raise BlockingIOError

    monkeypatch.setattr(
        "reader_api.article_image_cache.fcntl.flock",
        fake_flock,
    )
    cache = ArticleImageCache(tmp_path, max_bytes=1024)
    url = "https://cdn.example.com/lock.png"

    cache.register(url)
    with cache.proxy_lock(cache_key_for_url(url)):
        pass

    acquisitions = [operation for operation in operations if operation & fcntl.LOCK_EX]
    assert acquisitions
    assert all(operation & fcntl.LOCK_NB for operation in acquisitions)


def test_prepare_reading_images_keeps_original_bytes_and_isolates_failures(
    tmp_path,
) -> None:
    cache = ArticleImageCache(tmp_path, max_bytes=1024)
    first_url = "https://cdn.example.com/first.png"
    second_url = "https://cdn.example.com/second.png"
    source_html = (
        f'<figure><img src="/images/rss" data-reader-original-src="{first_url}">'
        "<figcaption>First</figcaption></figure>"
        f'<img src="/images/rss" data-reader-original-src="{second_url}">'
    )
    calls: list[str] = []
    original = b"\x89PNG\r\n\x1a\noriginal-image-bytes"

    def download(url: str, **_kwargs: object) -> DownloadedImage | None:
        calls.append(url)
        return (
            DownloadedImage(original, "image/png")
            if url == first_url
            else None
        )

    reading_html = prepare_reading_images(
        source_html,
        cache,
        downloader=download,
    )
    document = fragment_fromstring(reading_html, create_parent="div")
    images = document.xpath(".//img")

    assert calls == [first_url, second_url]
    assert cache.read(cache_key_for_url(first_url)).body == original
    assert cache.read(cache_key_for_url(second_url)) is None
    assert images[0].get("src").startswith(
        f"/api/images/article/{cache_key_for_url(first_url)}?"
    )
    assert images[1].get("src").startswith(
        f"/api/images/article/{cache_key_for_url(second_url)}?"
    )
    assert images[0].get("data-reader-original-src") == first_url
    assert images[1].get("data-reader-original-src") == second_url
    assert document.xpath("string(.//figcaption)") == "First"


def test_prepare_reading_images_keeps_article_proxy_when_cache_is_unavailable() -> None:
    url = "https://cdn.example.com/original.png"
    reading_html = prepare_reading_images(
        f'<img src="/images/rss" data-reader-original-src="{url}">',
        None,
        downloader=lambda *_args, **_kwargs: (
            (_ for _ in ()).throw(AssertionError("must not download"))
        ),
    )

    assert (
        f"/api/images/article/{cache_key_for_url(url)}?"
        in reading_html
    )


def test_protected_images_send_referer_without_changing_other_hosts() -> None:
    class Response:
        status = 200
        headers = {"Content-Type": "image/jpeg"}

        def __init__(self) -> None:
            self.chunks = [b"\xff\xd8\xfforiginal", b""]

        def read(self, _size: int) -> bytes:
            return self.chunks.pop(0)

        def close(self) -> None:
            return None

    request_headers: list[dict[str, str]] = []
    for url in (
        "https://cdnfile.sspai.com/2026/08/06/article/example.jpeg",
        "https://static.chongdiantou.com/uploads/2026/08/11/example.jpeg",
        "https://cdn.example.com/example.jpeg",
    ):
        assert download_image(
            url,
            deadline=60.0,
            budget=DownloadBudget(1024),
            resolver=lambda *_args: ("93.184.216.34",),
            request=lambda **kwargs: (
                request_headers.append(kwargs["headers"]) or Response()
            ),
            clock=lambda: 0.0,
            sleep=lambda _seconds: None,
        ) == DownloadedImage(b"\xff\xd8\xfforiginal", "image/jpeg")

    assert request_headers[0]["Referer"] == "https://cdnfile.sspai.com/"
    assert request_headers[1]["Referer"] == "https://static.chongdiantou.com/"
    assert "Referer" not in request_headers[2]


def test_chongdiantou_legacy_proxy_failure_gets_one_new_policy_attempt(tmp_path) -> None:
    cache = ArticleImageCache(tmp_path, max_bytes=1024)
    url = "https://static.chongdiantou.com/uploads/2026/08/11/example.jpeg"
    key = cache_key_for_url(url)
    (tmp_path / f"{key}.json").write_text(
        f'{{"url":"{url}","content_type":"","proxy_attempted":true}}',
        encoding="utf-8",
    )

    assert cache.proxy_attempted(key) is False
    cache.mark_proxy_attempted(url)
    assert cache.proxy_attempted(key) is True


def test_cache_evicts_least_recent_body_but_keeps_original_url_metadata(
    tmp_path,
) -> None:
    cache = ArticleImageCache(tmp_path, max_bytes=6)
    first_url = "https://cdn.example.com/first.jpg"
    second_url = "https://cdn.example.com/second.jpg"
    third_url = "https://cdn.example.com/third.jpg"
    first_key = cache.store(first_url, DownloadedImage(b"111", "image/jpeg"))
    second_key = cache.store(second_url, DownloadedImage(b"222", "image/jpeg"))
    first_path = tmp_path / f"{first_key}.body"
    second_path = tmp_path / f"{second_key}.body"
    os.utime(first_path, ns=(3, 3))
    os.utime(second_path, ns=(2, 2))

    cache.store(third_url, DownloadedImage(b"333", "image/jpeg"))
    cache.evict()

    reopened = ArticleImageCache(tmp_path, max_bytes=6)
    assert reopened.read(first_key).body == b"111"
    assert reopened.read(second_key) is None
    assert reopened.url_for_key(second_key) == second_url
    assert reopened.usage() == 6


def test_cache_store_defers_the_full_eviction_scan(monkeypatch, tmp_path) -> None:
    cache = ArticleImageCache(tmp_path, max_bytes=1024)
    scans: list[bool] = []
    monkeypatch.setattr(cache, "_evict", lambda: scans.append(True))

    cache.store(
        "https://cdn.example.com/deferred.jpg",
        DownloadedImage(b"image", "image/jpeg"),
    )

    assert scans == []
    cache.evict()
    assert scans == [True]


def test_download_image_retries_transient_failure_with_public_address_recheck() -> None:
    class Response:
        def __init__(
            self,
            *,
            status: int,
            body: bytes = b"",
            headers: dict[str, str] | None = None,
        ) -> None:
            self.status = status
            self.headers = headers or {}
            self.body = body
            self.offset = 0

        def read(self, size: int) -> bytes:
            chunk = self.body[self.offset : self.offset + size]
            self.offset += len(chunk)
            return chunk

        def set_timeout(self, _seconds: float) -> None:
            return None

        def close(self) -> None:
            return None

    responses = iter(
        (
            Response(status=503),
            Response(
                status=200,
                body=b"\x89PNG\r\n\x1a\nunchanged",
                headers={"Content-Type": "image/png"},
            ),
        )
    )
    resolved: list[str] = []
    requested_addresses: list[str] = []
    sleeps: list[float] = []

    def resolver(host: str, _port: int, _timeout: float) -> tuple[str, ...]:
        resolved.append(host)
        return ("93.184.216.34",)

    def request(**kwargs: object) -> Response:
        requested_addresses.append(str(kwargs["address"]))
        context = kwargs["ssl_context"]
        assert isinstance(context, ssl.SSLContext)
        assert context.verify_mode == ssl.CERT_REQUIRED
        return next(responses)

    image = download_image(
        "https://cdn.example.com/image.png",
        deadline=60.0,
        budget=DownloadBudget(1024),
        resolver=resolver,
        request=request,
        clock=lambda: 0.0,
        sleep=sleeps.append,
    )

    assert image == DownloadedImage(
        b"\x89PNG\r\n\x1a\nunchanged",
        "image/png",
    )
    assert resolved == ["cdn.example.com", "cdn.example.com"]
    assert requested_addresses == ["93.184.216.34", "93.184.216.34"]
    assert sleeps == [0.5]


def test_download_image_retries_connection_reset_while_reading() -> None:
    class Response:
        status = 200
        headers = {"Content-Type": "image/png"}

        def __init__(self, *, fails: bool) -> None:
            self.fails = fails
            self.read_count = 0

        def read(self, _size: int) -> bytes:
            self.read_count += 1
            if self.fails:
                raise ConnectionResetError("reset")
            return (
                b"\x89PNG\r\n\x1a\nunchanged"
                if self.read_count == 1
                else b""
            )

        def set_timeout(self, _seconds: float) -> None:
            return None

        def close(self) -> None:
            return None

    responses = iter((Response(fails=True), Response(fails=False)))
    resolves: list[str] = []
    sleeps: list[float] = []

    image = download_image(
        "https://cdn.example.com/image.png",
        deadline=60.0,
        budget=DownloadBudget(1024),
        resolver=lambda host, _port, _timeout: (
            resolves.append(host) or ("93.184.216.34",)
        ),
        request=lambda **_kwargs: next(responses),
        clock=lambda: 0.0,
        sleep=sleeps.append,
    )

    assert image == DownloadedImage(
        b"\x89PNG\r\n\x1a\nunchanged",
        "image/png",
    )
    assert resolves == ["cdn.example.com", "cdn.example.com"]
    assert sleeps == [0.5]


def test_prepare_reading_images_limits_downloads_and_workers(tmp_path) -> None:
    cache = ArticleImageCache(tmp_path, max_bytes=1024)
    source_html = "".join(
        (
            f'<img src="/images/rss" '
            f'data-reader-original-src="https://cdn.example.com/{index}.png">'
        )
        for index in range(55)
    )
    lock = threading.Lock()
    calls = 0
    active = 0
    maximum_active = 0

    def download(
        _url: str,
        **_kwargs: object,
    ) -> DownloadedImage:
        nonlocal calls, active, maximum_active
        with lock:
            calls += 1
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0.005)
        with lock:
            active -= 1
        return DownloadedImage(b"\x89PNG\r\n\x1a\n", "image/png")

    reading_html = prepare_reading_images(
        source_html,
        cache,
        downloader=download,
    )

    assert calls == 50
    assert maximum_active <= 4
    assert reading_html.count("/api/images/article/") == 55


def test_download_image_enforces_per_image_and_shared_article_budgets() -> None:
    class Response:
        status = 200

        def __init__(self, body: bytes, declared_length: int) -> None:
            self.body = body
            self.headers = {
                "Content-Type": "image/png",
                "Content-Length": str(declared_length),
            }
            self.offset = 0

        def read(self, size: int) -> bytes:
            chunk = self.body[self.offset : self.offset + size]
            self.offset += len(chunk)
            return chunk

        def set_timeout(self, _seconds: float) -> None:
            return None

        def close(self) -> None:
            return None

    original = b"\x89PNG\r\n\x1a\noriginal"
    budget = DownloadBudget(len(original))
    responses = iter(
        (
            Response(original, len(original)),
            Response(original, len(original)),
            Response(b"", MAX_IMAGE_BYTES + 1),
        )
    )

    def fetch() -> DownloadedImage | None:
        return download_image(
            "https://cdn.example.com/image.png",
            deadline=60.0,
            budget=budget,
            resolver=lambda *_args: ("93.184.216.34",),
            request=lambda **_kwargs: next(responses),
            clock=lambda: 0.0,
            sleep=lambda _seconds: None,
        )

    assert fetch() == DownloadedImage(original, "image/png")
    assert fetch() is None
    assert fetch() is None


def test_download_image_rechecks_private_redirect_target() -> None:
    class Redirect:
        status = 302
        headers = {"Location": "http://127.0.0.1/private.png"}

        def close(self) -> None:
            return None

    requests = 0

    def request(**_kwargs: object) -> Redirect:
        nonlocal requests
        requests += 1
        return Redirect()

    image = download_image(
        "https://cdn.example.com/image.png",
        deadline=60.0,
        budget=DownloadBudget(1024),
        resolver=lambda *_args: ("93.184.216.34",),
        request=request,
        clock=lambda: 0.0,
        sleep=lambda _seconds: None,
    )

    assert image is None
    assert requests == 1
