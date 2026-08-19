from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import ssl
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from urllib.parse import urlencode, urljoin, urlsplit

from lxml import html

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


MAX_IMAGES_PER_ARTICLE = 50
MAX_IMAGE_BYTES = 12 * 1024 * 1024
MAX_ARTICLE_IMAGE_BYTES = 256 * 1024 * 1024
MAX_IMAGE_WORKERS = 4
ARTICLE_IMAGE_SECONDS = 60.0
ON_DEMAND_IMAGE_SECONDS = 3.0
CACHE_LOCK_SECONDS = 3.0
CACHE_LOCK_RETRY_SECONDS = 0.01
TRANSIENT_IMAGE_STATUSES = frozenset({429, 502, 503, 504})
DEFAULT_CACHE_BYTES = 50 * 1024 * 1024 * 1024
DEFAULT_CACHE_DIRECTORY = "/tmp/reader-article-images"
REFERER_ATTEMPT = "source-referer-v2"


@dataclass(frozen=True, slots=True)
class DownloadedImage:
    body: bytes
    content_type: str


@dataclass(frozen=True, slots=True)
class CachedImage:
    body: bytes
    content_type: str
    url: str


class DownloadBudget:
    def __init__(self, maximum: int) -> None:
        self.maximum = maximum
        self.used = 0
        self._lock = threading.Lock()

    def consume(self, size: int) -> bool:
        with self._lock:
            if self.used + size > self.maximum:
                return False
            self.used += size
            return True

    def remaining(self) -> int:
        with self._lock:
            return self.maximum - self.used


class ArticleImageCache:
    def __init__(self, directory: str | Path, *, max_bytes: int) -> None:
        self.directory = Path(directory)
        self.max_bytes = max(max_bytes, 0)
        self.directory.mkdir(parents=True, exist_ok=True)

    def register(self, url: str) -> str:
        key = cache_key_for_url(url)
        with self._locked():
            metadata = self._metadata(key)
            if metadata.get("url") != url:
                self._write_metadata(key, {"url": url, "content_type": ""})
        return key

    def store(self, url: str, image: DownloadedImage) -> str:
        key = cache_key_for_url(url)
        if len(image.body) > self.max_bytes:
            self.register(url)
            return key
        with self._locked():
            self._write_file(self._body_path(key), image.body)
            self._write_metadata(
                key,
                {"url": url, "content_type": image.content_type},
            )
        return key

    def evict(self) -> None:
        try:
            with _exclusive_file_lock(
                self.directory / ".evict.lock",
                timeout_seconds=0,
            ):
                self._evict()
        except OSError:
            pass

    def read(self, key: str) -> CachedImage | None:
        if not valid_cache_key(key):
            return None
        with self._locked():
            metadata = self._metadata(key)
            body_path = self._body_path(key)
            if not metadata.get("url") or not body_path.is_file():
                return None
            try:
                body = body_path.read_bytes()
                os.utime(body_path, None)
            except OSError:
                return None
            return CachedImage(
                body=body,
                content_type=str(metadata.get("content_type") or ""),
                url=str(metadata["url"]),
            )

    def url_for_key(self, key: str) -> str:
        if not valid_cache_key(key):
            return ""
        with self._locked():
            return str(self._metadata(key).get("url") or "")

    def proxy_attempted(self, key: str) -> bool:
        if not valid_cache_key(key):
            return False
        with self._locked():
            metadata = self._metadata(key)
            attempted = metadata.get("proxy_attempted")
            if attempted is True and _image_referer(str(metadata.get("url") or "")):
                return False
            return bool(attempted)

    def mark_proxy_attempted(self, url: str) -> None:
        key = cache_key_for_url(url)
        with self._locked():
            metadata = self._metadata(key)
            if metadata.get("url") not in (None, url):
                return
            self._write_metadata(
                key,
                {
                    "url": url,
                    "content_type": "",
                    "proxy_attempted": (
                        REFERER_ATTEMPT if _image_referer(url) else True
                    ),
                },
            )

    @contextmanager
    def proxy_lock(self, key: str) -> Iterator[None]:
        if not valid_cache_key(key):
            raise ValueError("invalid cache key")
        lock_path = self.directory / f".{key}.proxy.lock"
        with _exclusive_file_lock(lock_path):
            yield

    def usage(self) -> int:
        # ponytail: filesystem scan is enough for a personal cache;
        # add an index if file count becomes a bottleneck.
        with self._locked():
            return sum(
                path.stat().st_size
                for path in self.directory.glob("*.body")
                if path.is_file()
            )

    @contextmanager
    def _locked(self) -> Iterator[None]:
        lock_path = self.directory / ".lock"
        with _exclusive_file_lock(lock_path):
            yield

    def _body_path(self, key: str) -> Path:
        return self.directory / f"{key}.body"

    def _metadata_path(self, key: str) -> Path:
        return self.directory / f"{key}.json"

    def _metadata(self, key: str) -> dict[str, object]:
        try:
            value = json.loads(self._metadata_path(key).read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _write_metadata(self, key: str, value: dict[str, object]) -> None:
        self._write_file(
            self._metadata_path(key),
            json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(),
        )

    def _write_file(self, destination: Path, body: bytes) -> None:
        temporary_name = ""
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.directory,
                delete=False,
            ) as temporary:
                temporary.write(body)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_name = temporary.name
            os.replace(temporary_name, destination)
        finally:
            if temporary_name:
                Path(temporary_name).unlink(missing_ok=True)

    def _evict(self) -> None:
        bodies = sorted(
            (
                (path.stat().st_mtime_ns, path.stat().st_size, path)
                for path in self.directory.glob("*.body")
                if path.is_file()
            ),
            key=lambda value: value[0],
        )
        total = sum(size for _mtime, size, _path in bodies)
        for _mtime, size, path in bodies:
            if total <= self.max_bytes:
                break
            path.unlink(missing_ok=True)
            total -= size


@contextmanager
def _exclusive_file_lock(
    path: Path,
    *,
    timeout_seconds: float = CACHE_LOCK_SECONDS,
) -> Iterator[None]:
    deadline = time.monotonic() + timeout_seconds
    with path.open("a", encoding="utf-8") as lock:
        while True:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise TimeoutError("文章图片缓存锁等待超时") from exc
                time.sleep(CACHE_LOCK_RETRY_SECONDS)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def cache_key_for_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def valid_cache_key(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _image_referer(url: str) -> str:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return ""
    return (
        f"{parsed.scheme}://{parsed.netloc}/"
        if parsed.hostname in {
            "cdnfile.sspai.com",
            "static.chongdiantou.com",
        }
        else ""
    )


def configured_article_image_cache() -> ArticleImageCache:
    directory = os.getenv(
        "READER_ARTICLE_IMAGE_CACHE_DIR",
        DEFAULT_CACHE_DIRECTORY,
    )
    try:
        max_bytes = int(
            os.getenv(
                "READER_ARTICLE_IMAGE_CACHE_MAX_BYTES",
                str(DEFAULT_CACHE_BYTES),
            )
        )
    except ValueError:
        max_bytes = DEFAULT_CACHE_BYTES
    return ArticleImageCache(directory, max_bytes=max_bytes)


def download_image(
    url: str,
    *,
    deadline: float,
    budget: DownloadBudget,
    resolver: Callable[[str, int, float], tuple[str, ...]] = resolve_addresses,
    request: Callable[..., object] = _request_once,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> DownloadedImage | None:
    current_url = url
    redirects = 0
    transient_attempts = 0
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            return None
        try:
            parsed, hostname, port, normalized_url = _article_url(current_url)
            addresses = (
                (hostname,)
                if _is_ip_literal(hostname)
                else resolver(hostname, port, remaining)
            )
            public_addresses = _validated_public_addresses(addresses)
        except (ValueError, UnicodeError, ssl.SSLError):
            return None
        except (TimeoutError, OSError):
            transient_attempts += 1
            if not _retry_image(
                transient_attempts,
                deadline,
                clock,
                sleep,
            ):
                return None
            continue

        remaining = deadline - clock()
        if remaining <= 0:
            return None
        ssl_context = (
            ssl.create_default_context()
            if parsed.scheme == "https"
            else None
        )
        try:
            headers = {
                "User-Agent": "Reader/0.1 (+personal RSS reader)",
                "Accept": "image/avif,image/webp,image/png,image/jpeg,image/gif,image/*",
                "Accept-Encoding": "identity",
                "Connection": "close",
            }
            referer = _image_referer(normalized_url)
            if referer:
                headers["Referer"] = referer
            response = request(
                url=normalized_url,
                address=public_addresses[0],
                timeout=remaining,
                ssl_context=ssl_context,
                headers=headers,
            )
        except (ssl.SSLCertVerificationError, ssl.SSLError):
            return None
        except (TimeoutError, ConnectionError, OSError):
            transient_attempts += 1
            if not _retry_image(
                transient_attempts,
                deadline,
                clock,
                sleep,
            ):
                return None
            continue

        try:
            status = int(getattr(response, "status"))
            headers = getattr(response, "headers")
            if status in REDIRECT_STATUSES:
                location = _header(headers, "Location")
                if not location or redirects >= MAX_REDIRECTS:
                    return None
                redirects += 1
                current_url = urljoin(normalized_url, location)
                continue
            if status in TRANSIENT_IMAGE_STATUSES:
                transient_attempts += 1
                if not _retry_image(
                    transient_attempts,
                    deadline,
                    clock,
                    sleep,
                ):
                    return None
                continue
            if status != 200:
                return None
            if _header(headers, "Content-Encoding").lower() not in {
                "",
                "identity",
            }:
                return None
            declared_length = _content_length(headers)
            if (
                declared_length > MAX_IMAGE_BYTES
                or declared_length > budget.remaining()
            ):
                return None
            body = _read_image_body(
                response,
                deadline=deadline,
                budget=budget,
                clock=clock,
            )
            if body is None:
                return None
            content_type = _image_content_type(
                _header(headers, "Content-Type"),
                body,
            )
            return (
                DownloadedImage(body, content_type)
                if content_type
                else None
            )
        except (TimeoutError, OSError, http.client.IncompleteRead):
            transient_attempts += 1
            if not _retry_image(
                transient_attempts,
                deadline,
                clock,
                sleep,
            ):
                return None
            continue
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()


def _retry_image(
    transient_attempts: int,
    deadline: float,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> bool:
    if transient_attempts >= 3:
        return False
    delay = 0.5 if transient_attempts == 1 else 1.5
    if clock() + delay >= deadline:
        return False
    sleep(delay)
    return True


def _content_length(headers: object) -> int:
    value = _header(headers, "Content-Length")
    try:
        return max(int(value), 0)
    except ValueError:
        return 0


def _read_image_body(
    response: object,
    *,
    deadline: float,
    budget: DownloadBudget,
    clock: Callable[[], float],
) -> bytes | None:
    body = bytearray()
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            return None
        set_timeout = getattr(response, "set_timeout", None)
        if callable(set_timeout):
            set_timeout(remaining)
        chunk = response.read(64 * 1024)  # type: ignore[attr-defined]
        if not chunk:
            return bytes(body)
        if len(body) + len(chunk) > MAX_IMAGE_BYTES:
            return None
        if not budget.consume(len(chunk)):
            return None
        body.extend(chunk)


def _image_content_type(value: str, body: bytes) -> str:
    content_type = value.partition(";")[0].strip().lower()
    if content_type.startswith("image/") and content_type != "image/svg+xml":
        return content_type
    if content_type not in {"", "application/octet-stream"}:
        return ""
    if body.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if body.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if body.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if body.startswith(b"RIFF") and body[8:12] == b"WEBP":
        return "image/webp"
    if len(body) >= 12 and body[4:8] == b"ftyp" and b"avif" in body[8:32]:
        return "image/avif"
    return ""


def prepare_reading_images(
    reading_html: str,
    cache: ArticleImageCache | None,
    *,
    additional_url: str = "",
    downloader: Callable[..., DownloadedImage | None] = download_image,
    prefetch: bool = True,
    clock: Callable[[], float] = time.monotonic,
) -> str:
    root = html.fragment_fromstring(
        reading_html or "",
        create_parent="div",
    )
    urls: list[str] = []
    active_cache = cache
    for image in root.iter("img"):
        url = image.get("data-reader-original-src", "")
        if not url:
            continue
        key = cache_key_for_url(url)
        image.set(
            "src",
            f"/api/images/article/{key}?{urlencode({'src': url})}",
        )
        if active_cache is not None:
            try:
                active_cache.register(url)
            except OSError:
                active_cache = None
        if url not in urls:
            urls.append(url)

    if additional_url and additional_url not in urls:
        if active_cache is not None:
            try:
                active_cache.register(additional_url)
            except OSError:
                active_cache = None
        urls.append(additional_url)

    if prefetch and active_cache is not None:
        try:
            missing = [
                url
                for url in urls[:MAX_IMAGES_PER_ARTICLE]
                if active_cache.read(cache_key_for_url(url)) is None
            ]
        except OSError:
            missing = []
        deadline = clock() + ARTICLE_IMAGE_SECONDS
        budget = DownloadBudget(MAX_ARTICLE_IMAGE_BYTES)
        with ThreadPoolExecutor(max_workers=MAX_IMAGE_WORKERS) as executor:
            results = executor.map(
                lambda url: _download_or_none(
                    downloader,
                    url,
                    deadline=deadline,
                    budget=budget,
                ),
                missing,
            )
            for url, result in zip(missing, results, strict=True):
                if result is not None:
                    try:
                        active_cache.store(url, result)
                    except OSError:
                        pass
        active_cache.evict()

    return "".join(
        html.tostring(
            child,
            encoding="unicode",
            method="html",
            with_tail=False,
        )
        for child in root
    )


def _download_or_none(
    downloader: Callable[..., DownloadedImage | None],
    url: str,
    *,
    deadline: float,
    budget: DownloadBudget,
) -> DownloadedImage | None:
    try:
        return downloader(url, deadline=deadline, budget=budget)
    except Exception:
        return None
