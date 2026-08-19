from __future__ import annotations

import hashlib
import html
import re
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\n]*?\]\([^)]*(?:\)|$)")
MARKDOWN_IMAGE_URL_RE = re.compile(r"!\[[^\n]*?\]\((https?://[^)\s]+)")
RELATED_SECTION_RE = re.compile(r"^\s*(?:相关阅读|相关文章|相关链接|延伸阅读|更多阅读)(?:\s*[:：].*)?\s*$")
DIGEST_TITLE_RE = re.compile(r"早报|晚报|日报|周报|月报|简报|\b(?:digest|newsletter|roundup|daily|weekly)\b", re.I)
DIGEST_SPLIT_THRESHOLD = 0.90
MIXED_THRESHOLD = 0.40
IMAGE_SOURCE_KEYS = (
    "data-original",
    "data-original-src",
    "data-src",
    "data-lazy-src",
    "data-actualsrc",
    "data-image",
    "data-url",
    "srcset",
    "data-srcset",
    "src",
)
PLACEHOLDER_IMAGE_NAMES = {"t.png", "grey.gif", "gray.gif"}
PLACEHOLDER_IMAGE_STEMS = {"1x1", "spacer", "placeholder", "blank", "loading", "transparent"}
PLACEHOLDER_IMAGE_EXTENSIONS = {"gif", "jpg", "jpeg", "png", "webp", "avif"}


class TextExtractor(HTMLParser):
    block_tags = {"address", "article", "blockquote", "br", "div", "figcaption", "figure", "h1", "h2", "h3", "h4", "h5", "h6", "li", "p", "section", "tr", "ul", "ol"}
    heading_tags = {"h1", "h2", "h3", "h4", "h5", "h6"}

    def __init__(self, base_url: str = "") -> None:
        super().__init__()
        self.parts: list[str] = []
        self.base_url = base_url

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self.block_tags:
            self.break_line()
        if tag in self.heading_tags:
            self.parts.append(f"{'#' * int(tag[1])} ")
        if tag == "li":
            self.parts.append("- ")
        if tag == "img":
            values = dict(attrs)
            src = image_src(values)
            if src:
                alt = normalize_space(values.get("alt") or "image")
                self.parts.append(f"\n![{alt}]({urljoin(self.base_url, src)})\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self.block_tags:
            self.break_line()

    def handle_data(self, data: str) -> None:
        text = normalize_space(data)
        if text:
            self.parts.append(text)

    def break_line(self) -> None:
        if self.parts and not self.parts[-1].endswith("\n"):
            self.parts.append("\n")

    def text(self) -> str:
        text = html.unescape(" ".join(self.parts))
        lines = [normalize_space(line) for line in text.splitlines()]
        return re.sub(r"\n{3,}", "\n\n", "\n".join(line for line in lines if line)).strip()


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def clean_title(value: str, fallback: str = "无标题") -> str:
    text = strip_html(value) if "<" in (value or "") and ">" in (value or "") else normalize_space(value)
    text = normalize_space(MARKDOWN_IMAGE_RE.sub(" ", text))
    return text or fallback


def clean_preview(value: str, limit: int | None = None) -> str:
    text = normalize_space(MARKDOWN_IMAGE_RE.sub(" ", value))
    text = re.sub(r"\s+!$", "", text).strip()
    return text[:limit] if limit is not None else text


def strip_html(value: str, base_url: str = "") -> str:
    parser = TextExtractor(base_url)
    parser.feed(value or "")
    return parser.text()


def image_src(values: dict[str, str | None]) -> str:
    fallback = ""
    for key in IMAGE_SOURCE_KEYS:
        for src in image_attr_candidates(key, values.get(key) or ""):
            if not fallback:
                fallback = src
            if not is_placeholder_image_url(src):
                return src
    return fallback


def image_attr_candidates(key: str, value: str) -> list[str]:
    value = normalize_space(value).strip("<>")
    if not value or value.startswith("data:"):
        return []
    if "srcset" not in key:
        return [value]
    return [part.strip().split()[0] for part in value.split(",") if part.strip() and not part.strip().startswith("data:")]


def is_placeholder_image_url(src: str) -> bool:
    path = urlsplit(normalize_space(src).strip("<>")).path.lower()
    name = path.rsplit("/", 1)[-1]
    if name in PLACEHOLDER_IMAGE_NAMES:
        return True
    stem, dot, extension = name.rpartition(".")
    return bool(dot and stem in PLACEHOLDER_IMAGE_STEMS and extension in PLACEHOLDER_IMAGE_EXTENSIONS)


def markdown_image_urls(text: str) -> list[str]:
    return [match.group(1).strip("<>") for match in MARKDOWN_IMAGE_URL_RE.finditer(text or "")]


def first_markdown_image_url(text: str) -> str:
    for url in markdown_image_urls(text):
        if not is_placeholder_image_url(url):
            return url
    return ""


def merge_missing_markdown_images(source_text: str, target_text: str) -> str:
    target_urls = set(markdown_image_urls(target_text))
    missing = []
    for match in MARKDOWN_IMAGE_RE.finditer(source_text or ""):
        url_match = MARKDOWN_IMAGE_URL_RE.search(match.group(0))
        if not url_match:
            continue
        url = url_match.group(1).strip("<>")
        if is_placeholder_image_url(url) or url in target_urls:
            continue
        missing.append(match.group(0))
        target_urls.add(url)
    return "\n\n".join([*missing, target_text]) if missing else target_text


def content_hash(*parts: str) -> str:
    joined = "\n".join(normalize_space(part) for part in parts if part)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def canonical_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlsplit(url.strip())
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in {"fbclid", "gclid"}
    ]
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), urlencode(query), ""))


def normalize_title(title: str) -> str:
    text = normalize_space(title).lower()
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "", text)
    return text[:240]


def lsh_signature(title: str, body: str, bits: int = 1024) -> str:
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "", normalize_space(f"{title} {body}").lower())
    if not text:
        return ""
    width = 5 if len(text) >= 5 else len(text)
    shingles = {text[index : index + width] for index in range(max(len(text) - width + 1, 1))}
    mask = 0
    for shingle in shingles:
        digest = content_hash(shingle)
        for offset in range(0, 16, 4):
            mask |= 1 << (int(digest[offset : offset + 4], 16) % bits)
    return f"b:{mask:0{bits // 4}x}"


def digest_score(title: str, body: str, raw_html: str = "") -> float:
    scoring_body = trim_related_section(body)
    scoring_html = trim_related_html(raw_html)
    score = 0.0
    if has_digest_title_signal(title):
        score += 0.35
    if len(re.findall(r"[|｜;；、/]| - | — |·", title or "")) >= 2:
        score += 0.15
    if len(re.findall(r"(^|\n)\s*(?:[-*•]|\d+[.)、]|[一二三四五六七八九十]+[、.])\s+", scoring_body)) >= 3:
        score += 0.20
    if external_link_count(scoring_body, scoring_html) >= 3:
        score += 0.10
    if rough_entity_count(f"{title}\n{scoring_body}") >= 6:
        score += 0.10
    if paragraph_topic_jumps(scoring_body):
        score += 0.10
    return min(round(score, 2), 1.0)


def external_link_count(body: str, raw_html: str = "") -> int:
    urls = set(re.findall(r"https?://[^\s\"')<]+|www\.[^\s\"')<]+", body or ""))
    urls.update(re.findall(r"""href=["']((?:https?:)?//[^"']+|www\.[^"']+)["']""", raw_html or "", re.I))
    return len(urls)


def has_digest_title_signal(title: str) -> bool:
    return bool(DIGEST_TITLE_RE.search(title or ""))


def document_type(score: float, title: str = "") -> str:
    if score >= DIGEST_SPLIT_THRESHOLD and has_digest_title_signal(title):
        return "digest"
    if score >= MIXED_THRESHOLD:
        return "mixed"
    return "normal_article"


def split_digest_items(title: str, body: str, score: float) -> list[dict[str, str]]:
    structured = (body or "").strip()
    split_body = trim_related_section(body)
    plain = normalize_space(split_body)
    preview = clean_preview(body)
    fallback_title = clean_title(title)
    if score < DIGEST_SPLIT_THRESHOLD or not has_digest_title_signal(title) or not plain:
        return [{"title": fallback_title, "content_text": structured, "summary": preview[:220]}]

    chunks = re.split(r"(?:\n|\r)+\s*(?:[-*•]|\d+[.)、]|[一二三四五六七八九十]+[、.])\s+", split_body)
    chunks = [chunk.strip() for chunk in chunks if len(normalize_space(chunk)) >= 20]
    if len(chunks) < 2:
        return [{"title": fallback_title or "摘要条目", "content_text": structured, "summary": preview[:220]}]

    items: list[dict[str, str]] = []
    for chunk in chunks:
        item_title = clean_title(first_sentence(chunk), fallback_title)[:140]
        items.append({"title": item_title, "content_text": chunk, "summary": clean_preview(chunk, 220)})
    return items


def first_sentence(text: str) -> str:
    text = MARKDOWN_IMAGE_RE.sub(" ", text)
    match = re.search(r"(.+?[。.!?！？])", text)
    return normalize_space(match.group(1) if match else text[:120])


def trim_related_section(body: str) -> str:
    lines = (body or "").splitlines()
    for index, line in enumerate(lines):
        if RELATED_SECTION_RE.match(line):
            return "\n".join(lines[:index]).strip()
    return body or ""


def trim_related_html(raw_html: str) -> str:
    match = re.search(r"(?:相关阅读|相关文章|相关链接|延伸阅读|更多阅读)\s*[:：]?", html.unescape(raw_html or ""))
    return (raw_html or "")[: match.start()] if match else raw_html or ""


def rough_entity_count(text: str) -> int:
    latin = re.findall(r"\b[A-Z][A-Za-z0-9&.-]{2,}\b", text or "")
    cjk = re.findall(r"[\u4e00-\u9fff]{2,}(?:公司|集团|大学|政府|部门|平台|模型|芯片|基金|法院|银行)", text or "")
    return len(set(latin + cjk))


def paragraph_topic_jumps(body: str) -> bool:
    paragraphs = [normalize_space(p) for p in re.split(r"\n{2,}", body or "") if len(normalize_space(p)) > 20]
    if len(paragraphs) < 4:
        return False
    starts = {p[:2] for p in paragraphs[:6]}
    return len(starts) >= 4
