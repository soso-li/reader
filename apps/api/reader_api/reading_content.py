from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from urllib.parse import urlencode, urljoin, urlsplit

from lxml import etree, html

from .digest import image_src, is_placeholder_image_url


_CONTAINER_TAGS = {"article", "div", "header", "main", "section"}
_INLINE_RUN_CONTAINER_TAGS = {*_CONTAINER_TAGS, "blockquote"}
_BLOCK_TAGS = {
    *_CONTAINER_TAGS,
    "blockquote",
    "figcaption",
    "figure",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "li",
    "ol",
    "p",
    "pre",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
}
_ALLOWED_TAGS = {
    *_BLOCK_TAGS,
    "a",
    "br",
    "code",
    "del",
    "em",
    "img",
    "mark",
    "s",
    "span",
    "strong",
    "sub",
    "sup",
    "u",
}
_DROP_TREE_TAGS = {
    "aside",
    "base",
    "button",
    "canvas",
    "embed",
    "fieldset",
    "footer",
    "form",
    "iframe",
    "input",
    "label",
    "legend",
    "link",
    "math",
    "meta",
    "nav",
    "noscript",
    "object",
    "option",
    "script",
    "select",
    "style",
    "svg",
    "template",
    "textarea",
}
_BLOCK_ID_TAGS = {
    "figcaption",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "p",
    "pre",
    "td",
    "th",
}
_INDENT_PREFIX_RE = re.compile(r"^[\t\r\n ]*(?:[\u2002\u2003\u3000]+|\u00a0{2,})")
_UNSAFE_CSS_RE = re.compile(
    r"(?:url\s*\(|expression\s*\(|javascript\s*:|data\s*:|"
    r"@import|var\s*\(|-moz-binding|[\\{}])",
    re.IGNORECASE,
)
_COLOR_RE = re.compile(
    r"(?:#[0-9a-f]{3,8}|[a-z]{1,30}|" r"(?:rgb|rgba|hsl|hsla)\([0-9.,%+\-/ ]+\))",
    re.IGNORECASE,
)
_RELATIVE_LENGTH_RE = re.compile(r"(?P<number>\d+(?:\.\d+)?|\.\d+)(?P<unit>em|rem|%)")
_TRANSLATABLE_SPACE_RE = re.compile(r"\s+")
_TEXT_ALIGN_VALUES = {"center", "end", "justify", "left", "right", "start"}


@dataclass(frozen=True, slots=True)
class NormalizedReadingContent:
    reading_html: str
    content_text: str


def normalize_reading_content(
    source_html: str,
    final_url: str,
) -> NormalizedReadingContent:
    """Create safe HTML and text projections from one normalized lxml tree."""
    parser = html.HTMLParser(
        no_network=True,
        recover=True,
        remove_comments=True,
        remove_pis=True,
    )
    root = html.fragment_fromstring(
        source_html or "",
        create_parent="div",
        parser=parser,
    )
    _sanitize_tree(root, final_url)
    _wrap_container_inline_content(root)
    _canonicalize_preformatted_blocks(root)
    _normalize_text_nodes(root)
    _assign_block_ids(root)

    return NormalizedReadingContent(
        reading_html="".join(
            html.tostring(child, encoding="unicode", method="html", with_tail=False)
            for child in root
        ),
        content_text=_render_container(root),
    )


def _sanitize_tree(root: html.HtmlElement, final_url: str) -> None:
    for element in reversed(list(root.iterdescendants())):
        if not isinstance(element.tag, str):
            element.drop_tree()
            continue
        tag = _local_tag(element)
        if tag in _DROP_TREE_TAGS or _is_hidden(element):
            element.drop_tree()
            continue

        legacy_style = ""
        if tag == "b":
            tag = "strong"
        elif tag == "i":
            tag = "em"
        elif tag == "strike":
            tag = "s"
        elif tag == "center":
            tag = "div"
            legacy_style = "text-align:center"
        elif tag == "font":
            tag = "span"
            legacy_style = _legacy_font_style(element)
        element.tag = tag

        if tag not in _ALLOWED_TAGS:
            element.drop_tag()
            continue

        original_attributes = dict(element.attrib)
        element.attrib.clear()
        style = _safe_style(
            ";".join(
                part
                for part in (
                    legacy_style,
                    original_attributes.get("style", ""),
                    _alignment_style(original_attributes.get("align", "")),
                )
                if part
            )
        )

        if tag == "p" and (
            _has_positive_text_indent(original_attributes.get("style", ""))
            or _strip_explicit_indent_prefix(element)
        ):
            element.set("class", "reader-first-line-indent")
        if style:
            element.set("style", style)

        if tag == "a":
            href = safe_absolute_url(original_attributes.get("href", ""), final_url)
            if not href:
                element.drop_tag()
                continue
            element.set("href", href)
            element.set("target", "_blank")
            element.set("rel", "noopener noreferrer")
        elif tag == "img":
            source = safe_absolute_url(
                image_src(original_attributes),
                final_url,
            )
            if not source:
                alt = _single_line(original_attributes.get("alt", ""))
                if not alt:
                    element.drop_tree()
                    continue
                element.tag = "span"
                element.attrib.clear()
                element.text = alt
                continue
            if _is_placeholder_image(source, original_attributes):
                element.drop_tree()
                continue
            element.set("src", _image_proxy_url(source))
            element.set("data-reader-original-src", source)
            element.set("alt", _single_line(original_attributes.get("alt", "")))
            element.set("loading", "lazy")
            element.set("decoding", "async")
        elif tag in {"td", "th"}:
            _copy_bounded_integer_attribute(
                element, original_attributes, "colspan", minimum=1, maximum=100
            )
            _copy_bounded_integer_attribute(
                element, original_attributes, "rowspan", minimum=1, maximum=100
            )
        elif tag == "ol":
            _copy_bounded_integer_attribute(
                element,
                original_attributes,
                "start",
                minimum=-1_000_000,
                maximum=1_000_000,
            )
        elif tag == "li":
            _copy_bounded_integer_attribute(
                element,
                original_attributes,
                "value",
                minimum=-1_000_000,
                maximum=1_000_000,
            )


def _local_tag(element: etree._Element) -> str:
    return etree.QName(element).localname.lower()


def _is_hidden(element: etree._Element) -> bool:
    role = (element.get("role") or "").strip().lower()
    return (
        element.get("hidden") is not None
        or (element.get("aria-hidden") or "").strip().lower() == "true"
        or role in {"banner", "complementary", "contentinfo", "form", "navigation"}
    )


def _legacy_font_style(element: etree._Element) -> str:
    declarations: list[str] = []
    if face := element.get("face"):
        declarations.append(f"font-family:{face}")
    if color := element.get("color"):
        declarations.append(f"color:{color}")
    if size := element.get("size"):
        relative_sizes = {
            "1": ".75em",
            "2": ".875em",
            "3": "1em",
            "4": "1.125em",
            "5": "1.25em",
            "6": "1.5em",
            "7": "2em",
        }
        if relative_size := relative_sizes.get(size.strip()):
            declarations.append(f"font-size:{relative_size}")
    return ";".join(declarations)


def _alignment_style(value: str) -> str:
    value = value.strip().lower()
    return f"text-align:{value}" if value in _TEXT_ALIGN_VALUES else ""


def _safe_style(style: str) -> str:
    declarations: list[str] = []
    seen: set[str] = set()
    for declaration in style.split(";"):
        name, separator, value = declaration.partition(":")
        name = name.strip().lower()
        value = _single_line(value)
        if not separator or not name or name in seen or _UNSAFE_CSS_RE.search(value):
            continue
        safe_value = _safe_css_value(name, value)
        if safe_value is None:
            continue
        seen.add(name)
        declarations.append(f"{name}:{safe_value}")
    return ";".join(declarations)


def _safe_css_value(name: str, value: str) -> str | None:
    lowered = value.lower()
    if name in {"color", "background-color"}:
        return value if _COLOR_RE.fullmatch(value) else None
    if name == "font-family":
        return _safe_font_family(value)
    if name == "font-size":
        return _safe_relative_font_size(lowered)
    if name == "text-align":
        return lowered if lowered in _TEXT_ALIGN_VALUES else None
    if name in {"margin-left", "padding-left"}:
        return _safe_relative_length(lowered, max_em=8, max_percent=50)
    if name == "font-style":
        return lowered if lowered in {"italic", "normal", "oblique"} else None
    if name == "font-weight":
        return (
            lowered
            if (
                lowered in {"bold", "bolder", "lighter", "normal"}
                or lowered in {str(weight) for weight in range(100, 1000, 100)}
            )
            else None
        )
    if name == "text-decoration":
        values = lowered.split()
        return (
            " ".join(values)
            if values
            and set(values)
            <= {
                "line-through",
                "none",
                "overline",
                "underline",
            }
            else None
        )
    return None


def _safe_font_family(value: str) -> str | None:
    families: list[str] = []
    for raw_family in value.split(","):
        family = raw_family.strip().strip("'\"")
        if (
            not family
            or len(family) > 40
            or not all(
                character.isalnum() or character in " _-" for character in family
            )
        ):
            return None
        families.append(family)
    return ", ".join(families) if families else None


def _safe_relative_font_size(value: str) -> str | None:
    if value in {
        "large",
        "larger",
        "medium",
        "small",
        "smaller",
        "x-large",
        "x-small",
        "xx-large",
        "xx-small",
    }:
        return value
    return _safe_relative_length(value, max_em=2.5, max_percent=250, min_em=0.5)


def _safe_relative_length(
    value: str,
    *,
    max_em: float,
    max_percent: float,
    min_em: float = 0,
) -> str | None:
    if value == "0":
        return value
    match = _RELATIVE_LENGTH_RE.fullmatch(value)
    if not match:
        return None
    number = float(match.group("number"))
    unit = match.group("unit")
    maximum = max_percent if unit == "%" else max_em
    minimum = min_em * 100 if unit == "%" else min_em
    return value if minimum <= number <= maximum else None


def _has_positive_text_indent(style: str) -> bool:
    for declaration in style.split(";"):
        name, separator, value = declaration.partition(":")
        if (
            separator
            and name.strip().lower() == "text-indent"
            and (_safe_relative_length(value.strip().lower(), max_em=8, max_percent=50))
            not in {None, "0"}
        ):
            return True
    return False


def _strip_explicit_indent_prefix(element: etree._Element) -> bool:
    for node, attribute in _text_slots(element):
        value = getattr(node, attribute)
        if not value:
            continue
        match = _INDENT_PREFIX_RE.match(value)
        if match:
            setattr(node, attribute, value[match.end() :])
            return True
        if value.strip():
            return False
    return False


def _text_slots(
    element: etree._Element,
) -> list[tuple[etree._Element, str]]:
    slots = [(element, "text")]
    for child in element:
        slots.extend(_text_slots(child))
        slots.append((child, "tail"))
    return slots


def _is_placeholder_image(
    source: str,
    attributes: dict[str, str],
) -> bool:
    if is_placeholder_image_url(source):
        return True
    return any(
        re.fullmatch(r"\s*(?:0|1(?:\.0+)?)\s*(?:px)?\s*", attributes.get(name, ""))
        is not None
        for name in ("height", "width")
    )


def safe_absolute_url(value: str, base_url: str) -> str:
    if not value or len(value) > 8192 or re.search(r"[\x00-\x20\x7f]", value):
        return ""
    try:
        absolute = urljoin(base_url, value)
        parsed = urlsplit(absolute)
        if (
            parsed.scheme.lower() not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
        ):
            return ""
        parsed.port
    except ValueError:
        return ""
    return absolute


def _image_proxy_url(source: str) -> str:
    return "/images/rss?" + urlencode({"src": source, "v": "5"})


def _copy_bounded_integer_attribute(
    element: etree._Element,
    attributes: dict[str, str],
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> None:
    try:
        value = int(attributes.get(name, ""))
    except ValueError:
        return
    if minimum <= value <= maximum:
        element.set(name, str(value))


def _wrap_container_inline_content(root: etree._Element) -> None:
    for container in [root, *list(root.iterdescendants())]:
        if (
            container is not root
            and _local_tag(container) not in _INLINE_RUN_CONTAINER_TAGS
        ):
            continue
        children = list(container)
        leading_text = container.text
        container.text = None
        for child in children:
            container.remove(child)

        paragraph: etree._Element | None = None

        def add_text(value: str | None) -> None:
            nonlocal paragraph
            if not value or (paragraph is None and not value.strip()):
                return
            if paragraph is None:
                paragraph = html.Element("p")
                container.append(paragraph)
            if len(paragraph):
                paragraph[-1].tail = (paragraph[-1].tail or "") + value
            else:
                paragraph.text = (paragraph.text or "") + value

        add_text(leading_text)
        for child in children:
            tail = child.tail
            child.tail = None
            if _local_tag(child) in _BLOCK_TAGS:
                paragraph = None
                container.append(child)
                add_text(tail)
                continue
            if paragraph is None:
                paragraph = html.Element("p")
                container.append(paragraph)
            paragraph.append(child)
            add_text(tail)


def _canonicalize_preformatted_blocks(root: etree._Element) -> None:
    for pre in root.iter("pre"):
        text = "".join(pre.itertext()).strip("\n")
        for child in list(pre):
            pre.remove(child)
        pre.text = None
        code = html.Element("code")
        code.text = text
        pre.append(code)


def _normalize_text_nodes(element: etree._Element) -> None:
    if _local_tag(element) == "pre":
        return
    element.text = _normalized_html_text(element.text, container=element)
    children = list(element)
    for index, child in enumerate(children):
        _normalize_text_nodes(child)
        next_child = children[index + 1] if index + 1 < len(children) else None
        child.tail = _normalized_html_tail(
            child.tail,
            child=child,
            next_child=next_child,
            parent=element,
        )


def _normalized_html_text(
    value: str | None,
    *,
    container: etree._Element,
) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[\t\r\n\f ]+", " ", value)
    if normalized.strip():
        return (
            normalized.lstrip() if _local_tag(container) in _BLOCK_TAGS else normalized
        )
    return None if _local_tag(container) in _CONTAINER_TAGS else " "


def _normalized_html_tail(
    value: str | None,
    *,
    child: etree._Element,
    next_child: etree._Element | None,
    parent: etree._Element,
) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[\t\r\n\f ]+", " ", value)
    if normalized.strip():
        return normalized
    if (
        _local_tag(parent) in _CONTAINER_TAGS
        or _local_tag(child) in _BLOCK_TAGS
        or (next_child is not None and _local_tag(next_child) in _BLOCK_TAGS)
    ):
        return None
    return " "


def _assign_block_ids(root: etree._Element) -> None:
    occurrences: dict[str, int] = {}
    for element in root.iter():
        if (
            not isinstance(element.tag, str)
            or _local_tag(element) not in _BLOCK_ID_TAGS
        ):
            continue
        text = _TRANSLATABLE_SPACE_RE.sub(" ", "".join(element.itertext())).strip()
        if not text:
            continue
        key = f"{_local_tag(element)}\0{text}"
        occurrence = occurrences.get(key, 0)
        occurrences[key] = occurrence + 1
        digest = hashlib.sha256(f"{key}\0{occurrence}".encode()).hexdigest()[:16]
        element.set("data-reader-block-id", f"block-{digest}")


def _render_container(element: etree._Element) -> str:
    parts = [rendered for child in element if (rendered := _render_block(child))]
    return re.sub(r"\n{3,}", "\n\n", "\n\n".join(parts)).strip()


def _render_block(element: etree._Element) -> str:
    tag = _local_tag(element)
    if tag in _CONTAINER_TAGS:
        return _render_container(element)
    if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
        return f"{'#' * int(tag[1])} {_render_inline(element)}".strip()
    if tag in {"p", "figcaption", "td", "th"}:
        return _render_inline(element)
    if tag == "blockquote":
        body = _render_container(element) or _render_inline(element)
        return "\n".join(f"> {line}" if line else ">" for line in body.splitlines())
    if tag in {"ul", "ol"}:
        return _render_list(element)
    if tag == "figure":
        return _render_container(element)
    if tag == "img":
        return _render_inline_node(element)
    if tag == "table":
        return _render_table(element)
    if tag == "pre":
        code = "".join(element.itertext()).strip("\n")
        fence = "`" * max(3, _longest_run(code, "`") + 1)
        return f"{fence}\n{code}\n{fence}"
    if tag == "hr":
        return "---"
    return _render_inline(element)


def _render_inline(element: etree._Element) -> str:
    parts = [element.text or ""]
    for child in element:
        parts.append(_render_inline_node(child))
        parts.append(child.tail or "")
    return _clean_inline_text("".join(parts))


def _render_inline_node(element: etree._Element) -> str:
    tag = _local_tag(element)
    body = _render_inline(element)
    if tag == "br":
        return "\n"
    if tag == "img":
        source = element.get("data-reader-original-src", "")
        alt = element.get("alt", "")
        return f"![{alt}]({_markdown_url(source)})" if source else alt
    if tag == "strong":
        return f"**{body}**" if body else ""
    if tag == "em":
        return f"*{body}*" if body else ""
    if tag == "code":
        delimiter = "`" * max(1, _longest_run(body, "`") + 1)
        return f"{delimiter}{body}{delimiter}" if body else ""
    if tag == "a":
        href = element.get("href", "")
        return f"[{body}]({_markdown_url(href)})" if href and body else body
    if tag in _BLOCK_TAGS:
        return _render_block(element)
    return body


def _render_list(element: etree._Element) -> str:
    ordered = _local_tag(element) == "ol"
    try:
        number = int(element.get("start", "1"))
    except ValueError:
        number = 1
    lines: list[str] = []
    for item in (child for child in element if _local_tag(child) == "li"):
        nested = [child for child in item if _local_tag(child) in {"ol", "ul"}]
        body_parts = [item.text or ""]
        for child in item:
            if child not in nested:
                body_parts.append(
                    _render_inline(child)
                    if _local_tag(child) == "p"
                    else _render_inline_node(child)
                )
            body_parts.append(child.tail or "")
        body = _clean_inline_text("".join(body_parts))
        try:
            item_number = int(item.get("value", str(number)))
        except ValueError:
            item_number = number
        marker = f"{item_number}." if ordered else "-"
        body_lines = body.splitlines() or [""]
        lines.append(f"{marker} {body_lines[0]}".rstrip())
        lines.extend(f"  {line}" for line in body_lines[1:])
        for child in nested:
            lines.extend(f"  {line}" for line in _render_list(child).splitlines())
        number = item_number + 1
    return "\n".join(lines)


def _render_table(element: etree._Element) -> str:
    rows: list[str] = []
    for row in element.iter("tr"):
        cells = [
            _render_inline(cell).replace("|", r"\|").replace("\n", " ")
            for cell in row
            if _local_tag(cell) in {"td", "th"}
        ]
        if cells:
            rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def _clean_inline_text(value: str) -> str:
    value = re.sub(r"[\t\r\f ]+", " ", value)
    return re.sub(r" *\n *", "\n", value).strip()


def _markdown_url(value: str) -> str:
    return f"<{value}>" if any(character in value for character in " ()") else value


def _longest_run(value: str, character: str) -> int:
    return max(
        (
            len(match.group())
            for match in re.finditer(re.escape(character) + "+", value)
        ),
        default=0,
    )


def _single_line(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()
