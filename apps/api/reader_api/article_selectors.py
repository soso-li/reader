import re

from cssselect import SelectorError
from lxml import etree
from lxml.cssselect import CSSSelector


_XPATH_FUNCTIONS = frozenset(
    {
        "boolean",
        "ceiling",
        "comment",
        "concat",
        "contains",
        "count",
        "false",
        "floor",
        "id",
        "lang",
        "last",
        "local-name",
        "name",
        "namespace-uri",
        "node",
        "normalize-space",
        "not",
        "number",
        "position",
        "processing-instruction",
        "round",
        "starts-with",
        "string",
        "string-length",
        "substring",
        "substring-after",
        "substring-before",
        "sum",
        "text",
        "translate",
        "true",
    }
)


def _validate_xpath_1_0(expression: str) -> None:
    without_strings = re.sub(r"'[^']*'|\"[^\"]*\"", "", expression)
    if re.search(r"(?<!:)\b[A-Za-z_][\w.-]*:(?!:)", without_strings):
        raise ValueError("XPath 扩展与命名空间前缀不受支持")
    functions = re.findall(
        r"(?<![\w.-])([A-Za-z_][\w.-]*)\s*\(", without_strings
    )
    if any(function not in _XPATH_FUNCTIONS for function in functions):
        raise ValueError("只支持 XPath 1.0 内置函数")
    _validate_xpath_element_result(without_strings)


def _validate_xpath_element_result(expression: str) -> None:
    candidates = [_without_predicates(expression).strip()]
    while candidates:
        candidate = candidates.pop()
        while (
            candidate.startswith("(")
            and candidate.endswith(")")
            and _matching_parenthesis(candidate, 0) == len(candidate) - 1
        ):
            candidate = candidate[1:-1].strip()
        branches = _top_level_parts(candidate, "|")
        if len(branches) > 1:
            candidates.extend(branches)
            continue
        path_parts = _top_level_parts(candidate, "/")
        step = path_parts[-1].strip()
        if step == ".":
            if len(path_parts) == 1:
                continue
            prefix = "/".join(path_parts[:-1]).strip()
            if prefix:
                candidates.append(prefix)
                continue
        if step in {"..", "*"} or re.fullmatch(
            r"(?:[A-Za-z_][\w.-]*::)?(?:\*|[A-Za-z_][\w.-]*)",
            step,
        ):
            axis = step.partition("::")[0] if "::" in step else ""
            if axis not in {"attribute", "namespace"}:
                continue
        if re.fullmatch(r"id\s*\(.*\)", step):
            continue
        raise ValueError("XPath 选择器只能返回元素节点")


def _without_predicates(value: str) -> str:
    result: list[str] = []
    depth = 0
    for character in value:
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
        elif depth == 0:
            result.append(character)
    return "".join(result)


def _matching_parenthesis(value: str, start: int) -> int:
    depth = 0
    for index, character in enumerate(value[start:], start=start):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return index
    return -1


def _top_level_parts(value: str, separator: str) -> list[str]:
    parts: list[str] = []
    start = 0
    parentheses = brackets = 0
    for index, character in enumerate(value):
        if character == "(":
            parentheses += 1
        elif character == ")":
            parentheses -= 1
        elif character == "[":
            brackets += 1
        elif character == "]":
            brackets -= 1
        elif character == separator and parentheses == brackets == 0:
            parts.append(value[start:index])
            start = index + 1
    parts.append(value[start:])
    return parts


def _compile(selector: str) -> CSSSelector | etree.XPath:
    value = selector.strip()
    kind = "css"
    if value.startswith("css:"):
        value = value[4:].strip()
    elif value.startswith("xpath:"):
        kind = "xpath"
        value = value[6:].strip()
    if not value:
        raise ValueError("正文选择器不能为空")

    try:
        if kind == "css":
            return CSSSelector(value, translator="html")
        _validate_xpath_1_0(value)
        return etree.XPath(value, smart_strings=False, regexp=False)
    except (SelectorError, etree.XPathError) as exc:
        raise ValueError(f"{kind.upper()} 选择器语法无效") from exc


def validate_article_selector(selector: str) -> str:
    _evaluate(_compile(selector), etree.Element("html"))
    return selector


def _evaluate(
    compiled: CSSSelector | etree.XPath, document: etree._Element
) -> list[etree._Element]:
    try:
        matches = compiled(document)
    except etree.XPathError as exc:
        raise ValueError("XPath 选择器无法安全求值") from exc
    if not isinstance(matches, list) or any(
        not isinstance(match, etree._Element) or not isinstance(match.tag, str)
        for match in matches
    ):
        raise ValueError("XPath 选择器只能返回元素节点")
    return matches


def select_article_elements(
    document: etree._Element, selector: str
) -> list[etree._Element]:
    return _evaluate(_compile(selector), document)
