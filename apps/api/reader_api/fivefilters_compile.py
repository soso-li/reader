from __future__ import annotations

import re


SAFE_DIRECTIVES = frozenset({"body", "strip", "strip_id_or_class"})
IGNORED_METADATA = frozenset({"author", "date", "test_url", "title"})
DIRECTIVE_RE = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_]*)(?:\([^)]*\))?\s*:(.*)$"
)


def compile_rule_text(
    text: str,
) -> tuple[dict[str, list[str]] | None, str]:
    values = {directive: [] for directive in SAFE_DIRECTIVES}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = DIRECTIVE_RE.match(line)
        if match is None:
            return None, "hard_dependency"
        directive, value = match.group(1), match.group(2).strip()
        if directive in SAFE_DIRECTIVES:
            if value:
                values[directive].append(value)
        elif directive not in IGNORED_METADATA:
            return None, "hard_dependency"
    if not values["body"]:
        return None, ""
    return values, ""
