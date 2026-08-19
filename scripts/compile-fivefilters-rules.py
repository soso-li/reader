#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apps" / "api"))

from reader_api.fivefilters_compile import compile_rule_text  # noqa: E402


def compile_rule(path: Path) -> tuple[dict[str, list[str]] | None, str]:
    return compile_rule_text(path.read_text(encoding="utf-8-sig"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()

    rules: dict[str, dict[str, list[str]]] = {}
    skipped: dict[str, str] = {}
    for path in sorted(args.source.glob("*.txt")):
        hostname = path.name.removesuffix(".txt").lower()
        rule, reason = compile_rule(path)
        if rule is not None:
            rules[hostname] = rule
        elif reason:
            skipped[hostname] = reason
    payload = {
        "version": f"fivefilters@{args.commit}",
        "rules": rules,
        "skipped": skipped,
    }
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
