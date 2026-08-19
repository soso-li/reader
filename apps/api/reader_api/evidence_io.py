from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from collections.abc import Mapping


class EvidenceError(RuntimeError):
    """Evidence is incomplete, invalid, or cannot be published safely."""


def canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _write_evidence_bytes(path: Path, payload: bytes) -> None:
    """Publish one private artifact exactly once and durably."""
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise EvidenceError(f"拒绝覆盖既有证据文件：{path}")
    temporary_path: Path | None = None

    def cleanup_temporary() -> OSError | None:
        nonlocal temporary_path
        if temporary_path is None:
            return None
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError as exc:
            return exc
        temporary_path = None
        return None

    def cleanup_failure_detail() -> str:
        cleanup_error = cleanup_temporary()
        return (
            f"；临时文件清理失败：{cleanup_error}" if cleanup_error else ""
        )

    try:
        with tempfile.NamedTemporaryFile(
            "wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(payload)
            temporary.flush()
            os.fchmod(temporary.fileno(), 0o600)
            os.fsync(temporary.fileno())
    except OSError as exc:
        raise EvidenceError(
            f"无法写入证据临时文件 {path}：{exc}{cleanup_failure_detail()}"
        ) from exc

    assert temporary_path is not None
    try:
        os.link(temporary_path, path)
    except FileExistsError as exc:
        raise EvidenceError(
            f"拒绝覆盖既有证据文件：{path}{cleanup_failure_detail()}"
        ) from exc
    except OSError as exc:
        raise EvidenceError(
            f"无法原子发布证据文件 {path}：{exc}{cleanup_failure_detail()}"
        ) from exc

    cleanup_error = cleanup_temporary()
    if cleanup_error is not None:
        raise EvidenceError(
            f"证据文件已原子发布到 {path}，但临时文件清理失败：{cleanup_error}"
        ) from cleanup_error
    try:
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        raise EvidenceError(
            f"证据文件已原子发布到 {path}，但目录同步失败：{exc}"
        ) from exc


def write_evidence_text(path: Path, payload: str) -> None:
    """Publish one private UTF-8 text artifact exactly once and durably."""
    _write_evidence_bytes(path, payload.encode("utf-8"))


def write_evidence_json(path: Path, payload: Mapping[str, object]) -> None:
    """Publish one private JSON artifact exactly once and durably."""
    write_evidence_text(path, canonical_json(payload) + "\n")


def publish_evidence_from_stdin(path: Path, *, input_format: str) -> int:
    """Validate stdin and publish it through the protected evidence writer."""
    raw = sys.stdin.read()
    if input_format == "text":
        write_evidence_text(path, raw)
        return 0
    if input_format != "json":
        raise EvidenceError(f"不支持的证据输入格式：{input_format}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise EvidenceError(f"证据输入不是有效 JSON：{exc}") from exc
    if not isinstance(payload, Mapping):
        raise EvidenceError("JSON 证据顶层必须是对象")
    write_evidence_json(path, payload)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="受保护证据文件发布入口")
    parser.add_argument("action", choices=("publish",))
    parser.add_argument("--input-format", choices=("text", "json"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return publish_evidence_from_stdin(
            args.output,
            input_format=args.input_format,
        )
    except (EvidenceError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
