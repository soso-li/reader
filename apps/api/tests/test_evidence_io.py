from __future__ import annotations

import json
from pathlib import Path
import stat

import pytest

from reader_api.evidence_io import (
    EvidenceError,
    publish_evidence_from_stdin,
    write_evidence_text,
)


def test_text_evidence_is_private_durable_and_never_overwritten(
    tmp_path: Path,
) -> None:
    output = tmp_path / "dump.list"

    write_evidence_text(output, "archive line\n")

    assert output.read_text(encoding="utf-8") == "archive line\n"
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    with pytest.raises(EvidenceError, match="拒绝覆盖既有证据文件"):
        write_evidence_text(output, "replacement\n")
    assert output.read_text(encoding="utf-8") == "archive line\n"


def test_stdin_publisher_validates_json_before_atomic_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "smoke.json"
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO('{"ok":true}'))

    assert publish_evidence_from_stdin(output, input_format="json") == 0

    assert json.loads(output.read_text(encoding="utf-8")) == {"ok": True}
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_stdin_publisher_rejects_invalid_json_without_leaving_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "smoke.json"
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO("not-json"))

    with pytest.raises(EvidenceError, match="JSON"):
        publish_evidence_from_stdin(output, input_format="json")

    assert not output.exists()
