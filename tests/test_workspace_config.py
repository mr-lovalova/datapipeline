from __future__ import annotations

import textwrap
from pathlib import Path

from datapipeline.config.workspace import load_workspace_context


def _write_jerry(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "jerry.yaml"
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return path


def test_shared_visuals_config(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visuals: OFF
          progress: bars
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visuals == "OFF"
    assert shared.progress == "BARS"


def test_shared_visuals_defaults_when_missing(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visuals: auto
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visuals == "AUTO"
    assert shared.progress is None
