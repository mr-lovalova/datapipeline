from __future__ import annotations

import textwrap
from pathlib import Path

from datapipeline.config.workspace import load_workspace_context


def _write_jerry(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "jerry.yaml"
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return path


def test_shared_visuals_alias_keys(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visuals_provider: OFF
          visuals_progress_style: bars
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visual_provider == "OFF"
    assert shared.progress_style == "BARS"


def test_shared_visual_provider_literal_off(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visual_provider: OFF
          progress_style: OFF
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visual_provider == "OFF"
    assert shared.progress_style == "OFF"


def test_alias_overrides_existing_key(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visual_provider: AUTO
          visuals_provider: OFF
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    assert context.config.shared.visual_provider == "OFF"


def test_top_level_visuals_block(tmp_path):
    _write_jerry(
        tmp_path,
        """
        visuals:
          provider: off
          progress_style: spinner
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visual_provider == "OFF"
    assert shared.progress_style == "SPINNER"
