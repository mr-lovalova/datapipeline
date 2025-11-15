import logging

from datapipeline.config.resolution import (
    cascade,
    minimum_level,
    resolve_log_level,
    resolve_visuals,
    workspace_output_defaults,
)
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext


def test_cascade_prefers_first_non_null():
    assert cascade(None, "cli", "cfg") == "cli"
    assert cascade(None, None, "cfg") == "cfg"
    assert cascade(None, None, fallback="default") == "default"


def test_resolve_visuals_applies_priority():
    visuals = resolve_visuals(
        cli_provider="RICH",
        config_provider="TQDM",
        workspace_provider="auto",
        cli_style=None,
        config_style="BARS",
        workspace_style="spinner",
    )
    assert visuals.provider == "rich"
    assert visuals.progress_style == "bars"

    visuals = resolve_visuals(
        cli_provider=None,
        config_provider=None,
        workspace_provider="tqdm",
        cli_style=None,
        config_style=None,
        workspace_style="spinner",
    )
    assert visuals.provider == "tqdm"
    assert visuals.progress_style == "spinner"


def test_resolve_log_level_handles_fallbacks():
    decision = resolve_log_level("debug", fallback="INFO")
    assert decision.name == "DEBUG"
    assert decision.value == logging.DEBUG

    default = resolve_log_level(None, fallback="INFO")
    assert default.name == "INFO"
    assert default.value == logging.INFO


def test_minimum_level_prefers_lowest_numeric():
    baseline = logging.WARNING
    resolved = minimum_level("DEBUG", "ERROR", start=baseline)
    assert resolved == logging.DEBUG
    assert minimum_level(start=None) is None


def test_workspace_output_defaults_handles_relative_paths(tmp_path):
    workspace_file = tmp_path / "jerry.yaml"
    workspace_file.write_text("visuals: {}\n", encoding="utf-8")
    cfg = WorkspaceConfig.model_validate(
        {
            "serve": {
                "output_defaults": {
                    "transport": "fs",
                    "format": "json-lines",
                    "path": "outputs/run.jsonl",
                }
            }
        }
    )
    context = WorkspaceContext(file_path=workspace_file, config=cfg)
    resolved = workspace_output_defaults(context)
    assert resolved.transport == "fs"
    assert resolved.format == "json-lines"
    assert resolved.path == (tmp_path / "outputs" / "run.jsonl").resolve()
