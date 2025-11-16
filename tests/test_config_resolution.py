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
        cli_visuals="RICH",
        config_visuals="TQDM",
        workspace_visuals="auto",
        cli_progress=None,
        config_progress="BARS",
        workspace_progress="spinner",
    )
    assert visuals.visuals == "rich"
    assert visuals.progress == "bars"

    visuals = resolve_visuals(
        cli_visuals=None,
        config_visuals=None,
        workspace_visuals="tqdm",
        cli_progress=None,
        config_progress=None,
        workspace_progress="spinner",
    )
    assert visuals.visuals == "tqdm"
    assert visuals.progress == "spinner"


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
                "output": {
                    "transport": "fs",
                    "format": "json-lines",
                    "directory": "outputs",
                }
            }
        }
    )
    context = WorkspaceContext(file_path=workspace_file, config=cfg)
    resolved = workspace_output_defaults(context)
    assert resolved.transport == "fs"
    assert resolved.format == "json-lines"
    assert resolved.directory == (tmp_path / "outputs").resolve()


def test_workspace_build_mode_accepts_booleans():
    cfg = WorkspaceConfig.model_validate({"build": {"mode": False}})
    assert cfg.build.mode == "OFF"

    cfg = WorkspaceConfig.model_validate({"build": {"mode": True}})
    assert cfg.build.mode == "AUTO"
