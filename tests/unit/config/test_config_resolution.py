import logging
from pathlib import Path

from datapipeline.config.resolution import (
    LogOutputTarget,
    cascade,
    parse_log_output_specs,
    materialize_log_output_for_run,
    minimum_level,
    resolve_log_level,
    resolve_log_output,
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
        cli_visuals="OFF",
        config_visuals="ON",
        workspace_visuals="ON",
    )
    assert visuals.visuals == "off"

    visuals = resolve_visuals(
        cli_visuals=None,
        config_visuals=None,
        workspace_visuals="ON",
    )
    assert visuals.visuals == "on"


def test_resolve_log_level_handles_fallbacks():
    decision = resolve_log_level("debug", fallback="INFO")
    assert decision.name == "DEBUG"
    assert decision.value == logging.DEBUG

    default = resolve_log_level(None, fallback="INFO")
    assert default.name == "INFO"
    assert default.value == logging.INFO


def test_resolve_log_output_prefers_first_non_empty_candidate():
    resolved = resolve_log_output(
        output_candidates=(
            [],
            [LogOutputTarget(transport="stdout")],
            [LogOutputTarget(transport="fs", destination=Path("/tmp/ignored.log"))],
        )
    )
    assert resolved.outputs[0].transport == "stdout"
    assert resolved.outputs[0].destination is None


def test_resolve_log_output_validates_file_destination():
    try:
        resolve_log_output(output_candidates=([LogOutputTarget(transport="fs")],))
    except ValueError as exc:
        assert "requires a log path" in str(exc)
    else:
        raise AssertionError("Expected ValueError for fs transport without destination")


def test_resolve_log_output_rejects_run_scope_when_not_allowed():
    try:
        resolve_log_output(
            output_candidates=([LogOutputTarget(transport="fs", scope="run")],),
        )
    except ValueError as exc:
        assert "only valid for serve execution" in str(exc)
    else:
        raise AssertionError("Expected ValueError for run scope outside serve")


def test_materialize_log_output_for_run_resolves_under_run_directory(tmp_path):
    settings = resolve_log_output(
        output_candidates=([LogOutputTarget(transport="fs", scope="run")],),
        allow_run_scope=True,
    )
    resolved = materialize_log_output_for_run(
        settings=settings,
        run_dir=tmp_path / "runs" / "r1" / "dataset",
    )
    assert resolved.outputs[0].transport == "fs"
    assert resolved.outputs[0].destination == (
        tmp_path / "runs" / "r1" / "dataset" / "logs" / "serve.run.log"
    )


def test_parse_log_output_specs_parses_supported_targets(tmp_path):
    outputs = parse_log_output_specs(
        ["stderr", "run:logs/serve.log", f"fs:{tmp_path / 'jerry.log'}"],
        resolve_global_path=lambda value: Path(value),
    )
    assert [item.transport for item in outputs] == ["stderr", "fs", "fs"]
    assert [item.scope for item in outputs] == ["global", "run", "global"]
    assert outputs[1].destination == Path("logs/serve.log")
    assert outputs[2].destination == (tmp_path / "jerry.log")


def test_parse_log_output_specs_rejects_invalid_values():
    try:
        parse_log_output_specs(["file:./x.log"], resolve_global_path=Path)
    except ValueError as exc:
        assert "invalid --log-output value" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported log output syntax")


def test_minimum_level_prefers_lowest_numeric():
    baseline = logging.WARNING
    resolved = minimum_level("DEBUG", "ERROR", start=baseline)
    assert resolved == logging.DEBUG
    assert minimum_level(start=None) is None


def test_workspace_output_defaults_handles_relative_paths(tmp_path):
    workspace_file = tmp_path / "jerry.yaml"
    workspace_file.write_text("shared: {}\n", encoding="utf-8")
    cfg = WorkspaceConfig.model_validate(
        {
            "serve": {
                "output": {
                    "transport": "fs",
                    "format": "jsonl",
                    "directory": "outputs",
                }
            }
        }
    )
    context = WorkspaceContext(file_path=workspace_file, config=cfg)
    resolved = workspace_output_defaults(context)
    assert resolved.transport == "fs"
    assert resolved.format == "jsonl"
    assert resolved.directory == (tmp_path / "outputs").resolve()


def test_workspace_build_mode_accepts_booleans():
    cfg = WorkspaceConfig.model_validate({"build": {"mode": False}})
    assert cfg.build.mode == "OFF"

    cfg = WorkspaceConfig.model_validate({"build": {"mode": True}})
    assert cfg.build.mode == "AUTO"
