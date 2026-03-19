import logging
from pathlib import Path

from datapipeline.config.resolution import (
    LogOutputTarget,
    cascade,
    parse_log_output_specs,
    materialize_log_output_for_execution,
    minimum_level,
    resolve_log_level,
    resolve_log_output,
    resolve_visuals,
)
from datapipeline.config.workspace import WorkspaceConfig


def test_cascade_prefers_first_non_null():
    assert cascade(None, "cli", "cfg") == "cli"
    assert cascade(None, None, "cfg") == "cfg"
    assert cascade(None, None, fallback="default") == "default"


def test_resolve_visuals_applies_priority():
    visuals = resolve_visuals(
        cli_visuals="OFF",
        config_visuals="ON",
    )
    assert visuals.visuals == "off"

    visuals = resolve_visuals(
        cli_visuals=None,
        config_visuals=None,
        default_visuals="on",
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
            output_candidates=([LogOutputTarget(transport="fs", scope="execution")],),
        )
    except ValueError as exc:
        assert "only valid for execution-scoped operations" in str(exc)
    else:
        raise AssertionError("Expected ValueError for execution scope outside execution-aware resolution")


def test_materialize_log_output_for_execution_resolves_under_execution_directory(tmp_path):
    settings = resolve_log_output(
        output_candidates=([LogOutputTarget(transport="fs", scope="execution")],),
        allow_execution_scope=True,
    )
    resolved = materialize_log_output_for_execution(
        settings=settings,
        execution_dir=tmp_path / "_system" / "executions" / "e1",
        command="serve",
    )
    assert resolved.outputs[0].transport == "fs"
    assert resolved.outputs[0].destination == (
        tmp_path / "_system" / "executions" / "e1" / "logs" / "serve.execution.log"
    )


def test_parse_log_output_specs_parses_supported_targets(tmp_path):
    outputs = parse_log_output_specs(
        ["stderr", "execution:logs/serve.log", f"fs:{tmp_path / 'jerry.log'}"],
        resolve_global_path=lambda value: Path(value),
    )
    assert [item.transport for item in outputs] == ["stderr", "fs", "fs"]
    assert [item.scope for item in outputs] == ["global", "execution", "global"]
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


def test_workspace_rejects_serve_and_build_blocks():
    try:
        WorkspaceConfig.model_validate({"serve": {"limit": 10}})
    except Exception as exc:
        assert "serve" in str(exc)
    else:
        raise AssertionError("Expected validation failure for workspace serve defaults")

    try:
        WorkspaceConfig.model_validate({"build": {"mode": "AUTO"}})
    except Exception as exc:
        assert "build" in str(exc)
    else:
        raise AssertionError("Expected validation failure for workspace build defaults")
