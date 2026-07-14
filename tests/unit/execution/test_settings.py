import logging
from pathlib import Path

import pytest

from datapipeline.execution.settings import (
    LogOutputSettings,
    LogOutputTarget,
    resolve_execution_log_outputs,
    resolve_heartbeat_interval_seconds,
    resolve_log_level,
    resolve_log_output,
    resolve_visuals,
)


def test_resolve_visuals_applies_cli_config_default_precedence():
    assert resolve_visuals("OFF", "ON") == "off"
    assert resolve_visuals(None, "OFF") == "off"
    assert resolve_visuals(None, None) == "on"


def test_resolve_heartbeat_interval_applies_cli_config_precedence():
    assert resolve_heartbeat_interval_seconds(10, 60) == 10
    assert resolve_heartbeat_interval_seconds(None, 30) == 30
    assert resolve_heartbeat_interval_seconds(0, 30) == 0
    assert resolve_heartbeat_interval_seconds(None, None) is None


def test_resolve_heartbeat_interval_rejects_negative_values():
    with pytest.raises(ValueError, match="must be non-negative"):
        resolve_heartbeat_interval_seconds(-1, None)


def test_resolve_log_level_applies_cli_config_default_precedence():
    assert resolve_log_level("debug", "ERROR", "WARNING").name == "DEBUG"
    assert resolve_log_level(None, "error", "WARNING").name == "ERROR"

    default = resolve_log_level(None, None, "INFO")
    assert default.name == "INFO"
    assert default.value == logging.INFO


@pytest.mark.parametrize("level", ["TRACE", "", "unknown"])
def test_resolve_log_level_rejects_invalid_explicit_levels(level):
    with pytest.raises(ValueError, match="log level"):
        resolve_log_level(level)


def test_resolve_log_output_applies_cli_config_default_precedence():
    cli = LogOutputTarget(transport="stdout")
    configured = LogOutputTarget(
        transport="fs",
        destination=Path("/tmp/configured.log"),
    )

    resolved = resolve_log_output(
        cli_outputs=[cli],
        config_outputs=[configured],
    )
    assert resolved.outputs == (cli,)

    resolved = resolve_log_output(cli_outputs=[], config_outputs=[configured])
    assert resolved.outputs == (configured,)

    resolved = resolve_log_output()
    assert resolved.outputs == (LogOutputTarget(transport="stderr"),)


def test_resolve_log_output_rejects_missing_file_destination():
    with pytest.raises(ValueError, match="requires a log path"):
        resolve_log_output(cli_outputs=[LogOutputTarget(transport="fs")])


def test_resolve_log_output_rejects_execution_scope_when_not_allowed():
    with pytest.raises(
        ValueError,
        match="only valid for execution-scoped operations",
    ):
        resolve_log_output(
            cli_outputs=[LogOutputTarget(transport="fs", scope="execution")]
        )


def test_resolve_log_output_rejects_invalid_default_transport():
    with pytest.raises(ValueError, match="default log transport"):
        resolve_log_output(default_transport="udp")


def test_resolve_execution_log_outputs_resolves_under_execution_directory(
    tmp_path,
):
    settings = resolve_log_output(
        cli_outputs=[LogOutputTarget(transport="fs", scope="execution")],
        allow_execution_scope=True,
    )
    resolved = resolve_execution_log_outputs(
        settings,
        tmp_path / "_system" / "executions" / "e1",
        default_path=Path("logs") / "serve.execution.log",
    )

    assert resolved.outputs == (
        LogOutputTarget(
            transport="fs",
            destination=(
                tmp_path
                / "_system"
                / "executions"
                / "e1"
                / "logs"
                / "serve.execution.log"
            ),
        ),
    )


def test_execution_log_path_cannot_escape_execution_directory(tmp_path):
    with pytest.raises(ValueError, match="stay inside"):
        resolve_log_output(
            cli_outputs=[
                LogOutputTarget(
                    transport="fs",
                    destination=Path("..") / "escaped.log",
                    scope="execution",
                )
            ],
            allow_execution_scope=True,
        )

    settings = LogOutputSettings(
        outputs=(LogOutputTarget(transport="fs", scope="execution"),)
    )
    with pytest.raises(ValueError, match="stay inside"):
        resolve_execution_log_outputs(
            settings,
            tmp_path / "execution",
            default_path=Path("..") / "escaped.log",
        )
