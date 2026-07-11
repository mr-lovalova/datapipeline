from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget
from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.runtime import Runtime


def _log_output() -> LogOutputSettings:
    return LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),))


def _runtime() -> Runtime:
    return Runtime(project_yaml=Path("."), artifacts_root=Path("."))


def test_profile_execution_spec_requires_runtime():
    with pytest.raises(TypeError, match="runtime"):
        ProfileExecutionSpec(
            command="build",
            name="schema",
            index=1,
            total=1,
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=_log_output(),
        )


def test_run_profile_emits_start_inside_visual_context_before_work(monkeypatch):
    runtime = _runtime()
    configured = []
    calls = []
    inside_visual_context = False

    monkeypatch.setattr(
        "datapipeline.profiles.executor.configure_root_logging",
        lambda level, output: configured.append((level, output)),
    )

    def emit_profile_started(command, name, index, total):
        calls.append(("profile", command, name, index, total, inside_visual_context))

    monkeypatch.setattr(
        "datapipeline.profiles.executor.emit_profile_started",
        emit_profile_started,
    )

    def run_with_backend(visuals, runtime, level, work):
        nonlocal inside_visual_context
        calls.append(("visuals", visuals, runtime, level))
        inside_visual_context = True
        try:
            return work()
        finally:
            inside_visual_context = False

    monkeypatch.setattr(
        "datapipeline.profiles.executor.run_with_backend",
        run_with_backend,
    )

    def work():
        calls.append(("work", inside_visual_context))
        return "ok"

    log_output = _log_output()
    result = run_profile(
        ProfileExecutionSpec(
            command="build",
            name="schema",
            index=2,
            total=3,
            visuals="on",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=log_output,
            runtime=runtime,
        ),
        work,
    )

    assert result == "ok"
    assert configured == [(20, log_output)]
    assert calls == [
        ("visuals", "on", runtime, 20),
        ("profile", "build", "schema", 2, 3, True),
        ("work", True),
    ]
