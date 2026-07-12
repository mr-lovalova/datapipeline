from pathlib import Path
from types import SimpleNamespace

from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget
from datapipeline.profiles.executor import ExecutionSpec, run_execution
from datapipeline.runtime import Runtime


def _log_output() -> LogOutputSettings:
    return LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),))


def _runtime() -> Runtime:
    return Runtime(project_yaml=Path("."), artifacts_root=Path("."))


def test_run_execution_configures_logging_and_runs_work_inside_backend(monkeypatch):
    runtime = _runtime()
    calls = []
    inside_visual_context = False

    monkeypatch.setattr(
        "datapipeline.profiles.executor.configure_root_logging",
        lambda level, output: calls.append(("logging", level, output)),
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
    result = run_execution(
        ExecutionSpec(
            visuals="on",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=log_output,
            runtime=runtime,
        ),
        work,
    )

    assert result == "ok"
    assert calls == [
        ("logging", 20, log_output),
        ("visuals", "on", runtime, 20),
        ("work", True),
    ]
