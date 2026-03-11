from types import SimpleNamespace

import pytest

from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget
from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile


def test_run_profile_without_visual_runner(monkeypatch):
    configured: list[tuple[int, LogOutputSettings]] = []
    messages: list[str] = []
    monkeypatch.setattr(
        "datapipeline.profiles.executor.configure_root_logging",
        lambda level, output: configured.append((level, output)),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.executor.emit_execution_message",
        lambda message, level, logger, message_kind=None: messages.append(message),
    )

    called = {"work": 0}

    def _work():
        called["work"] += 1
        return "ok"

    result = run_profile(
        spec=ProfileExecutionSpec(
            command="build",
            name="schema",
            idx=1,
            total=1,
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            use_visual_runner=False,
        ),
        work=_work,
    )

    assert result == "ok"
    assert called["work"] == 1
    assert configured
    assert messages and messages[0].startswith("Profile start ")
    assert "command=build" in messages[0]
    assert "name=schema" in messages[0]


def test_run_profile_visual_runner_requires_runtime():
    with pytest.raises(ValueError, match="runtime is required"):
        run_profile(
            spec=ProfileExecutionSpec(
                command="build",
                name="schema",
                idx=1,
                total=1,
                visuals="off",
                log_decision=SimpleNamespace(name="INFO", value=20),
                log_output=LogOutputSettings(
                    outputs=(LogOutputTarget(transport="stderr"),)
                ),
                use_visual_runner=True,
            ),
            work=lambda: None,
        )


def test_run_profile_can_skip_header_without_visual_runner(monkeypatch):
    monkeypatch.setattr(
        "datapipeline.profiles.executor.configure_root_logging",
        lambda level, output: None,
    )
    messages: list[str] = []
    monkeypatch.setattr(
        "datapipeline.profiles.executor.emit_execution_message",
        lambda message, level, logger, message_kind=None: messages.append(message),
    )

    rendered = {"count": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.executor._render_profile_header",
        lambda _spec: rendered.__setitem__("count", rendered["count"] + 1),
    )

    run_profile(
        spec=ProfileExecutionSpec(
            command="build",
            name="coverage.skip_header",
            idx=1,
            total=1,
            visuals="on",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            use_visual_runner=False,
            render_header=False,
        ),
        work=lambda: None,
    )

    assert rendered["count"] == 0
    assert messages and messages[0].startswith("Profile start ")
