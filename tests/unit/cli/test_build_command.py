from pathlib import Path
from types import SimpleNamespace

from datapipeline.cli.commands import build as build_cmd


class _TaskStub:
    def __init__(self, name: str, kind: str, output: str, enabled: bool = True) -> None:
        self._name = name
        self.kind = kind
        self.output = output
        self.enabled = enabled

    def effective_name(self) -> str:
        return self._name


def test_log_build_settings_debug_emits_execution_message(monkeypatch):
    captured: list[tuple[str, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, message_kind)
        ),
    )

    settings = SimpleNamespace(mode="FORCE", force=True, visuals="on")
    build_cmd._log_build_settings_debug(Path("/tmp/project.yaml"), settings)

    assert captured
    message, level, message_kind = captured[0]
    assert message.startswith("Build settings:")
    assert level == 10
    assert message_kind == "build_settings"


def test_log_task_overview_emits_execution_message(monkeypatch):
    captured: list[tuple[str, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, message_kind)
        ),
    )

    tasks = [_TaskStub(name="schema", kind="schema", output="schema.json")]
    build_cmd._log_task_overview(tasks)

    assert captured
    message, level, message_kind = captured[0]
    assert message.startswith("Artifact tasks:")
    assert level == 10
    assert message_kind == "artifact_tasks"


def test_run_artifact_builder_emits_materialized_message(monkeypatch):
    captured: list[tuple[str, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, message_kind)
        ),
    )

    runtime = SimpleNamespace(artifacts_root=Path("/tmp/artifacts"))
    definition = SimpleNamespace(
        key="vector_schema",
        run=lambda runtime, task: ("schema.json", {"features": 5, "targets": 0}),
    )
    task = _TaskStub(name="schema", kind="schema", output="schema.json")

    result = build_cmd._run_artifact_builder(
        runtime=runtime,
        definition=definition,
        task=task,
    )

    assert result is not None
    assert result["relative_path"] == "schema.json"
    assert captured
    message, level, message_kind = captured[0]
    assert message.startswith("Materialized vector_schema: ")
    assert level == 20
    assert message_kind == "materialized"
