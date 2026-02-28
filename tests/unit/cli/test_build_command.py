from pathlib import Path
from types import SimpleNamespace
import contextlib

import pytest
from datapipeline.cli.commands import build as build_cmd
from datapipeline.config.build_resolution import resolve_build_settings
from datapipeline.config.tasks import BuildTask, SchemaTask, ScalerTask
from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext


class _TaskStub:
    def __init__(
        self,
        name: str,
        kind: str,
        output: str,
        enabled: bool = True,
        entrypoint: str = "core.build.schema",
    ) -> None:
        self._name = name
        self.kind = kind
        self.output = output
        self.enabled = enabled
        self.entrypoint = entrypoint

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

    settings = SimpleNamespace(mode="FORCE", force=True, visuals="on", profile_name=None)
    build_cmd._log_build_settings_debug(Path("/tmp/project.yaml"), settings)

    assert captured
    message, level, message_kind = captured[0]
    assert message.startswith("Build settings:")
    assert '"mode": "FORCE"' in message
    assert '"visuals": "on"' in message
    assert '"force"' not in message
    assert level == 10
    assert message_kind == "build_settings"


def test_run_artifact_builder_emits_materialized_message(monkeypatch):
    captured: list[tuple[str, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, message_kind)
        ),
    )

    runtime = SimpleNamespace(artifacts_root=Path("/tmp/artifacts"))
    definition = SimpleNamespace(key="vector_schema")
    task = _TaskStub(name="schema", kind="schema", output="schema.json")
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.run_build_operation",
        lambda operation, runtime: ("schema.json", {"features": 5, "targets": 0}),
    )

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


def test_run_build_if_needed_forwards_cli_log_outputs(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def _fake_resolve_build_settings(
        *,
        project_path=None,
        workspace,
        cli_log_level,
        cli_visuals,
        cli_log_outputs,
        force_flag,
        base_log_level,
        build_profile,
    ):
        captured["workspace"] = workspace
        captured["cli_log_level"] = cli_log_level
        captured["cli_visuals"] = cli_visuals
        captured["cli_log_outputs"] = cli_log_outputs
        captured["force_flag"] = force_flag
        captured["base_log_level"] = base_log_level
        captured["build_profile"] = build_profile
        return SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="OFF",
            force=False,
            profile_name=None,
        )

    monkeypatch.setattr(
        "datapipeline.cli.commands.build.resolve_build_settings",
        _fake_resolve_build_settings,
    )

    build_cmd.run_build_if_needed(
        tmp_path / "project.yaml",
        force=False,
        cli_visuals="off",
        cli_log_outputs=[LogOutputTarget(
            transport="fs",
            destination=tmp_path / "logs" / "jerry.log",
        )],
        workspace=SimpleNamespace(),
    )

    assert captured["cli_log_outputs"] == [LogOutputTarget(
        transport="fs",
        destination=tmp_path / "logs" / "jerry.log",
    )]
    assert captured["cli_log_level"] is None
    assert captured["build_profile"] is None


def test_resolve_build_settings_prefers_build_observability(tmp_path):
    cfg = WorkspaceConfig.model_validate(
        {
            "shared": {
                "observability": {
                    "visuals": "OFF",
                    "logging": {
                        "level": "INFO",
                        "outputs": [{"transport": "stderr"}],
                    },
                }
            },
            "build": {
                "mode": "AUTO",
                "observability": {
                    "visuals": "ON",
                    "logging": {
                        "level": "DEBUG",
                        "outputs": [{"transport": "fs", "path": "./logs/build.log"}],
                    },
                },
            },
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=cfg)

    settings = resolve_build_settings(
        workspace=workspace,
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="WARNING",
    )

    assert settings.visuals == "on"
    assert settings.log_decision.name == "DEBUG"
    assert settings.log_output.outputs[0].transport == "fs"
    assert settings.log_output.outputs[0].destination == (tmp_path / "logs" / "build.log").resolve()
    assert settings.profile_name is None


def test_resolve_build_settings_cli_overrides_build_observability(tmp_path):
    cfg = WorkspaceConfig.model_validate(
        {
            "build": {
                "mode": "AUTO",
                "observability": {
                    "visuals": "ON",
                    "logging": {
                        "level": "DEBUG",
                        "outputs": [{"transport": "fs", "path": "./logs/build.log"}],
                    },
                },
            },
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=cfg)
    cli_target = LogOutputTarget(transport="stdout")

    settings = resolve_build_settings(
        workspace=workspace,
        cli_log_level="ERROR",
        cli_visuals="off",
        cli_log_outputs=[cli_target],
        force_flag=False,
        base_log_level="INFO",
    )

    assert settings.visuals == "off"
    assert settings.log_decision.name == "ERROR"
    assert settings.log_output.outputs[0].transport == "stdout"


def test_resolve_build_settings_prefers_profile_over_workspace_build(tmp_path):
    cfg = WorkspaceConfig.model_validate(
        {
            "build": {
                "mode": "AUTO",
                "observability": {
                    "visuals": "OFF",
                    "logging": {
                        "level": "INFO",
                        "outputs": [{"transport": "stderr"}],
                    },
                },
            },
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=cfg)
    profile = BuildTask.model_validate(
        {
            "kind": "build",
            "name": "nightly",
            "target": "schema",
            "mode": "FORCE",
            "observability": {
                "visuals": "ON",
                "logging": {
                    "level": "DEBUG",
                    "outputs": [{"transport": "fs", "path": "./logs/nightly.log"}],
                },
            },
        }
    )

    settings = resolve_build_settings(
        project_path=tmp_path / "project.yaml",
        workspace=workspace,
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="WARNING",
        build_profile=profile,
    )

    assert settings.profile_name == "nightly"
    assert settings.mode == "FORCE"
    assert settings.visuals == "on"
    assert settings.log_decision.name == "DEBUG"
    assert settings.log_output.outputs[0].transport == "fs"
    assert settings.log_output.outputs[0].destination == (tmp_path / "logs" / "nightly.log").resolve()


def test_resolve_build_settings_requires_project_path_with_profile(tmp_path):
    workspace = WorkspaceContext(
        file_path=tmp_path / "jerry.yaml",
        config=WorkspaceConfig.model_validate({}),
    )
    profile = BuildTask.model_validate(
        {
            "kind": "build",
            "name": "nightly",
            "target": "schema",
        }
    )

    with pytest.raises(ValueError, match="project_path is required"):
        resolve_build_settings(
            workspace=workspace,
            cli_log_level=None,
            cli_visuals=None,
            cli_log_outputs=None,
            force_flag=False,
            base_log_level="INFO",
            build_profile=profile,
        )


def test_resolve_build_settings_profile_log_paths_are_project_relative(tmp_path):
    workspace_root = tmp_path / "workspace"
    project_root = workspace_root / "datasets" / "demo"
    workspace_root.mkdir(parents=True, exist_ok=True)
    project_root.mkdir(parents=True, exist_ok=True)
    workspace = WorkspaceContext(
        file_path=workspace_root / "jerry.yaml",
        config=WorkspaceConfig.model_validate({}),
    )
    project_path = project_root / "project.yaml"

    profile = BuildTask.model_validate(
        {
            "kind": "build",
            "name": "nightly",
            "target": "schema",
            "observability": {
                "logging": {
                    "outputs": [{"transport": "fs", "path": "./logs/nightly.log"}],
                },
            },
        }
    )

    settings = resolve_build_settings(
        project_path=project_path,
        workspace=workspace,
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="INFO",
        build_profile=profile,
    )
    assert settings.log_output.outputs[0].destination == (project_root / "logs" / "nightly.log").resolve()


def test_resolve_build_settings_ignores_run_scoped_workspace_outputs(tmp_path):
    cfg = WorkspaceConfig.model_validate(
        {
            "shared": {
                "observability": {
                    "logging": {
                        "outputs": [{"transport": "fs", "scope": "run"}],
                    },
                }
            },
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=cfg)

    settings = resolve_build_settings(
        workspace=workspace,
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="INFO",
    )

    assert settings.log_output.outputs[0].transport == "stderr"
    assert settings.log_output.outputs[0].destination is None


def test_run_build_if_needed_preserves_previous_artifacts_in_state(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text("version: 1\npaths:\n  tasks: ./tasks\n", encoding="utf-8")
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.cli.commands.build.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="AUTO",
            force=False,
            profile_name="schema",
        ),
    )
    monkeypatch.setattr("datapipeline.cli.commands.build.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.cli.commands.build.compute_config_hash", lambda *_: "hash-1")
    monkeypatch.setattr("datapipeline.cli.commands.build.artifacts_root", lambda _: tmp_path / "artifacts")
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.get_visuals_backend",
        lambda visuals: SimpleNamespace(
            on_build_start=lambda project: True,
            wrap_events=lambda level: contextlib.nullcontext(),
        ),
    )
    monkeypatch.setattr("datapipeline.cli.commands.build.configure_root_logging", lambda **kwargs: None)
    monkeypatch.setattr("datapipeline.cli.commands.build.bootstrap", lambda _: SimpleNamespace(artifacts_root=tmp_path / "artifacts"))
    monkeypatch.setattr("datapipeline.cli.commands.build.sections_from_path", lambda *_: ("Build Tasks",))
    monkeypatch.setattr("datapipeline.cli.commands.build.run_job", lambda **kwargs: kwargs["work"]())
    schema_task = SchemaTask(kind="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    scaler_task = ScalerTask(kind="scaler")
    scaler_task.source_path = tasks_root / "scaler.yaml"
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.artifact_tasks",
        lambda _: [schema_task, scaler_task],
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.build._run_artifact_builder",
        lambda *, definition, **kwargs: {"relative_path": f"{definition.task_kind}.json"},
    )

    schema_profile = BuildTask.model_validate({"kind": "build", "name": "schema", "target": "schema"})
    scaler_profile = BuildTask.model_validate({"kind": "build", "name": "scaler", "target": "scaler"})

    from datapipeline.cli.commands import build as build_cmd
    from datapipeline.build.state import load_build_state

    build_cmd.run_build_if_needed(project_path, build_profile=schema_profile)
    build_cmd.run_build_if_needed(project_path, build_profile=scaler_profile)

    state = load_build_state(tmp_path / "artifacts" / "build" / "state.json")
    assert state is not None
    assert set(state.artifacts.keys()) == {"vector_schema", "scaler_statistics"}


def test_run_build_if_needed_rebuilds_stale_profile_artifact(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text("version: 1\npaths:\n  tasks: ./tasks\n", encoding="utf-8")
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    from datapipeline.build.state import BuildState, save_build_state

    state_path = tmp_path / "artifacts" / "build" / "state.json"
    stale_state = BuildState(config_hash="hash-2")
    stale_state.register(
        "scaler_statistics",
        "scaler.json",
        meta={"_config_hash": "hash-1"},
    )
    save_build_state(stale_state, state_path)

    monkeypatch.setattr(
        "datapipeline.cli.commands.build.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="AUTO",
            force=False,
            profile_name="scaler",
        ),
    )
    monkeypatch.setattr("datapipeline.cli.commands.build.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.cli.commands.build.compute_config_hash", lambda *_: "hash-2")
    monkeypatch.setattr("datapipeline.cli.commands.build.artifacts_root", lambda _: tmp_path / "artifacts")
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.get_visuals_backend",
        lambda visuals: SimpleNamespace(
            on_build_start=lambda project: True,
            wrap_events=lambda level: contextlib.nullcontext(),
        ),
    )
    monkeypatch.setattr("datapipeline.cli.commands.build.configure_root_logging", lambda **kwargs: None)
    monkeypatch.setattr("datapipeline.cli.commands.build.bootstrap", lambda _: SimpleNamespace(artifacts_root=tmp_path / "artifacts"))
    monkeypatch.setattr("datapipeline.cli.commands.build.sections_from_path", lambda *_: ("Build Tasks",))

    calls = {"run_job": 0}
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.run_job",
        lambda **kwargs: (calls.__setitem__("run_job", calls["run_job"] + 1) or kwargs["work"]()),
    )
    scaler_task = ScalerTask(kind="scaler")
    scaler_task.source_path = tasks_root / "scaler.yaml"
    monkeypatch.setattr(
        "datapipeline.cli.commands.build.artifact_tasks",
        lambda _: [scaler_task],
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.build._run_artifact_builder",
        lambda *, definition, **kwargs: {"relative_path": f"{definition.task_kind}.json"},
    )

    scaler_profile = BuildTask.model_validate({"kind": "build", "name": "scaler", "target": "scaler"})
    from datapipeline.cli.commands import build as build_cmd

    did_build = build_cmd.run_build_if_needed(project_path, build_profile=scaler_profile)
    assert did_build is True
    assert calls["run_job"] == 1
