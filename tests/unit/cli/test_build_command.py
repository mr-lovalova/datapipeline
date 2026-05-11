from pathlib import Path
from types import SimpleNamespace

import pytest
from datapipeline.artifacts import executor as build_exec
from datapipeline.config.build_resolution import resolve_build_settings
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget
from datapipeline.config.tasks import SchemaTask, ScalerTask, StatsTask
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA_METADATA,
    VECTOR_STATS,
)


class _TaskStub:
    def __init__(
        self,
        id: str,
        output: str,
        enabled: bool = True,
        entrypoint: str = "core.artifact.schema",
    ) -> None:
        self.id = id
        self.output = output
        self.enabled = enabled
        self.entrypoint = entrypoint


def _dataset_with_feature(*, scale: bool) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                id="x",
                record_stream="stream",
                field="value",
                scale=scale,
            )
        ],
        targets=[],
    )


def test_log_build_decision_emits_execution_message(monkeypatch):
    captured: list[tuple[str, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, message_kind)
        ),
    )

    settings = SimpleNamespace(mode="AUTO", profile_name="metadata")
    build_exec._log_build_decision(
        action="skip",
        reason="up_to_date",
        settings=settings,
        selected_artifacts=1,
    )

    assert captured
    message, level, message_kind = captured[0]
    assert message.startswith("Build decision:")
    assert '"action": "skip"' in message
    assert '"reason": "up_to_date"' in message
    assert '"mode": "AUTO"' in message
    assert '"profile": "metadata"' in message
    assert '"selected_artifacts": 1' in message
    assert level == 20
    assert message_kind == "build_decision"


def test_run_artifact_builder_emits_materialized_message(monkeypatch):
    captured: list[str] = []

    runtime = SimpleNamespace(artifacts_root=Path("/tmp/artifacts"))
    definition = SimpleNamespace(key="vector_schema")
    task = _TaskStub(id="schema", output="schema.json")
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.execute_operation",
        lambda *, persist, **kwargs: persist(
            ArtifactOutput(
                relative_path="schema.json",
                meta={"features": 5, "targets": 0},
            )
        ),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.persist_artifact_output",
        lambda result, *, artifact_key, runtime, logger: (
            captured.append(f"Materialized {artifact_key}: {result.relative_path}") or {
                "relative_path": result.relative_path,
                **dict(result.meta),
            }
        ),
    )

    result = build_exec._run_artifact_builder(
        runtime=runtime,
        definition=definition,
        task=task,
    )

    assert result is not None
    assert result["relative_path"] == "schema.json"
    assert captured
    assert captured[0].startswith("Materialized vector_schema: ")


def test_run_build_if_needed_forwards_cli_log_outputs(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    def _fake_resolve_build_settings(
        
        project_path=None,
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        runtime_build_mode=None,
        base_log_level=None,
        build_profile=None,
    ):
        captured["cli_log_level"] = cli_log_level
        captured["cli_visuals"] = cli_visuals
        captured["cli_log_outputs"] = cli_log_outputs
        captured["force_flag"] = force_flag
        captured["runtime_build_mode"] = runtime_build_mode
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
        "datapipeline.artifacts.executor.resolve_build_settings",
        _fake_resolve_build_settings,
    )

    build_exec.run_build_if_needed(
        tmp_path / "project.yaml",
        force=False,
        cli_visuals="off",
        cli_log_outputs=[LogOutputTarget(
            transport="fs",
            destination=tmp_path / "logs" / "jerry.log",
        )],
    )

    assert captured["cli_log_outputs"] == [LogOutputTarget(
        transport="fs",
        destination=tmp_path / "logs" / "jerry.log",
    )]
    assert captured["cli_log_level"] is None
    assert captured["build_profile"] is None
    assert captured["runtime_build_mode"] is None


def test_resolve_build_settings_uses_builtin_observability_defaults(tmp_path):
    settings = resolve_build_settings(
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="WARNING",
    )

    assert settings.visuals == "on"
    assert settings.log_decision.name == "WARNING"
    assert settings.log_output.outputs[0].transport == "stderr"
    assert settings.log_output.outputs[0].destination is None
    assert settings.profile_name is None


def test_resolve_build_settings_cli_overrides_defaults(tmp_path):
    cli_target = LogOutputTarget(transport="stdout")

    settings = resolve_build_settings(
        cli_log_level="ERROR",
        cli_visuals="off",
        cli_log_outputs=[cli_target],
        force_flag=False,
        base_log_level="INFO",
    )

    assert settings.visuals == "off"
    assert settings.log_decision.name == "ERROR"
    assert settings.log_output.outputs[0].transport == "stdout"


def test_resolve_build_settings_runtime_mode_override(tmp_path):
    settings = resolve_build_settings(
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        runtime_build_mode="OFF",
        base_log_level="INFO",
    )

    assert settings.mode == "OFF"
    assert settings.force is False


def test_resolve_build_settings_prefers_profile_over_defaults(tmp_path):
    profile = BuildProfile.model_validate(
        {
            "cmd": "build",
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
    profile = BuildProfile.model_validate(
        {
            "cmd": "build",
            "name": "nightly",
            "target": "schema",
        }
    )

    with pytest.raises(ValueError, match="project_path is required"):
        resolve_build_settings(
            cli_log_level=None,
            cli_visuals=None,
            cli_log_outputs=None,
            force_flag=False,
            base_log_level="INFO",
            build_profile=profile,
        )


def test_resolve_build_settings_profile_log_paths_are_project_relative(tmp_path):
    project_root = tmp_path / "datasets" / "demo"
    project_root.mkdir(parents=True, exist_ok=True)
    project_path = project_root / "project.yaml"

    profile = BuildProfile.model_validate(
        {
            "cmd": "build",
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
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="INFO",
        build_profile=profile,
    )
    assert settings.log_output.outputs[0].destination == (project_root / "logs" / "nightly.log").resolve()


def test_resolve_build_settings_keeps_execution_scoped_profile_outputs_for_later_materialization(tmp_path):
    profile = BuildProfile.model_validate(
        {
            "cmd": "build",
            "name": "nightly",
            "target": "schema",
            "observability": {
                "logging": {
                    "outputs": [{"transport": "fs", "scope": "execution"}],
                },
            },
        }
    )

    settings = resolve_build_settings(
        project_path=tmp_path / "project.yaml",
        cli_log_level=None,
        cli_visuals=None,
        cli_log_outputs=None,
        force_flag=False,
        base_log_level="INFO",
        build_profile=profile,
    )
    assert settings.log_output.outputs[0].scope == "execution"


def test_run_build_if_needed_preserves_previous_artifacts_in_state(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
            ]
        ),
        encoding="utf-8",
    )
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="AUTO",
            force=False,
            profile_name="schema",
        ),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1")
    monkeypatch.setattr("datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None)
    monkeypatch.setattr("datapipeline.artifacts.executor.bootstrap", lambda _: SimpleNamespace(artifacts_root=tmp_path / "artifacts"))
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=True),
    )
    schema_task = SchemaTask(id="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    scaler_task = ScalerTask(id="scaler")
    scaler_task.source_path = tasks_root / "scaler.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([schema_task, scaler_task], []),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, definition, **kwargs: {"relative_path": f"{definition.task_id}.json"},
    )

    schema_profile = BuildProfile.model_validate({"cmd": "build", "name": "schema", "target": "schema"})
    scaler_profile = BuildProfile.model_validate({"cmd": "build", "name": "scaler", "target": "scaler"})

    from datapipeline.build.state import load_build_state
    from datapipeline.services.bootstrap import build_state_path

    build_exec.run_build_if_needed(project_path, build_profile=schema_profile)
    build_exec.run_build_if_needed(project_path, build_profile=scaler_profile)

    state = load_build_state(build_state_path(project_path))
    assert state is not None
    assert set(state.artifacts.keys()) == {"vector_schema", "scaler_statistics"}


def test_run_build_if_needed_rebuilds_stale_profile_artifact(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
            ]
        ),
        encoding="utf-8",
    )
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    from datapipeline.build.state import BuildState, save_build_state
    from datapipeline.services.bootstrap import build_state_path

    state_path = build_state_path(project_path)
    stale_state = BuildState(config_hash="hash-2")
    stale_state.register(
        "scaler_statistics",
        "build/scaler.json",
        meta={"_config_hash": "hash-1"},
    )
    save_build_state(stale_state, state_path)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="AUTO",
            force=False,
            profile_name="scaler",
        ),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-2")
    monkeypatch.setattr("datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None)
    monkeypatch.setattr("datapipeline.artifacts.executor.bootstrap", lambda _: SimpleNamespace(artifacts_root=tmp_path / "artifacts"))
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=True),
    )
    captured: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, message_kind)
        ),
    )

    calls = {"build_artifact": 0}
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, definition, **kwargs: (
            calls.__setitem__("build_artifact", calls["build_artifact"] + 1)
            or {"relative_path": f"{definition.task_id}.json"}
        ),
    )
    scaler_task = ScalerTask(id="scaler")
    scaler_task.source_path = tasks_root / "scaler.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([scaler_task], []),
    )
    scaler_profile = BuildProfile.model_validate({"cmd": "build", "name": "scaler", "target": "scaler"})

    did_build = build_exec.run_build_if_needed(project_path, build_profile=scaler_profile)
    assert did_build is True
    assert calls["build_artifact"] == 1
    decisions = [message for message, kind in captured if kind == "build_decision"]
    assert any('"action": "run"' in message and '"reason": "stale"' in message for message in decisions)


def test_run_build_if_needed_emits_missing_reason_when_artifact_not_built(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
            ]
        ),
        encoding="utf-8",
    )
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="AUTO",
            force=False,
            profile_name="schema",
        ),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1")
    monkeypatch.setattr("datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.bootstrap",
        lambda _: SimpleNamespace(artifacts_root=tmp_path / "artifacts"),
    )
    schema_task = SchemaTask(id="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([schema_task], []),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, definition, **kwargs: {"relative_path": f"{definition.task_id}.json"},
    )

    captured: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, message_kind)
        ),
    )

    schema_profile = BuildProfile.model_validate({"cmd": "build", "name": "schema", "target": "schema"})
    did_build = build_exec.run_build_if_needed(project_path, build_profile=schema_profile)

    assert did_build is True
    decisions = [message for message, kind in captured if kind == "build_decision"]
    assert any('"action": "run"' in message and '"reason": "missing"' in message for message in decisions)


def test_run_build_if_needed_hydrates_runtime_from_state_before_jobs(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
            ]
        ),
        encoding="utf-8",
    )
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    from datapipeline.build.state import BuildState, save_build_state
    from datapipeline.services.bootstrap import build_state_path

    state_path = build_state_path(project_path)
    state = BuildState(config_hash="hash-1")
    state.register(
        VECTOR_SCHEMA_METADATA,
        "build/metadata.json",
        meta={"_config_hash": "hash-1"},
    )
    save_build_state(state, state_path)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="AUTO",
            force=False,
            profile_name="stats",
        ),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1")
    monkeypatch.setattr("datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None)

    runtime = SimpleNamespace(
        artifacts_root=tmp_path / "artifacts",
        artifacts=ArtifactManager(tmp_path / "artifacts"),
    )

    def _fake_builder(*, runtime, **kwargs):
        assert runtime.artifacts.has(VECTOR_SCHEMA_METADATA)
        return {"relative_path": "build/stats.json"}

    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        _fake_builder,
    )

    stats_task = StatsTask(id="stats", mode="final")
    did_build = build_exec.run_build_if_needed(
        project_path,
        required_artifacts={VECTOR_STATS},
        artifact_task_configs=[stats_task],
        runtime_override=runtime,
    )

    assert did_build is True


def test_run_build_if_needed_emits_run_decision(monkeypatch, tmp_path):
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
            ]
        ),
        encoding="utf-8",
    )
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
            mode="FORCE",
            force=True,
            profile_name="schema",
        ),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root)
    monkeypatch.setattr("datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1")
    monkeypatch.setattr("datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.bootstrap",
        lambda _: SimpleNamespace(artifacts_root=tmp_path / "artifacts"),
    )
    schema_task = SchemaTask(id="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([schema_task], []),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, definition, **kwargs: {"relative_path": f"{definition.task_id}.json"},
    )

    captured: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, message_kind)
        ),
    )

    schema_profile = BuildProfile.model_validate({"cmd": "build", "name": "schema", "target": "schema"})
    did_build = build_exec.run_build_if_needed(project_path, build_profile=schema_profile)

    assert did_build is True
    decisions = [message for message, kind in captured if kind == "build_decision"]
    assert any('"action": "run"' in message and '"reason": "force"' in message for message in decisions)


def test_plan_build_skips_scaler_when_dataset_has_no_scaled_features(
    monkeypatch,
    tmp_path,
) -> None:
    project_path = tmp_path / "project.yaml"
    project_path.write_text("version: 1\n", encoding="utf-8")
    settings = SimpleNamespace(
        mode="AUTO",
        force=True,
        profile_name="scaler",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        settings=settings,
        build_profile=None,
        required_artifacts={SCALER_STATISTICS},
        artifact_task_configs=[ScalerTask(id="scaler")],
    )

    assert plan == {
        "action": "skip",
        "reason": "not_required",
        "selected_artifacts": 0,
    }
