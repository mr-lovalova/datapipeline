from pathlib import Path
from types import SimpleNamespace

import pytest
from datapipeline.artifacts import executor as build_exec
from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.build.state import BuildState, load_build_state
from datapipeline.config.build_resolution import resolve_build_settings
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    SchemaTask,
    ScalerTask,
    StatsTask,
    TicksTask,
    VectorInputsTask,
)
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
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


def _fake_built_artifact(runtime, task) -> ArtifactOutput:
    relative_path = task.output
    destination = runtime.artifacts_root / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("{}", encoding="utf-8")
    return ArtifactOutput(relative_path=relative_path)


def _runtime_stub(artifacts_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        artifacts_root=artifacts_root,
        artifacts=ArtifactManager(artifacts_root),
        execution_observer=None,
    )


def _write_build_project(tmp_path: Path) -> Path:
    path = tmp_path / "project.yaml"
    path.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  ingests: ./ingests",
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
    return path


def test_load_build_state_invalidates_previous_cache_version(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        '{"version": 1, "config_hash": "old", "artifacts": {}}',
        encoding="utf-8",
    )

    assert load_build_state(state_path) is None


def test_log_build_decision_emits_build_decision(monkeypatch):
    captured: list[str] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_build_decision",
        lambda message, logger, depth=0: captured.append(message),
    )

    settings = SimpleNamespace(mode="AUTO", profile_name="metadata")
    build_exec._log_build_decision(
        build_exec.SkippedBuild(
            reason="up_to_date",
            artifacts=("vector_inputs", "vector_schema"),
        ),
        settings=settings,
    )

    assert captured
    message = captured[0]
    assert message.startswith("Build decision:")
    assert '"action": "skip"' in message
    assert '"reason": "up_to_date"' in message
    assert '"mode": "AUTO"' in message
    assert '"profile": "metadata"' in message
    assert '"selected_artifacts": 2' in message
    assert '"expanded_artifacts": [' in message
    assert '"vector_inputs"' in message
    assert '"jobs": []' in message
    assert '"skipped_current": [' in message


def test_run_artifact_builder_emits_materialized_message(monkeypatch):
    captured: list[str] = []

    runtime = _runtime_stub(Path("/tmp/artifacts"))
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
        lambda result, *, artifact_key, expected_relative_path, runtime, logger: (
            captured.append(f"Materialized {artifact_key}: {result.relative_path}")
            or ArtifactOutput(
                relative_path=result.relative_path,
                meta=dict(result.meta),
            )
        ),
    )

    result = build_exec._run_artifact_builder(
        runtime=runtime,
        definition=definition,
        task=task,
    )

    assert result is not None
    assert result.relative_path == "schema.json"
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
        cli_heartbeat_interval_seconds=None,
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
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            mode="OFF",
            force=False,
            profile_name=None,
            heartbeat_interval_seconds=None,
        )

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        _fake_resolve_build_settings,
    )

    build_exec.run_build_if_needed(
        tmp_path / "project.yaml",
        force=False,
        cli_visuals="off",
        cli_log_outputs=[
            LogOutputTarget(
                transport="fs",
                destination=tmp_path / "logs" / "jerry.log",
            )
        ],
    )

    assert captured["cli_log_outputs"] == [
        LogOutputTarget(
            transport="fs",
            destination=tmp_path / "logs" / "jerry.log",
        )
    ]
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
    assert settings.heartbeat_interval_seconds is None


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
                "heartbeat_interval_seconds": 15,
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
    assert settings.heartbeat_interval_seconds == 15
    assert settings.log_decision.name == "DEBUG"
    assert settings.log_output.outputs[0].transport == "fs"
    assert (
        settings.log_output.outputs[0].destination
        == (tmp_path / "logs" / "nightly.log").resolve()
    )


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
    assert (
        settings.log_output.outputs[0].destination
        == (project_root / "logs" / "nightly.log").resolve()
    )


def test_resolve_build_settings_keeps_execution_scoped_profile_outputs_for_later_materialization(
    tmp_path,
):
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


def test_run_build_if_needed_preserves_previous_artifacts_in_state(
    monkeypatch, tmp_path
):
    project_path = _write_build_project(tmp_path)
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            mode="AUTO",
            force=False,
            profile_name="schema",
            heartbeat_interval_seconds=None,
        ),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.bootstrap_build_runtime",
        lambda _: _runtime_stub(tmp_path / "artifacts"),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=True),
    )
    schema_task = SchemaTask(id="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    scaler_task = ScalerTask(id="scaler")
    scaler_task.source_path = tasks_root / "scaler.yaml"
    vector_inputs_task = VectorInputsTask(id="vector_inputs")
    vector_inputs_task.source_path = tasks_root / "vector_inputs.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([schema_task, scaler_task, vector_inputs_task], []),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, runtime, task, **kwargs: _fake_built_artifact(
            runtime,
            task,
        ),
    )

    schema_profile = BuildProfile.model_validate(
        {"cmd": "build", "name": "schema", "target": "schema"}
    )
    scaler_profile = BuildProfile.model_validate(
        {"cmd": "build", "name": "scaler", "target": "scaler"}
    )

    from datapipeline.services.bootstrap import build_state_path

    build_exec.run_build_if_needed(project_path, build_profile=schema_profile)
    build_exec.run_build_if_needed(project_path, build_profile=scaler_profile)

    state = load_build_state(build_state_path(project_path))
    assert state is not None
    assert set(state.artifacts.keys()) == {
        "scaler_statistics",
        "vector_inputs",
        "vector_schema",
    }


def test_run_build_if_needed_rejects_inputs_changed_during_build(
    monkeypatch,
    tmp_path,
) -> None:
    project_path = _write_build_project(tmp_path)
    plan = SimpleNamespace(config_hash="before")
    settings = SimpleNamespace(heartbeat_interval_seconds=None)

    monkeypatch.setattr(build_exec, "_plan_build", lambda **_kwargs: plan)
    monkeypatch.setattr(
        build_exec, "_log_build_decision", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(build_exec, "bootstrap_build_runtime", lambda _path: object())
    monkeypatch.setattr(build_exec, "_execute_build_jobs", lambda **_kwargs: None)
    monkeypatch.setattr(build_exec, "tasks_dir", lambda _path: tmp_path)
    monkeypatch.setattr(build_exec, "compute_config_hash", lambda *_args: "after")

    with pytest.raises(RuntimeError, match="Build inputs changed"):
        build_exec.run_build_if_needed(
            project_path,
            settings=settings,
            skip_logging_setup=True,
        )


def test_execute_build_jobs_persists_completed_artifact_before_later_failure(
    monkeypatch,
    tmp_path,
):
    state_path = tmp_path / "artifacts" / "build_state.json"
    runtime = _runtime_stub(tmp_path / "artifacts")
    vector_inputs_definition = SimpleNamespace(
        key=VECTOR_INPUTS,
        task_id="vector_inputs",
    )
    schema_definition = SimpleNamespace(
        key=VECTOR_SCHEMA,
        task_id="schema",
    )
    vector_inputs_task = _TaskStub(
        id="vector_inputs",
        output="build/vector_inputs/manifest.json",
        entrypoint="core.artifact.vector_inputs",
    )
    schema_task = _TaskStub(id="schema", output="build/schema.json")
    previous_state = BuildState(config_hash="hash-1")
    previous_state.register(
        VECTOR_INPUTS,
        "build/old-vector-inputs.json",
        meta={"_config_hash": "hash-1"},
    )
    previous_state.register(
        VECTOR_SCHEMA,
        "build/old-schema.json",
        meta={"_config_hash": "hash-1"},
    )

    def _fake_builder(*, definition, **_kwargs):
        if definition.key == VECTOR_SCHEMA:
            raise RuntimeError("schema failed")
        return ArtifactOutput(relative_path="build/vector_inputs/manifest.json")

    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        _fake_builder,
    )
    graph = build_artifact_graph(
        [
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
        ]
    )
    plan = build_exec.BuildPlan(
        reason="stale",
        artifacts=(VECTOR_INPUTS, VECTOR_SCHEMA),
        jobs=(
            build_exec.ArtifactBuildJob(
                vector_inputs_definition,
                vector_inputs_task,
                (VECTOR_INPUTS, VECTOR_SCHEMA),
            ),
            build_exec.ArtifactBuildJob(
                schema_definition,
                schema_task,
                (VECTOR_SCHEMA,),
            ),
        ),
        skipped_current=(),
        config_hash="hash-1",
        state_path=state_path,
        previous_state=previous_state,
        graph=graph,
    )

    with pytest.raises(RuntimeError, match="schema failed"):
        build_exec._execute_build_jobs(
            runtime=runtime,
            plan=plan,
        )

    state = load_build_state(state_path)
    assert state is not None
    assert list(state.artifacts) == [VECTOR_INPUTS]
    assert state.artifacts[VECTOR_INPUTS].relative_path == (
        "build/vector_inputs/manifest.json"
    )
    assert VECTOR_SCHEMA not in state.artifacts


def test_execute_build_job_invalidates_only_its_graph_descendants(
    monkeypatch,
    tmp_path,
) -> None:
    state_path = tmp_path / "artifacts/_system/build/state.json"
    runtime = _runtime_stub(tmp_path / "artifacts")
    previous_state = BuildState(config_hash="hash-1")
    artifact_paths = {
        SCALER_STATISTICS: "build/scaler.json",
        VECTOR_INPUTS: "build/vector_inputs.json",
        VECTOR_SCHEMA: "build/schema.json",
        VECTOR_SCHEMA_METADATA: "build/metadata.json",
        VECTOR_STATS: "build/stats.json",
        "custom_snapshot": "build/custom.json",
    }
    for key, relative_path in artifact_paths.items():
        previous_state.register(
            key,
            relative_path,
            meta={"_config_hash": "hash-1"},
        )
        runtime.artifacts.register(key, relative_path)
        destination = runtime.artifacts_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("{}", encoding="utf-8")

    definition = SimpleNamespace(key=SCALER_STATISTICS, task_id="scaler")
    task = ScalerTask(id="scaler")

    def _fake_builder(*, runtime, **_kwargs):
        output = runtime.artifacts_root / task.output
        output.write_text("{}", encoding="utf-8")
        runtime.artifacts.register(SCALER_STATISTICS, task.output)
        return ArtifactOutput(relative_path=task.output)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        _fake_builder,
    )

    custom_task = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph(
        [
            ScalerTask(id="scaler"),
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
            MetadataTask(id="metadata"),
            StatsTask(id="stats", mode="final"),
            custom_task,
        ]
    )
    plan = build_exec.BuildPlan(
        reason="force",
        artifacts=(SCALER_STATISTICS,),
        jobs=(
            build_exec.ArtifactBuildJob(
                definition,
                task,
                (
                    SCALER_STATISTICS,
                    VECTOR_INPUTS,
                    VECTOR_SCHEMA,
                    VECTOR_SCHEMA_METADATA,
                    VECTOR_STATS,
                ),
            ),
        ),
        skipped_current=(),
        config_hash="hash-1",
        state_path=state_path,
        previous_state=previous_state,
        graph=graph,
    )

    state = build_exec._execute_build_jobs(
        runtime=runtime,
        plan=plan,
    )

    assert set(state.artifacts) == {SCALER_STATISTICS, "custom_snapshot"}
    assert runtime.artifacts.has(SCALER_STATISTICS)
    assert not runtime.artifacts.has(VECTOR_INPUTS)
    assert not runtime.artifacts.has(VECTOR_SCHEMA)
    assert not runtime.artifacts.has(VECTOR_SCHEMA_METADATA)
    assert not runtime.artifacts.has(VECTOR_STATS)


def test_run_build_if_needed_rebuilds_stale_profile_artifact(monkeypatch, tmp_path):
    project_path = _write_build_project(tmp_path)
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
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            mode="AUTO",
            force=False,
            profile_name="scaler",
            heartbeat_interval_seconds=None,
        ),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-2"
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.bootstrap_build_runtime",
        lambda _: _runtime_stub(tmp_path / "artifacts"),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=True),
    )
    captured: list[str] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_build_decision",
        lambda message, logger, depth=0: captured.append(message),
    )

    calls = {"build_artifact": 0}
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, runtime, task, **kwargs: (
            calls.__setitem__("build_artifact", calls["build_artifact"] + 1)
            or _fake_built_artifact(runtime, task)
        ),
    )
    scaler_task = ScalerTask(id="scaler")
    scaler_task.source_path = tasks_root / "scaler.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([scaler_task], []),
    )
    scaler_profile = BuildProfile.model_validate(
        {"cmd": "build", "name": "scaler", "target": "scaler"}
    )

    did_build = build_exec.run_build_if_needed(
        project_path, build_profile=scaler_profile
    )
    assert did_build is True
    assert calls["build_artifact"] == 1
    assert any(
        '"action": "run"' in message and '"reason": "stale"' in message
        for message in captured
    )


def test_run_build_if_needed_emits_missing_reason_when_artifact_not_built(
    monkeypatch, tmp_path
):
    project_path = _write_build_project(tmp_path)
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            mode="AUTO",
            force=False,
            profile_name="schema",
            heartbeat_interval_seconds=None,
        ),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.bootstrap_build_runtime",
        lambda _: _runtime_stub(tmp_path / "artifacts"),
    )
    schema_task = SchemaTask(id="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    vector_inputs_task = VectorInputsTask(id="vector_inputs")
    vector_inputs_task.source_path = tasks_root / "vector_inputs.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([schema_task, vector_inputs_task], []),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, runtime, task, **kwargs: _fake_built_artifact(
            runtime,
            task,
        ),
    )

    captured: list[str] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_build_decision",
        lambda message, logger, depth=0: captured.append(message),
    )

    schema_profile = BuildProfile.model_validate(
        {"cmd": "build", "name": "schema", "target": "schema"}
    )
    did_build = build_exec.run_build_if_needed(
        project_path, build_profile=schema_profile
    )

    assert did_build is True
    assert any(
        '"action": "run"' in message and '"reason": "missing"' in message
        for message in captured
    )


def test_run_build_if_needed_hydrates_runtime_from_state_before_jobs(
    monkeypatch, tmp_path
):
    project_path = _write_build_project(tmp_path)
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    from datapipeline.build.state import BuildState, save_build_state
    from datapipeline.services.bootstrap import build_state_path

    state_path = build_state_path(project_path)
    state = BuildState(config_hash="hash-1")
    artifacts_root = tmp_path / "artifacts"
    vector_inputs_path = artifacts_root / "build/vector_inputs/manifest.json"
    vector_inputs_path.parent.mkdir(parents=True, exist_ok=True)
    vector_inputs_path.write_text("{}", encoding="utf-8")
    metadata_path = artifacts_root / "build/metadata.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text("{}", encoding="utf-8")
    state.register(
        VECTOR_INPUTS,
        "build/vector_inputs/manifest.json",
        meta={"_config_hash": "hash-1"},
    )
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
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            mode="AUTO",
            force=False,
            profile_name="stats",
            heartbeat_interval_seconds=None,
        ),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )

    runtime = _runtime_stub(tmp_path / "artifacts")

    def _fake_builder(*, runtime, **kwargs):
        assert runtime.artifacts.has(VECTOR_SCHEMA_METADATA)
        stats_path = runtime.artifacts_root / "build/stats.json"
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        stats_path.write_text("{}", encoding="utf-8")
        return ArtifactOutput(relative_path="build/stats.json")

    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        _fake_builder,
    )

    stats_task = StatsTask(id="stats", mode="raw")
    metadata_task = MetadataTask(id="metadata")
    vector_inputs_task = VectorInputsTask(id="vector_inputs")
    did_build = build_exec.run_build_if_needed(
        project_path,
        required_artifacts={VECTOR_STATS},
        artifact_task_configs=[stats_task, metadata_task, vector_inputs_task],
        runtime_override=runtime,
    )

    assert did_build is True


def test_run_build_if_needed_emits_run_decision(monkeypatch, tmp_path):
    project_path = _write_build_project(tmp_path)
    tasks_root = tmp_path / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.artifacts.executor.resolve_build_settings",
        lambda **kwargs: SimpleNamespace(
            visuals="off",
            log_decision=SimpleNamespace(name="INFO", value=20),
            log_output=LogOutputSettings(
                outputs=(LogOutputTarget(transport="stderr"),)
            ),
            mode="FORCE",
            force=True,
            profile_name="schema",
            heartbeat_interval_seconds=None,
        ),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.tasks_dir", lambda _: tasks_root
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.configure_root_logging", lambda **kwargs: None
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.bootstrap_build_runtime",
        lambda _: _runtime_stub(tmp_path / "artifacts"),
    )
    schema_task = SchemaTask(id="schema")
    schema_task.source_path = tasks_root / "schema.yaml"
    vector_inputs_task = VectorInputsTask(id="vector_inputs")
    vector_inputs_task.source_path = tasks_root / "vector_inputs.yaml"
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.operation_specs",
        lambda _: ([schema_task, vector_inputs_task], []),
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor._run_artifact_builder",
        lambda *, runtime, task, **kwargs: _fake_built_artifact(
            runtime,
            task,
        ),
    )

    captured: list[str] = []
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.emit_build_decision",
        lambda message, logger, depth=0: captured.append(message),
    )

    schema_profile = BuildProfile.model_validate(
        {"cmd": "build", "name": "schema", "target": "schema"}
    )
    did_build = build_exec.run_build_if_needed(
        project_path, build_profile=schema_profile
    )

    assert did_build is True
    assert any(
        '"action": "run"' in message and '"reason": "force"' in message
        for message in captured
    )


def test_plan_build_skips_scaler_when_dataset_has_no_scaled_features(
    monkeypatch,
    tmp_path,
) -> None:
    project_path = _write_build_project(tmp_path)
    settings = SimpleNamespace(
        mode="AUTO",
        force=True,
        profile_name="scaler",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tmp_path)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        settings=settings,
        build_profile=None,
        required_artifacts={SCALER_STATISTICS},
        artifact_task_configs=[
            ScalerTask(id="scaler"),
            TicksTask(
                id="dataset_ticks",
                stream="reference.stream",
                output="build/dataset_ticks.jsonl",
            ),
        ],
    )

    assert plan == build_exec.SkippedBuild(
        reason="not_required",
        artifacts=(),
    )


def test_plan_build_supports_generic_artifact_task(monkeypatch, tmp_path) -> None:
    project_path = _write_build_project(tmp_path)
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tmp_path)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash",
        lambda *_: "hash-1",
    )
    task = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: pytest.fail(
            "unrelated dataset configuration was loaded"
        ),
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        settings=SimpleNamespace(mode="FORCE", force=True, profile_name="custom"),
        build_profile=BuildProfile.model_validate(
            {"cmd": "build", "name": "custom", "target": "custom_snapshot"}
        ),
        required_artifacts=None,
        artifact_task_configs=[task, ScalerTask(id="scaler")],
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert [(job.definition.key, job.task.id) for job in plan.jobs] == [
        ("custom_snapshot", "custom_snapshot")
    ]


def test_plan_build_schema_runs_vector_inputs_dependency(monkeypatch, tmp_path) -> None:
    project_path = _write_build_project(tmp_path)
    settings = SimpleNamespace(
        mode="FORCE",
        force=True,
        profile_name="schema",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tmp_path)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        settings=settings,
        build_profile=BuildProfile.model_validate(
            {"cmd": "build", "name": "schema", "target": "schema"}
        ),
        required_artifacts=None,
        artifact_task_configs=[
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
            ScalerTask(id="scaler"),
        ],
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert plan.artifacts == (VECTOR_INPUTS, VECTOR_SCHEMA)
    assert tuple(job.definition.key for job in plan.jobs) == (
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
    )
    assert plan.skipped_current == ()
    assert [(job.definition.key, job.task.id) for job in plan.jobs] == [
        (VECTOR_INPUTS, "vector_inputs"),
        (VECTOR_SCHEMA, "schema"),
    ]


def test_plan_build_does_not_rebuild_current_dependency(monkeypatch, tmp_path) -> None:
    project_path = _write_build_project(tmp_path)

    from datapipeline.build.state import BuildState, save_build_state
    from datapipeline.services.bootstrap import build_state_path

    state = BuildState(config_hash="hash-1")
    state.register(
        VECTOR_INPUTS,
        "build/vector_inputs/manifest.json",
        meta={"_config_hash": "hash-1"},
    )
    save_build_state(state, build_state_path(project_path))
    manifest_path = tmp_path / "artifacts/build/vector_inputs/manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{}", encoding="utf-8")

    settings = SimpleNamespace(
        mode="AUTO",
        force=False,
        profile_name="schema",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tmp_path)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash", lambda *_: "hash-1"
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        settings=settings,
        build_profile=BuildProfile.model_validate(
            {"cmd": "build", "name": "schema", "target": "schema"}
        ),
        required_artifacts=None,
        artifact_task_configs=[
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
        ],
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert plan.artifacts == (VECTOR_INPUTS, VECTOR_SCHEMA)
    assert tuple(job.definition.key for job in plan.jobs) == (VECTOR_SCHEMA,)
    assert plan.skipped_current == (VECTOR_INPUTS,)
    assert [(job.definition.key, job.task.id) for job in plan.jobs] == [
        (VECTOR_SCHEMA, "schema"),
    ]


def test_plan_build_rejects_missing_producer_even_when_cache_is_current(
    monkeypatch,
    tmp_path,
) -> None:
    project_path = _write_build_project(tmp_path)
    artifacts_root = tmp_path / "artifacts"
    input_path = artifacts_root / "build/vector_inputs/manifest.json"
    schema_path = artifacts_root / "build/schema.json"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    input_path.write_text("{}", encoding="utf-8")
    schema_path.write_text("{}", encoding="utf-8")

    from datapipeline.build.state import save_build_state
    from datapipeline.services.bootstrap import build_state_path

    state = BuildState(config_hash="hash-1")
    state.register(
        VECTOR_INPUTS,
        "build/vector_inputs/manifest.json",
        meta={"_config_hash": "hash-1"},
    )
    state.register(
        VECTOR_SCHEMA,
        "build/schema.json",
        meta={"_config_hash": "hash-1"},
    )
    save_build_state(state, build_state_path(project_path))

    settings = SimpleNamespace(mode="AUTO", force=False, profile_name="schema")
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )

    with pytest.raises(SystemExit) as exc:
        build_exec._plan_build(
            project_path=project_path,
            settings=settings,
            build_profile=BuildProfile.model_validate(
                {"cmd": "build", "name": "schema", "target": "schema"}
            ),
            required_artifacts=None,
            artifact_task_configs=[SchemaTask(id="schema")],
        )

    assert exc.value.code == 2


def test_plan_build_rebuilds_current_dependent_when_dependency_is_stale(
    monkeypatch,
    tmp_path,
) -> None:
    project_path = _write_build_project(tmp_path)
    artifacts_root = tmp_path / "artifacts"
    input_path = artifacts_root / "build/vector_inputs/manifest.json"
    schema_path = artifacts_root / "build/schema.json"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    input_path.write_text("{}", encoding="utf-8")
    schema_path.write_text("{}", encoding="utf-8")

    from datapipeline.build.state import save_build_state
    from datapipeline.services.bootstrap import build_state_path

    state = BuildState(config_hash="hash-2")
    state.register(
        VECTOR_INPUTS,
        "build/vector_inputs/manifest.json",
        meta={"_config_hash": "hash-1"},
    )
    state.register(
        VECTOR_SCHEMA,
        "build/schema.json",
        meta={"_config_hash": "hash-2"},
    )
    save_build_state(state, build_state_path(project_path))

    settings = SimpleNamespace(mode="AUTO", force=False, profile_name="schema")
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.load_dataset",
        lambda *_args, **_kwargs: _dataset_with_feature(scale=False),
    )
    monkeypatch.setattr("datapipeline.artifacts.executor.tasks_dir", lambda _: tmp_path)
    monkeypatch.setattr(
        "datapipeline.artifacts.executor.compute_config_hash",
        lambda *_: "hash-2",
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        settings=settings,
        build_profile=BuildProfile.model_validate(
            {"cmd": "build", "name": "schema", "target": "schema"}
        ),
        required_artifacts=None,
        artifact_task_configs=[
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
        ],
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert tuple(job.definition.key for job in plan.jobs) == (
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
    )
    assert plan.jobs[0].invalidated_artifacts == (
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
        VECTOR_SCHEMA_METADATA,
        VECTOR_STATS,
    )
