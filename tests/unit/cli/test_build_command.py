from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.artifacts import executor as build_exec
from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.build.state import BuildState, load_build_state, save_build_state
from datapipeline.config.build_resolution import resolve_build_settings
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    ScalerTask,
    SchemaTask,
    StatsTask,
    VectorInputsTask,
)
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.bootstrap import build_state_path
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
    VECTOR_STATS,
)


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
    )


def _runtime(artifacts_root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        artifacts_root=artifacts_root,
        artifacts=ArtifactManager(artifacts_root),
        execution_observer=None,
        heartbeat_interval_seconds=None,
    )


def _write_project(tmp_path: Path) -> Path:
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
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
    return project_path


def _write_artifact(root: Path, task: ArtifactTask) -> None:
    destination = root / task.output
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("{}", encoding="utf-8")


def _build_artifact(runtime, task: ArtifactTask) -> ArtifactOutput:
    _write_artifact(runtime.artifacts_root, task)
    return ArtifactOutput(relative_path=task.output)


def _patch_hash(monkeypatch, value: str) -> None:
    monkeypatch.setattr(build_exec, "tasks_dir", lambda _project: Path("tasks"))
    monkeypatch.setattr(build_exec, "compute_config_hash", lambda *_args: value)


def test_load_build_state_invalidates_previous_cache_version(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        '{"version": 2, "config_hash": "old", "artifacts": {}}',
        encoding="utf-8",
    )

    assert load_build_state(state_path) is None


def test_log_build_decision_describes_explicit_mode_and_profile(monkeypatch) -> None:
    captured: list[str] = []
    monkeypatch.setattr(
        build_exec,
        "emit_build_decision",
        lambda message, logger: captured.append(message),
    )

    build_exec._log_build_decision(
        build_exec.SkippedBuild(
            reason="up_to_date",
            artifacts=(VECTOR_INPUTS, VECTOR_SCHEMA),
        ),
        mode="AUTO",
        profile_name="schema",
    )

    assert len(captured) == 1
    assert '"action": "skip"' in captured[0]
    assert '"reason": "up_to_date"' in captured[0]
    assert '"mode": "AUTO"' in captured[0]
    assert '"profile": "schema"' in captured[0]
    assert '"jobs": []' in captured[0]


def test_run_artifact_builder_persists_under_task_id(monkeypatch, tmp_path) -> None:
    task = SchemaTask(id="schema")
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        build_exec,
        "execute_operation",
        lambda *, persist, **_kwargs: persist(
            ArtifactOutput(relative_path=task.output, meta={"features": 5})
        ),
    )

    def persist(result, *, artifact_key, expected_relative_path, **_kwargs):
        captured["key"] = artifact_key
        captured["path"] = expected_relative_path
        return result

    monkeypatch.setattr(build_exec, "persist_artifact_output", persist)

    result = build_exec._run_artifact_builder(_runtime(tmp_path), task)

    assert result is not None
    assert captured == {"key": "schema", "path": task.output}


def test_resolve_build_settings_uses_builtin_observability_defaults() -> None:
    settings = resolve_build_settings(base_log_level="WARNING")

    assert settings.visuals == "on"
    assert settings.log_decision.name == "WARNING"
    assert settings.log_output.outputs[0].transport == "stderr"
    assert settings.mode == "AUTO"
    assert settings.profile_name is None


def test_resolve_build_settings_applies_cli_overrides() -> None:
    target = LogOutputTarget(transport="stdout")

    settings = resolve_build_settings(
        cli_log_level="ERROR",
        cli_visuals="off",
        cli_log_outputs=[target],
        force_flag=True,
        base_log_level="INFO",
    )

    assert settings.visuals == "off"
    assert settings.log_decision.name == "ERROR"
    assert settings.log_output.outputs == (target,)
    assert settings.mode == "FORCE"


def test_resolve_build_settings_uses_profile_configuration(tmp_path: Path) -> None:
    project_path = tmp_path / "project.yaml"
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
                    "outputs": [{"transport": "fs", "path": "./logs/build.log"}],
                },
            },
        }
    )

    settings = resolve_build_settings(
        project_path=project_path,
        base_log_level="WARNING",
        build_profile=profile,
    )

    assert settings.profile_name == "nightly"
    assert settings.mode == "FORCE"
    assert settings.visuals == "on"
    assert settings.heartbeat_interval_seconds == 15
    assert settings.log_decision.name == "DEBUG"
    assert (
        settings.log_output.outputs[0].destination
        == (tmp_path / "logs/build.log").resolve()
    )


def test_resolve_build_settings_requires_project_for_profile() -> None:
    profile = BuildProfile.model_validate(
        {"cmd": "build", "name": "nightly", "target": "schema"}
    )

    with pytest.raises(ValueError, match="project_path is required"):
        resolve_build_settings(build_profile=profile)


def test_plan_skips_scaler_when_dataset_has_no_scaled_features(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = _write_project(tmp_path)
    graph = build_artifact_graph([ScalerTask(id="scaler")])
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        graph=graph,
        required_artifacts={SCALER_STATISTICS},
        mode="AUTO",
    )

    assert plan == build_exec.SkippedBuild(reason="not_required", artifacts=())


def test_plan_builds_only_requested_generic_artifact(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph([task, ScalerTask(id="scaler")])
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: pytest.fail("unrelated dataset config was loaded"),
    )

    plan = build_exec._plan_build(
        project_path=_write_project(tmp_path),
        graph=graph,
        required_artifacts={task.id},
        mode="FORCE",
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert plan.artifacts == (task.id,)
    assert tuple(job.task.id for job in plan.jobs) == (task.id,)


def test_plan_expands_schema_dependencies(monkeypatch, tmp_path: Path) -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([ScalerTask(id="scaler"), vector_inputs, schema])
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )

    plan = build_exec._plan_build(
        project_path=_write_project(tmp_path),
        graph=graph,
        required_artifacts={VECTOR_SCHEMA},
        mode="FORCE",
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert plan.artifacts == (VECTOR_INPUTS, VECTOR_SCHEMA)
    assert tuple(job.task.id for job in plan.jobs) == (
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
    )


def test_plan_skips_current_dependency(monkeypatch, tmp_path: Path) -> None:
    project_path = _write_project(tmp_path)
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([vector_inputs, schema])
    state = BuildState()
    state.register(
        VECTOR_INPUTS,
        vector_inputs.output,
        config_hash="hash-1",
    )
    _write_artifact(tmp_path / "artifacts", vector_inputs)
    save_build_state(state, build_state_path(project_path))
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        graph=graph,
        required_artifacts={VECTOR_SCHEMA},
        mode="AUTO",
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert tuple(job.task.id for job in plan.jobs) == (VECTOR_SCHEMA,)
    assert plan.skipped_current == (VECTOR_INPUTS,)


def test_plan_rejects_config_drift_before_build(
    monkeypatch,
    tmp_path: Path,
) -> None:
    graph = build_artifact_graph([ScalerTask(id="scaler")])
    _patch_hash(monkeypatch, "hash-2")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: pytest.fail("dataset resolution ran"),
    )

    with pytest.raises(RuntimeError, match="after profiles were resolved"):
        build_exec._plan_build(
            project_path=_write_project(tmp_path),
            graph=graph,
            required_artifacts={SCALER_STATISTICS},
            mode="FORCE",
            expected_config_hash="hash-1",
        )


def test_plan_rejects_resolved_artifact_that_became_stale(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = _write_project(tmp_path)
    first = ArtifactTask(
        id="first_snapshot",
        entrypoint="plugin.first",
        output="build/first.json",
    )
    second = ArtifactTask(
        id="second_snapshot",
        entrypoint="plugin.second",
        output="build/second.json",
    )
    graph = build_artifact_graph([first, second])
    state = BuildState()
    state.register(first.id, first.output, config_hash="hash-1")
    _write_artifact(tmp_path / "artifacts", first)
    save_build_state(state, build_state_path(project_path))
    _patch_hash(monkeypatch, "hash-2")

    with pytest.raises(RuntimeError, match="earlier build profile became stale"):
        build_exec._plan_build(
            project_path=project_path,
            graph=graph,
            required_artifacts={second.id},
            mode="FORCE",
            resolved_artifacts={first.id},
        )


def test_plan_rejects_missing_dependency_producer(
    monkeypatch,
    tmp_path: Path,
) -> None:
    graph = build_artifact_graph([SchemaTask(id="schema")])
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )

    with pytest.raises(SystemExit) as exc:
        build_exec._plan_build(
            project_path=_write_project(tmp_path),
            graph=graph,
            required_artifacts={VECTOR_SCHEMA},
            mode="AUTO",
        )

    assert exc.value.code == 2


def test_stale_dependency_rebuilds_current_dependent(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = _write_project(tmp_path)
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([vector_inputs, schema])
    state = BuildState()
    state.register(
        VECTOR_INPUTS,
        vector_inputs.output,
        config_hash="hash-1",
    )
    state.register(
        VECTOR_SCHEMA,
        schema.output,
        config_hash="hash-2",
    )
    for task in (vector_inputs, schema):
        _write_artifact(tmp_path / "artifacts", task)
    save_build_state(state, build_state_path(project_path))
    _patch_hash(monkeypatch, "hash-2")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )

    plan = build_exec._plan_build(
        project_path=project_path,
        graph=graph,
        required_artifacts={VECTOR_SCHEMA},
        mode="AUTO",
    )

    assert isinstance(plan, build_exec.BuildPlan)
    assert tuple(job.task.id for job in plan.jobs) == (
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
    )
    assert plan.jobs[0].invalidated_artifacts == (
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
        VECTOR_METADATA,
        VECTOR_STATS,
    )


def test_mode_off_rejects_missing_artifact(monkeypatch, tmp_path: Path) -> None:
    task = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/snapshot.json",
    )
    graph = build_artifact_graph([task])
    _patch_hash(monkeypatch, "hash-1")

    with pytest.raises(SystemExit) as exc:
        build_exec._plan_build(
            project_path=_write_project(tmp_path),
            graph=graph,
            required_artifacts={task.id},
            mode="OFF",
        )

    assert exc.value.code == 2


def test_execute_build_jobs_persists_completed_job_before_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([vector_inputs, schema])
    state_path = tmp_path / "artifacts/_system/build/state.json"
    previous_state = BuildState()
    previous_state.register(
        VECTOR_SCHEMA,
        schema.output,
        config_hash="hash-1",
    )

    def build(runtime, task):
        if task.id == VECTOR_SCHEMA:
            raise RuntimeError("schema failed")
        return _build_artifact(runtime, task)

    monkeypatch.setattr(build_exec, "_run_artifact_builder", build)
    plan = build_exec.BuildPlan(
        reason="stale",
        artifacts=(VECTOR_INPUTS, VECTOR_SCHEMA),
        jobs=(
            build_exec.ArtifactBuildJob(
                vector_inputs,
                (VECTOR_INPUTS, VECTOR_SCHEMA),
            ),
            build_exec.ArtifactBuildJob(schema, (VECTOR_SCHEMA,)),
        ),
        skipped_current=(),
        config_hash="hash-1",
        state_path=state_path,
        previous_state=previous_state,
        graph=graph,
    )

    with pytest.raises(RuntimeError, match="schema failed"):
        build_exec._execute_build_jobs(
            runtime=_runtime(tmp_path / "artifacts"), plan=plan
        )

    state = load_build_state(state_path)
    assert state is not None
    assert list(state.artifacts) == [VECTOR_INPUTS]


def test_execute_build_job_invalidates_only_graph_descendants(
    monkeypatch,
    tmp_path: Path,
) -> None:
    scaler = ScalerTask(id="scaler")
    custom = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph(
        [
            scaler,
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
            MetadataTask(id="metadata"),
            StatsTask(id="stats", mode="final"),
            custom,
        ]
    )
    runtime = _runtime(tmp_path / "artifacts")
    previous_state = BuildState()
    for task in graph.tasks_by_id.values():
        previous_state.register(
            task.id,
            task.output,
            config_hash="hash-1",
        )
        _write_artifact(runtime.artifacts_root, task)
    monkeypatch.setattr(build_exec, "_run_artifact_builder", _build_artifact)
    plan = build_exec.BuildPlan(
        reason="force",
        artifacts=(SCALER_STATISTICS,),
        jobs=(
            build_exec.ArtifactBuildJob(
                scaler,
                (
                    SCALER_STATISTICS,
                    VECTOR_INPUTS,
                    VECTOR_SCHEMA,
                    VECTOR_METADATA,
                    VECTOR_STATS,
                ),
            ),
        ),
        skipped_current=(),
        config_hash="hash-1",
        state_path=tmp_path / "artifacts/_system/build/state.json",
        previous_state=previous_state,
        graph=graph,
    )

    state = build_exec._execute_build_jobs(runtime=runtime, plan=plan)

    assert set(state.artifacts) == {SCALER_STATISTICS, custom.id}


def test_run_build_hydrates_current_dependencies_before_job(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = _write_project(tmp_path)
    vector_inputs = VectorInputsTask(id="vector_inputs")
    metadata = MetadataTask(id="metadata")
    stats = StatsTask(id="stats", mode="raw")
    graph = build_artifact_graph([vector_inputs, metadata, stats])
    state = BuildState()
    for task in (vector_inputs, metadata):
        state.register(task.id, task.output, config_hash="hash-1")
        _write_artifact(tmp_path / "artifacts", task)
    save_build_state(state, build_state_path(project_path))
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )
    runtime = _runtime(tmp_path / "artifacts")

    def build(runtime, task):
        assert task is stats
        assert runtime.artifacts.has(VECTOR_METADATA)
        return _build_artifact(runtime, task)

    monkeypatch.setattr(build_exec, "_run_artifact_builder", build)

    did_build = build_exec.run_build_if_needed(
        project_path,
        graph=graph,
        required_artifacts={VECTOR_STATS},
        mode="AUTO",
        runtime=runtime,
    )

    assert did_build is True


def test_force_build_preserves_artifacts_resolved_by_previous_profile(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = _write_project(tmp_path)
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    graph = build_artifact_graph([vector_inputs, schema, metadata])
    state = BuildState()
    for task in (vector_inputs, schema):
        state.register(task.id, task.output, config_hash="hash-1")
        _write_artifact(tmp_path / "artifacts", task)
    save_build_state(state, build_state_path(project_path))
    _patch_hash(monkeypatch, "hash-1")
    monkeypatch.setattr(
        build_exec,
        "load_dataset",
        lambda *_args: _dataset_with_feature(scale=False),
    )
    built: list[str] = []

    def build(runtime, task):
        built.append(task.id)
        return _build_artifact(runtime, task)

    monkeypatch.setattr(build_exec, "_run_artifact_builder", build)
    runtime = _runtime(tmp_path / "artifacts")
    resolved: set[str] = set()

    assert not build_exec.run_build_if_needed(
        project_path,
        graph=graph,
        required_artifacts={VECTOR_SCHEMA},
        mode="AUTO",
        runtime=runtime,
        resolved_artifacts=resolved,
    )
    assert build_exec.run_build_if_needed(
        project_path,
        graph=graph,
        required_artifacts={VECTOR_METADATA},
        mode="FORCE",
        runtime=runtime,
        resolved_artifacts=resolved,
    )

    assert built == [VECTOR_METADATA]
    assert resolved == {VECTOR_INPUTS, VECTOR_SCHEMA, VECTOR_METADATA}


def test_run_build_rejects_inputs_changed_during_execution(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_path = _write_project(tmp_path)
    task = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/snapshot.json",
    )
    graph = build_artifact_graph([task])
    plan = build_exec.BuildPlan(
        reason="missing",
        artifacts=(task.id,),
        jobs=(),
        skipped_current=(),
        config_hash="before",
        state_path=tmp_path / "state.json",
        previous_state=None,
        graph=graph,
    )
    monkeypatch.setattr(build_exec, "_plan_build", lambda **_kwargs: plan)
    monkeypatch.setattr(
        build_exec, "_log_build_decision", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(build_exec, "_execute_build_jobs", lambda **_kwargs: None)
    monkeypatch.setattr(build_exec, "tasks_dir", lambda _project: tmp_path)
    monkeypatch.setattr(build_exec, "compute_config_hash", lambda *_args: "after")
    resolved: set[str] = set()

    with pytest.raises(RuntimeError, match="Build inputs changed"):
        build_exec.run_build_if_needed(
            project_path,
            graph=graph,
            required_artifacts={task.id},
            mode="AUTO",
            runtime=_runtime(tmp_path / "artifacts"),
            resolved_artifacts=resolved,
        )

    assert resolved == set()


def test_run_build_rejects_unknown_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown artifact mode 'SOMETIMES'"):
        build_exec.run_build_if_needed(
            _write_project(tmp_path),
            graph=build_artifact_graph([]),
            required_artifacts=set(),
            mode="sometimes",
            runtime=_runtime(tmp_path / "artifacts"),
        )
