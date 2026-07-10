from contextlib import contextmanager
import json
from types import SimpleNamespace

import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.resolution import LogLevelDecision, LogOutputSettings
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    OperationTask,
    PipelineTask,
    SchemaTask,
    VectorInputsTask,
)
from datapipeline.io.output import OutputTarget
from datapipeline.profiles.models import (
    ExecutionProfile,
    ProfileRunRequest,
    ServeRunPlan,
)
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.constants import (
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
)
from datapipeline.services.executions import ExecutionPaths
from datapipeline.services.runs import RunPaths

_LOG_DECISION = LogLevelDecision(name="INFO", value=20)
_LOG_OUTPUT = LogOutputSettings(outputs=())


def _dataset() -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                id="feature",
                record_stream="stream",
                field="value",
            )
        ],
        targets=[],
    )


def _runtime(tmp_path):
    return SimpleNamespace(
        artifacts=ArtifactManager(tmp_path / "artifacts"),
        heartbeat_interval_seconds=None,
    )


def _run_paths(tmp_path):
    run_root = tmp_path / "runs" / "r1"
    return RunPaths(
        serve_root=tmp_path,
        runs_root=tmp_path / "runs",
        run_id="r1",
        run_root=run_root,
        dataset_dir=run_root / "dataset",
        metadata_path=run_root / "run.json",
    )


def _execution_paths(tmp_path):
    root = tmp_path / "execution"
    return ExecutionPaths(
        execution_id="e1",
        root=root,
        logs_dir=root / "logs",
        meta_dir=root / "meta",
        metadata_path=root / "execution.json",
    )


def _assert_preflight_rejected(request: ProfileRunRequest) -> None:
    with pytest.raises(SystemExit) as exc:
        run_profiles(request)

    assert exc.value.code == 2
    assert not request.execution.root.exists()


def test_run_profiles_executes_profile_target(monkeypatch, tmp_path):
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = PipelineTask(id="pipeline")
    runtime = _runtime(tmp_path)

    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[vector_inputs, schema, metadata, serve],
        artifact_task_configs=[vector_inputs, schema, metadata],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                dataset=_dataset(),
            )
        ],
    )

    calls = {"bootstrap": 0, "build": 0, "hydrate": 0, "dispatch": 0}

    def _bootstrap(_project_path):
        calls["bootstrap"] += 1
        return runtime

    def _hydrate(*_args, **_kwargs):
        calls["hydrate"] += 1
        return VECTOR_INPUTS, VECTOR_SCHEMA, VECTOR_SCHEMA_METADATA

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.bootstrap_build_runtime",
        _bootstrap,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: calls.__setitem__("build", calls["build"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: calls.__setitem__("dispatch", calls["dispatch"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        _hydrate,
    )

    run_profiles(request)

    assert calls["bootstrap"] == 1
    assert calls["build"] == 1
    assert calls["hydrate"] == 1
    assert calls["dispatch"] == 1


def test_run_profiles_rejects_unknown_target_before_starting_execution(tmp_path):
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="missing",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
            )
        ],
    )

    _assert_preflight_rejected(request)


def test_run_profiles_rejects_unbound_artifact_before_side_effects(tmp_path):
    artifact = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/snapshot.json",
    )
    request = ProfileRunRequest(
        command="build",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[artifact],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="snapshot",
                target_id="snapshot",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
            )
        ],
    )

    _assert_preflight_rejected(request)


def test_run_profiles_rejects_missing_artifact_dependency_before_side_effects(
    monkeypatch,
    tmp_path,
):
    schema = SchemaTask(id="schema")
    request = ProfileRunRequest(
        command="build",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[schema],
        artifact_task_configs=[schema],
        profiles=[
            ExecutionProfile(
                name="schema",
                target_id="schema",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
            )
        ],
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.load_dataset",
        lambda *_args, **_kwargs: _dataset(),
    )

    _assert_preflight_rejected(request)


def test_run_profiles_rejects_missing_dataset_before_side_effects(tmp_path):
    runtime_task = OperationTask(
        id="custom",
        entrypoint="plugin.runtime.custom",
    )
    run = _run_paths(tmp_path)
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[runtime_task],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="custom",
                target_id="custom",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
            )
        ],
        serve_run_plans=(ServeRunPlan(run, None),),
    )

    _assert_preflight_rejected(request)
    assert not run.run_root.exists()


def test_run_profiles_rejects_missing_runtime_producer_before_side_effects(tmp_path):
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    pipeline = PipelineTask(id="pipeline")
    run = _run_paths(tmp_path)
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[schema, metadata, pipeline],
        artifact_task_configs=[schema, metadata],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
                dataset=_dataset(),
            )
        ],
        serve_run_plans=(ServeRunPlan(run, None),),
    )

    _assert_preflight_rejected(request)
    assert not run.run_root.exists()


def test_run_profiles_rejects_invalid_preview_before_side_effects(tmp_path):
    pipeline = PipelineTask(id="pipeline")
    run = _run_paths(tmp_path)
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[pipeline],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
                dataset=_dataset(),
                preview_index=15,
            )
        ],
        serve_run_plans=(ServeRunPlan(run, 15),),
    )

    _assert_preflight_rejected(request)
    assert not run.run_root.exists()


def test_run_profiles_parent_scope_does_not_announce(monkeypatch, tmp_path):
    serve = PipelineTask(id="pipeline")

    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[serve],
        artifact_task_configs=[],
        skip_build=True,
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
                dataset=_dataset(),
                preview_index=0,
            )
        ],
    )

    captured: dict[str, object] = {}

    @contextmanager
    def _capture_scope(**kwargs):
        captured.update(kwargs)
        yield

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execution_scope",
        _capture_scope,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (),
    )

    run_profiles(request)

    assert captured.get("announce") is False


def test_run_profiles_can_skip_build_when_runtime_artifacts_are_current(
    monkeypatch,
    tmp_path,
):
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = PipelineTask(id="pipeline")

    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[vector_inputs, schema, metadata, serve],
        artifact_task_configs=[vector_inputs, schema, metadata],
        skip_build=True,
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
                dataset=_dataset(),
            )
        ],
    )

    calls = {"build": 0, "dispatch": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: calls.__setitem__("build", calls["build"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: calls.__setitem__("dispatch", calls["dispatch"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (
            VECTOR_INPUTS,
            VECTOR_SCHEMA,
            VECTOR_SCHEMA_METADATA,
        ),
    )

    run_profiles(request)

    assert calls["build"] == 0
    assert calls["dispatch"] == 1


def test_run_profiles_skip_build_rejects_missing_runtime_artifacts(
    monkeypatch,
    tmp_path,
):
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = PipelineTask(id="pipeline")
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[vector_inputs, schema, metadata, serve],
        artifact_task_configs=[vector_inputs, schema, metadata],
        skip_build=True,
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
                dataset=_dataset(),
            )
        ],
    )
    calls = {"build": 0, "dispatch": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: calls.__setitem__("build", calls["build"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: calls.__setitem__("dispatch", calls["dispatch"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (),
    )

    with pytest.raises(SystemExit) as exc:
        run_profiles(request)

    assert exc.value.code == 2
    assert calls == {"build": 0, "dispatch": 0}


def test_run_profiles_requires_pipeline_artifact_closure(monkeypatch, tmp_path):
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = PipelineTask(id="pipeline")

    runtime = _runtime(tmp_path)
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[vector_inputs, schema, metadata, serve],
        artifact_task_configs=[vector_inputs, schema, metadata],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=runtime,
                dataset=_dataset(),
            )
        ],
    )

    seen: dict[str, object] = {}

    def _capture_selected_artifacts(**kwargs):
        seen["required_artifacts"] = kwargs["required_artifacts"]

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_selected_artifacts",
        _capture_selected_artifacts,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (
            VECTOR_INPUTS,
            VECTOR_SCHEMA,
            VECTOR_SCHEMA_METADATA,
        ),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )

    run_profiles(request)

    assert seen["required_artifacts"] == {
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
        VECTOR_SCHEMA_METADATA,
    }


def test_run_profiles_hydrates_runtime_artifacts_after_build(monkeypatch, tmp_path):
    (tmp_path / "project.yaml").write_text(
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

    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = PipelineTask(id="pipeline")

    runtime = _runtime(tmp_path)
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[vector_inputs, schema, metadata, serve],
        artifact_task_configs=[vector_inputs, schema, metadata],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=runtime,
                dataset=_dataset(),
            )
        ],
    )

    seen = {"synced": False}

    def _capture_dispatch(**kwargs):
        seen["synced"] = kwargs["runtime"].artifacts.has(VECTOR_SCHEMA)

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        _capture_dispatch,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_selected_artifacts",
        lambda **_kwargs: None,
    )

    def _hydrate(runtime, *_args, **_kwargs):
        runtime.artifacts.register(VECTOR_INPUTS, "build/vector_inputs.json")
        runtime.artifacts.register(VECTOR_SCHEMA, "build/schema.json")
        runtime.artifacts.register(VECTOR_SCHEMA_METADATA, "build/metadata.json")
        return VECTOR_INPUTS, VECTOR_SCHEMA, VECTOR_SCHEMA_METADATA

    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        _hydrate,
    )

    run_profiles(request)

    assert seen["synced"] is True


def test_run_profiles_forward_runtime_build_settings(monkeypatch, tmp_path):
    snapshot = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/snapshot.json",
    )

    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[snapshot],
        artifact_task_configs=[snapshot],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="snapshot",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
                build_mode="FORCE",
            )
        ],
    )

    seen: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: seen.update(kwargs),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )

    run_profiles(request)

    settings = seen["settings"]
    assert settings.mode == "FORCE"
    assert settings.force is True
    assert settings.profile_name == "serve"


def test_runtime_dependency_build_scope_isolated_from_parent_profile(
    monkeypatch, tmp_path
):
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = PipelineTask(id="pipeline")

    request = ProfileRunRequest(
        command="inspect",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[vector_inputs, schema, metadata, serve],
        artifact_task_configs=[vector_inputs, schema, metadata],
        profiles=[
            ExecutionProfile(
                name="coverage",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=_runtime(tmp_path),
                dataset=_dataset(),
            )
        ],
    )

    captured = {"count": 0}

    def _capture_selected_artifacts(**_kwargs):
        captured["count"] += 1

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_selected_artifacts",
        _capture_selected_artifacts,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (
            VECTOR_INPUTS,
            VECTOR_SCHEMA,
            VECTOR_SCHEMA_METADATA,
        ),
    )

    run_profiles(request)

    assert captured["count"] == 1


def test_run_profiles_finalize_shared_serve_run_once(monkeypatch, tmp_path):
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "plugin.runtime",
        }
    )
    run_paths = _run_paths(tmp_path)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=run_paths.dataset_dir / "train.jsonl",
        run=run_paths,
    )
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="train",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
                output=target,
            ),
            ExecutionProfile(
                name="val",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
                output=target,
            ),
        ],
        serve_run_plans=(ServeRunPlan(run_paths, None),),
    )

    calls = {"start": 0, "success": 0, "failed": 0, "latest": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.start_run",
        lambda _run, preview_index: calls.__setitem__("start", calls["start"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_success",
        lambda _run: calls.__setitem__("success", calls["success"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        lambda _run: calls.__setitem__("failed", calls["failed"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        lambda _run: calls.__setitem__("latest", calls["latest"] + 1),
    )

    run_profiles(request)

    assert calls == {"start": 1, "success": 1, "failed": 0, "latest": 1}


def test_run_profiles_fail_shared_serve_run_once_when_later_profile_errors(
    monkeypatch, tmp_path
):
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "plugin.runtime",
        }
    )
    run_paths = _run_paths(tmp_path)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=run_paths.dataset_dir / "train.jsonl",
        run=run_paths,
    )
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="train",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
                output=target,
            ),
            ExecutionProfile(
                name="val",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
                output=target,
            ),
        ],
        serve_run_plans=(ServeRunPlan(run_paths, None),),
    )

    state = {"calls": 0}
    outcomes = {"start": 0, "success": 0, "failed": 0, "latest": 0}

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )

    def _execute_profile(**kwargs):
        state["calls"] += 1
        if state["calls"] == 2:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        _execute_profile,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.start_run",
        lambda _run, preview_index: outcomes.__setitem__(
            "start", outcomes["start"] + 1
        ),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_success",
        lambda _run: outcomes.__setitem__("success", outcomes["success"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        lambda _run: outcomes.__setitem__("failed", outcomes["failed"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        lambda _run: outcomes.__setitem__("latest", outcomes["latest"] + 1),
    )

    with pytest.raises(RuntimeError, match="boom"):
        run_profiles(request)

    assert outcomes == {"start": 1, "success": 0, "failed": 1, "latest": 0}


def test_run_profiles_materializes_preview_run_at_execution_boundary(
    monkeypatch,
    tmp_path,
):
    serve = PipelineTask(id="pipeline")
    run_paths = _run_paths(tmp_path / "serve")
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=run_paths.dataset_dir / "preview.jsonl",
        run=run_paths,
    )
    runtime = SimpleNamespace()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="preview",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=runtime,
                dataset=object(),
                output=target,
                preview_index=3,
                heartbeat_interval_seconds=15,
            )
        ],
        serve_run_plans=(ServeRunPlan(run_paths, 3),),
    )
    observed: dict[str, object] = {}

    def _run_profile(spec, work):
        execution_metadata = json.loads(
            request.execution.metadata_path.read_text(encoding="utf-8")
        )
        run_metadata = json.loads(run_paths.metadata_path.read_text(encoding="utf-8"))
        observed["execution_command"] = execution_metadata["command"]
        observed["running_status"] = run_metadata["status"]
        observed["running_preview_index"] = run_metadata["preview_index"]
        observed["heartbeat"] = spec.runtime.heartbeat_interval_seconds
        return work()

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        _run_profile,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: None,
    )
    latest_calls: list[RunPaths] = []
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        latest_calls.append,
    )

    assert not request.execution.root.exists()
    assert not run_paths.run_root.exists()
    run_profiles(request)

    assert observed["execution_command"] == "serve"
    assert observed["running_status"] == "running"
    assert observed["running_preview_index"] == 3
    assert observed["heartbeat"] == 15
    finished_run = json.loads(run_paths.metadata_path.read_text(encoding="utf-8"))
    assert finished_run["status"] == "success"
    assert finished_run["preview_index"] == 3
    assert latest_calls == []


def test_run_profiles_applies_each_profile_heartbeat_before_work(
    monkeypatch,
    tmp_path,
):
    task = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "plugin.runtime",
        }
    )
    runtime = SimpleNamespace()
    request = ProfileRunRequest(
        command="inspect",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[task],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name=name,
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=runtime,
                dataset=object(),
                heartbeat_interval_seconds=heartbeat,
            )
            for name, heartbeat in (("first", 10), ("second", 20), ("third", None))
        ],
    )
    observed: list[float | None] = []
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: observed.append(
            kwargs["runtime_override"].heartbeat_interval_seconds
        ),
    )

    run_profiles(request)

    assert observed == [10, 20, None]


def test_later_run_start_failure_marks_only_started_runs_failed(
    monkeypatch,
    tmp_path,
):
    task = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "plugin.runtime",
        }
    )
    first_run = _run_paths(tmp_path / "first")
    second_run = _run_paths(tmp_path / "second")
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        execution=_execution_paths(tmp_path),
        tasks=[task],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=_LOG_DECISION,
                log_output=_LOG_OUTPUT,
                runtime=SimpleNamespace(),
                dataset=object(),
            )
        ],
        serve_run_plans=(
            ServeRunPlan(first_run, None),
            ServeRunPlan(second_run, None),
        ),
    )
    starts: list[RunPaths] = []

    def _start_run(paths, preview_index):
        starts.append(paths)
        if paths == second_run:
            raise RuntimeError("cannot start second run")

    failed: list[RunPaths] = []
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.start_run",
        _start_run,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        failed.append,
    )

    with pytest.raises(RuntimeError, match="cannot start second run"):
        run_profiles(request)

    assert starts == [first_run, second_run]
    assert failed == [first_run]
