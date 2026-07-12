from contextlib import contextmanager
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.resolution import LogLevelDecision, LogOutputSettings
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    OperationTask,
    PipelineTask,
    SchemaTask,
    VectorInputsTask,
)
from datapipeline.profiles.executor import ExecutionSpec
from datapipeline.profiles.models import (
    ExecutionProfile,
    ProfileRunRequest,
    ServeRunPlan,
)
from datapipeline.profiles.orchestration import (
    _validate_build_profile_order,
    run_profiles,
)
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.constants import (
    VECTOR_INPUTS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
)
from datapipeline.services.runs import RunPaths

_LOG_DECISION = LogLevelDecision(name="INFO", value=20)
_LOG_OUTPUT = LogOutputSettings(outputs=())


def _artifact_settings(
    mode: str = "AUTO",
    heartbeat_interval_seconds: float | None = None,
) -> BuildSettings:
    return BuildSettings(
        visuals="off",
        log_decision=_LOG_DECISION,
        log_output=_LOG_OUTPUT,
        mode=mode,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


@pytest.fixture(autouse=True)
def _stable_config_hash(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.compute_config_hash",
        lambda *_args: "hash-1",
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.tasks_dir",
        lambda project: project.parent / "tasks",
    )


def _dataset(*, scale: bool = False) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                id="feature",
                record_stream="stream",
                field="value",
                scale=scale,
            )
        ],
    )


def _runtime(tmp_path: Path, marker: str = "runtime") -> SimpleNamespace:
    return SimpleNamespace(
        marker=marker,
        artifacts=ArtifactManager(tmp_path / marker / "artifacts"),
        heartbeat_interval_seconds=None,
    )


def _runtime_profile(
    name: str,
    target_id: str,
    runtime,
    dataset,
    *,
    preview_index: int | None = None,
    heartbeat_interval_seconds: float | None = None,
) -> ExecutionProfile:
    return ExecutionProfile(
        name=name,
        target_id=target_id,
        visuals="off",
        log_decision=_LOG_DECISION,
        log_output=_LOG_OUTPUT,
        runtime=runtime,
        dataset=dataset,
        preview_index=preview_index,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


def _build_profile(
    name: str,
    target_id: str,
    runtime,
    *,
    mode: str = "AUTO",
) -> ExecutionProfile:
    return ExecutionProfile(
        name=name,
        target_id=target_id,
        visuals="off",
        log_decision=_LOG_DECISION,
        log_output=_LOG_OUTPUT,
        runtime=runtime,
        build_settings=BuildSettings(
            visuals="off",
            log_decision=_LOG_DECISION,
            log_output=_LOG_OUTPUT,
            mode=mode,
        ),
    )


def _run_paths(tmp_path: Path, run_id: str = "r1") -> RunPaths:
    run_root = tmp_path / "runs" / run_id
    return RunPaths(
        serve_root=tmp_path,
        runs_root=tmp_path / "runs",
        run_id=run_id,
        run_root=run_root,
        dataset_dir=run_root / "dataset",
        metadata_path=run_root / "run.json",
    )


def _request(
    tmp_path: Path,
    *,
    command: str,
    tasks,
    artifact_tasks,
    profiles,
    artifact_settings: BuildSettings | None = None,
    serve_run_plans: tuple[ServeRunPlan, ...] = (),
    execution: ExecutionConfig | None = None,
) -> ProfileRunRequest:
    return ProfileRunRequest(
        command=command,
        project_path=tmp_path / "project.yaml",
        tasks=tasks,
        artifact_task_configs=artifact_tasks,
        profiles=profiles,
        execution=ExecutionConfig() if execution is None else execution,
        config_hash="hash-1",
        artifact_settings=artifact_settings,
        serve_run_plans=serve_run_plans,
    )


def _assert_preflight_rejected(request: ProfileRunRequest) -> None:
    with pytest.raises(SystemExit) as exc:
        run_profiles(request)

    assert exc.value.code == 2


def test_build_profile_order_accepts_configured_dependency_order() -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    graph = build_artifact_graph([vector_inputs, schema, metadata])

    _validate_build_profile_order([vector_inputs, schema, metadata], graph)


def test_build_profile_order_rejects_dependency_after_dependent() -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([vector_inputs, schema])

    with pytest.raises(ValueError, match="vector_inputs.*before.*schema"):
        _validate_build_profile_order([schema, vector_inputs], graph)


def test_build_profile_order_rejects_duplicate_targets() -> None:
    schema = SchemaTask(id="schema")

    with pytest.raises(ValueError, match="unique artifact targets"):
        _validate_build_profile_order(
            [schema, schema],
            build_artifact_graph([schema]),
        )


def test_build_profile_order_rejects_runtime_task() -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")

    with pytest.raises(ValueError, match="must target artifact tasks"):
        _validate_build_profile_order([task], build_artifact_graph([]))


def test_build_profiles_keep_configured_order_and_share_resolved_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    vector_runtime = _runtime(tmp_path, "vector-runtime")
    schema_runtime = _runtime(tmp_path, "schema-runtime")
    request = _request(
        tmp_path,
        command="build",
        tasks=[vector_inputs, schema],
        artifact_tasks=[vector_inputs, schema],
        profiles=[
            _build_profile("vector_inputs", "vector_inputs", vector_runtime),
            _build_profile("schema", "schema", schema_runtime, mode="FORCE"),
        ],
    )
    calls: list[dict[str, object]] = []

    def build(_project, **kwargs):
        calls.append(dict(kwargs))
        kwargs["resolved_artifacts"].update(kwargs["required_artifacts"])

    monkeypatch.setattr(
        "datapipeline.profiles.execution.load_dataset",
        lambda *_args: _dataset(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        build,
    )

    run_profiles(request)

    assert [call["profile_name"] for call in calls] == ["vector_inputs", "schema"]
    assert [call["runtime"].marker for call in calls] == [
        "vector-runtime",
        "schema-runtime",
    ]
    assert calls[0]["mode"] == "AUTO"
    assert calls[1]["mode"] == "FORCE"
    assert calls[0]["resolved_artifacts"] is calls[1]["resolved_artifacts"]
    assert calls[1]["resolved_artifacts"] == {VECTOR_INPUTS, VECTOR_SCHEMA}
    assert {call["expected_config_hash"] for call in calls} == {"hash-1"}


def test_runtime_artifact_union_is_prepared_once_before_profiles(
    monkeypatch,
    tmp_path: Path,
) -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    pipeline = PipelineTask(id="pipeline")
    first_runtime = _runtime(tmp_path, "first-profile")
    second_runtime = _runtime(tmp_path, "second-profile")
    canonical_runtime = _runtime(tmp_path, "canonical-build")
    execution = ExecutionConfig(sort_batch_records=32)
    artifact_settings = _artifact_settings("FORCE", 0)
    request = _request(
        tmp_path,
        command="serve",
        tasks=[vector_inputs, schema, metadata, pipeline],
        artifact_tasks=[vector_inputs, schema, metadata],
        profiles=[
            _runtime_profile(
                "metadata-preview",
                "pipeline",
                first_runtime,
                _dataset(),
                preview_index=12,
            ),
            _runtime_profile(
                "schema-preview",
                "pipeline",
                second_runtime,
                _dataset(),
                preview_index=13,
            ),
        ],
        artifact_settings=artifact_settings,
        execution=execution,
    )
    events: list[tuple[str, object]] = []
    build_calls: list[dict[str, object]] = []
    execution_specs: list[ExecutionSpec] = []

    def build(_project, **kwargs):
        events.append(("build", kwargs["runtime"]))
        build_calls.append(dict(kwargs))

    def execute(spec, work):
        execution_specs.append(spec)
        events.append(("artifact execution started", spec.runtime))
        result = work()
        events.append(("artifact execution finished", spec.runtime))
        return result

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.bootstrap_build_runtime",
        lambda _project: canonical_runtime,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_build_if_needed",
        build,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        execute,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: events.append(("profile", kwargs["runtime_override"])),
    )

    run_profiles(request)

    assert events == [
        ("artifact execution started", canonical_runtime),
        ("build", canonical_runtime),
        ("artifact execution finished", canonical_runtime),
        ("profile", first_runtime),
        ("profile", second_runtime),
    ]
    assert execution_specs == [
        ExecutionSpec(
            visuals=artifact_settings.visuals,
            log_decision=artifact_settings.log_decision,
            log_output=artifact_settings.log_output,
            runtime=canonical_runtime,
        )
    ]
    assert len(build_calls) == 1
    assert build_calls[0]["required_artifacts"] == {
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
        VECTOR_METADATA,
    }
    assert build_calls[0]["mode"] == "FORCE"
    assert "profile_name" not in build_calls[0]
    assert build_calls[0]["heartbeat_interval_seconds"] == 0
    assert build_calls[0]["expected_config_hash"] == "hash-1"
    assert canonical_runtime.execution == execution
    assert first_runtime.execution == execution
    assert second_runtime.execution == execution


def test_custom_runtime_artifact_requirement_is_prepared(
    monkeypatch,
    tmp_path: Path,
) -> None:
    snapshot = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/snapshot.json",
    )
    report = OperationTask(
        id="report",
        entrypoint="plugin.runtime.report",
        requires=("snapshot",),
    )
    request = _request(
        tmp_path,
        command="inspect",
        tasks=[snapshot, report],
        artifact_tasks=[snapshot],
        profiles=[_runtime_profile("report", "report", _runtime(tmp_path), object())],
        artifact_settings=_artifact_settings(),
    )
    build_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.bootstrap_build_runtime",
        lambda _project: _runtime(tmp_path, "artifact-build"),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_build_if_needed",
        lambda _project, **kwargs: build_calls.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: None,
    )

    run_profiles(request)

    assert len(build_calls) == 1
    assert build_calls[0]["required_artifacts"] == {"snapshot"}


def test_custom_runtime_missing_required_producer_is_rejected_before_execution(
    tmp_path: Path,
) -> None:
    report = OperationTask(
        id="report",
        entrypoint="plugin.runtime.report",
        requires=("schema",),
    )
    request = _request(
        tmp_path,
        command="inspect",
        tasks=[report],
        artifact_tasks=[],
        profiles=[_runtime_profile("report", "report", _runtime(tmp_path), object())],
    )

    _assert_preflight_rejected(request)


def test_artifact_free_runtime_rejects_config_drift_before_execution(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report = OperationTask(id="report", entrypoint="plugin.runtime.report")
    request = _request(
        tmp_path,
        command="inspect",
        tasks=[report],
        artifact_tasks=[],
        profiles=[_runtime_profile("report", "report", _runtime(tmp_path), object())],
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.compute_config_hash",
        lambda *_args: "changed",
    )

    _assert_preflight_rejected(request)


def test_unknown_target_is_rejected_before_execution(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        command="serve",
        tasks=[],
        artifact_tasks=[],
        profiles=[_runtime_profile("serve", "missing", _runtime(tmp_path), object())],
    )

    _assert_preflight_rejected(request)


@pytest.mark.parametrize("command", ["serve", "inspect"])
def test_runtime_command_rejects_artifact_target(
    command: str,
    tmp_path: Path,
) -> None:
    artifact = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/snapshot.json",
    )
    request = _request(
        tmp_path,
        command=command,
        tasks=[artifact],
        artifact_tasks=[artifact],
        profiles=[
            _runtime_profile("snapshot", "snapshot", _runtime(tmp_path), object())
        ],
    )

    _assert_preflight_rejected(request)


def test_build_command_rejects_runtime_target(tmp_path: Path) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    request = _request(
        tmp_path,
        command="build",
        tasks=[task],
        artifact_tasks=[],
        profiles=[_build_profile("pipeline", "pipeline", _runtime(tmp_path))],
    )

    _assert_preflight_rejected(request)


def test_runtime_profile_requires_dataset_before_execution(tmp_path: Path) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    profile = ExecutionProfile(
        name="serve",
        target_id="pipeline",
        visuals="off",
        log_decision=_LOG_DECISION,
        log_output=_LOG_OUTPUT,
        runtime=_runtime(tmp_path),
    )
    request = _request(
        tmp_path,
        command="serve",
        tasks=[task],
        artifact_tasks=[],
        profiles=[profile],
    )

    _assert_preflight_rejected(request)


def test_missing_runtime_artifact_producer_is_rejected_before_execution(
    tmp_path: Path,
) -> None:
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    pipeline = PipelineTask(id="pipeline")
    request = _request(
        tmp_path,
        command="serve",
        tasks=[schema, metadata, pipeline],
        artifact_tasks=[schema, metadata],
        profiles=[
            _runtime_profile("serve", "pipeline", _runtime(tmp_path), _dataset())
        ],
    )

    _assert_preflight_rejected(request)


def test_invalid_preview_is_rejected_before_execution(tmp_path: Path) -> None:
    pipeline = PipelineTask(id="pipeline")
    run_paths = _run_paths(tmp_path)
    request = _request(
        tmp_path,
        command="serve",
        tasks=[pipeline],
        artifact_tasks=[],
        profiles=[
            _runtime_profile(
                "preview",
                "pipeline",
                _runtime(tmp_path),
                _dataset(),
                preview_index=14,
            )
        ],
        serve_run_plans=(ServeRunPlan(run_paths, 14),),
    )

    _assert_preflight_rejected(request)
    assert not run_paths.run_root.exists()


def test_runtime_profiles_keep_order_heartbeat_and_execution_scope(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    runtime = _runtime(tmp_path)
    request = _request(
        tmp_path,
        command="inspect",
        tasks=[task],
        artifact_tasks=[],
        profiles=[
            _runtime_profile(
                name,
                "pipeline",
                runtime,
                object(),
                heartbeat_interval_seconds=heartbeat,
            )
            for name, heartbeat in (("first", 10), ("second", 20), ("third", None))
        ],
    )
    observed: list[tuple[str, float | None]] = []
    scopes: list[dict[str, object]] = []
    artifact_execution_specs: list[ExecutionSpec] = []

    @contextmanager
    def scope(**kwargs):
        scopes.append(kwargs)
        yield

    def artifact_execution(spec, work):
        artifact_execution_specs.append(spec)
        return work()

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execution_scope",
        scope,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        artifact_execution,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: observed.append(
            (
                kwargs["profile"].name,
                kwargs["runtime_override"].heartbeat_interval_seconds,
            )
        ),
    )

    run_profiles(request)

    assert observed == [("first", 10), ("second", 20), ("third", None)]
    assert [scope["profile_name"] for scope in scopes] == [
        "first",
        "second",
        "third",
    ]
    assert artifact_execution_specs == []


def test_shared_serve_run_is_finalized_once(monkeypatch, tmp_path: Path) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    run_paths = _run_paths(tmp_path)
    request = _request(
        tmp_path,
        command="serve",
        tasks=[task],
        artifact_tasks=[],
        profiles=[
            _runtime_profile(name, "pipeline", _runtime(tmp_path, name), object())
            for name in ("train", "val")
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
    for name in calls:
        function = "set_latest_run" if name == "latest" else f"{name}_run"
        if name == "success":
            function = "finish_run_success"
        elif name == "failed":
            function = "finish_run_failed"
        monkeypatch.setattr(
            f"datapipeline.profiles.orchestration.{function}",
            lambda *_args, key=name, **_kwargs: calls.__setitem__(key, calls[key] + 1),
        )

    run_profiles(request)

    assert calls == {"start": 1, "success": 1, "failed": 0, "latest": 1}


def test_profile_failure_marks_shared_run_failed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    run_paths = _run_paths(tmp_path)
    request = _request(
        tmp_path,
        command="serve",
        tasks=[task],
        artifact_tasks=[],
        profiles=[
            _runtime_profile(name, "pipeline", _runtime(tmp_path, name), object())
            for name in ("train", "val")
        ],
        serve_run_plans=(ServeRunPlan(run_paths, None),),
    )
    calls = 0
    failed: list[RunPaths] = []

    def execute(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        execute,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.start_run",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        failed.append,
    )

    with pytest.raises(RuntimeError, match="boom"):
        run_profiles(request)

    assert failed == [run_paths]


def test_preview_run_exists_at_profile_boundary_and_is_not_latest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    pipeline = PipelineTask(id="pipeline")
    run_paths = _run_paths(tmp_path)
    runtime = _runtime(tmp_path)
    request = _request(
        tmp_path,
        command="serve",
        tasks=[pipeline],
        artifact_tasks=[],
        profiles=[
            _runtime_profile(
                "preview",
                "pipeline",
                runtime,
                object(),
                preview_index=3,
                heartbeat_interval_seconds=15,
            )
        ],
        serve_run_plans=(ServeRunPlan(run_paths, 3),),
    )
    observed: dict[str, object] = {}

    def run_profile(spec, work):
        run = json.loads(run_paths.metadata_path.read_text(encoding="utf-8"))
        observed.update(
            status=run["status"],
            preview_index=run["preview_index"],
            heartbeat=spec.runtime.heartbeat_interval_seconds,
        )
        return work()

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        run_profile,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: None,
    )
    latest: list[RunPaths] = []
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        latest.append,
    )

    run_profiles(request)

    assert observed == {
        "status": "running",
        "preview_index": 3,
        "heartbeat": 15,
    }
    finished = json.loads(run_paths.metadata_path.read_text(encoding="utf-8"))
    assert finished["status"] == "success"
    assert latest == []


def test_later_run_start_failure_fails_only_started_run(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    first = _run_paths(tmp_path / "first")
    second = _run_paths(tmp_path / "second")
    request = _request(
        tmp_path,
        command="serve",
        tasks=[task],
        artifact_tasks=[],
        profiles=[_runtime_profile("serve", "pipeline", _runtime(tmp_path), object())],
        serve_run_plans=(ServeRunPlan(first, None), ServeRunPlan(second, None)),
    )
    starts: list[RunPaths] = []
    failed: list[RunPaths] = []

    def start(paths, *, preview_index):
        starts.append(paths)
        if paths == second:
            raise RuntimeError("cannot start second run")

    monkeypatch.setattr("datapipeline.profiles.orchestration.start_run", start)
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        failed.append,
    )

    with pytest.raises(RuntimeError, match="cannot start second run"):
        run_profiles(request)

    assert starts == [first, second]
    assert failed == [first]
