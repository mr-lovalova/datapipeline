import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    ObservabilitySettings,
)
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    OperationTask,
    PipelineTask,
    SchemaTask,
    VectorInputsTask,
)
from datapipeline.io.output import OutputTarget
from datapipeline.profiles.execution import RuntimeJobPlan, execute_runtime_job
from datapipeline.profiles.executor import ExecutionSpec
from datapipeline.profiles.models import (
    BuildJob,
    BuildRunRequest,
    RuntimeJob,
    RuntimeRunRequest,
    ServeRunPlan,
)
from datapipeline.profiles.orchestration import _validate_build_order, run_profiles
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.constants import (
    VECTOR_INPUTS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
)
from datapipeline.services.runs import RunPaths

_LOG_DECISION = LogLevelDecision(name="INFO", value=logging.INFO)
_LOG_OUTPUT = LogOutputSettings(outputs=())


def _artifact_settings(
    mode: str = "AUTO",
    heartbeat_interval_seconds: float | None = None,
) -> BuildSettings:
    return BuildSettings(
        mode=mode,
        observability=_observability(heartbeat_interval_seconds),
    )


def _observability(
    heartbeat_interval_seconds: float | None = None,
) -> ObservabilitySettings:
    return ObservabilitySettings(
        visuals="off",
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        log_decision=_LOG_DECISION,
        log_output=_LOG_OUTPUT,
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
        sample=SampleConfig(cadence="1h"),
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
        execution=ExecutionConfig(),
        heartbeat_interval_seconds=None,
        split_labels=(),
    )


def _output() -> OutputTarget:
    return OutputTarget(
        transport="stdout",
        format="jsonl",
        view="raw",
        encoding=None,
        destination=None,
    )


def _runtime_job(
    name: str,
    task: OperationTask,
    runtime,
    dataset,
    *,
    preview: PreviewStage | None = None,
    heartbeat_interval_seconds: float | None = None,
    splits: tuple[str, ...] = (),
) -> RuntimeJob:
    return RuntimeJob(
        name=name,
        task=task,
        runtime=runtime,
        dataset=dataset,
        output=_output(),
        observability=_observability(heartbeat_interval_seconds),
        limit=None,
        throttle_ms=None,
        preview=preview,
        splits=splits,
    )


def _build_request(
    tmp_path: Path,
    artifact_tasks,
    jobs,
    execution: ExecutionConfig | None = None,
) -> BuildRunRequest:
    return BuildRunRequest(
        project_path=tmp_path / "project.yaml",
        artifact_task_configs=artifact_tasks,
        jobs=jobs,
        execution=ExecutionConfig() if execution is None else execution,
        config_hash="hash-1",
    )


def _runtime_request(
    tmp_path: Path,
    *,
    command: str,
    artifact_tasks,
    jobs,
    artifact_settings: BuildSettings | None = None,
    serve_run_plans: tuple[ServeRunPlan, ...] = (),
    execution: ExecutionConfig | None = None,
) -> RuntimeRunRequest:
    return RuntimeRunRequest(
        command=command,
        project_path=tmp_path / "project.yaml",
        artifact_task_configs=artifact_tasks,
        jobs=jobs,
        execution=ExecutionConfig() if execution is None else execution,
        config_hash="hash-1",
        artifact_settings=artifact_settings or _artifact_settings(),
        serve_run_plans=serve_run_plans,
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


def _assert_preflight_rejected(request: BuildRunRequest | RuntimeRunRequest) -> None:
    with pytest.raises(SystemExit) as exc:
        run_profiles(request)
    assert exc.value.code == 2


def test_build_order_accepts_configured_dependency_order() -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    graph = build_artifact_graph([vector_inputs, schema, metadata])

    _validate_build_order(
        [
            BuildJob(vector_inputs, _artifact_settings()),
            BuildJob(metadata, _artifact_settings()),
            BuildJob(schema, _artifact_settings()),
        ],
        graph,
    )


def test_build_order_rejects_dependency_after_dependent() -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    metadata = MetadataTask(id="metadata")
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([vector_inputs, metadata, schema])

    with pytest.raises(ValueError, match="vector_inputs.*before.*schema"):
        _validate_build_order(
            [
                BuildJob(schema, _artifact_settings()),
                BuildJob(vector_inputs, _artifact_settings()),
            ],
            graph,
        )


def test_build_order_rejects_duplicate_targets() -> None:
    schema = SchemaTask(id="schema")

    with pytest.raises(ValueError, match="unique artifact targets"):
        _validate_build_order(
            [
                BuildJob(schema, _artifact_settings()),
                BuildJob(schema, _artifact_settings()),
            ],
            build_artifact_graph([schema]),
        )


def test_build_jobs_keep_order_and_share_resolved_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    metadata = MetadataTask(id="metadata")
    schema = SchemaTask(id="schema")
    vector_runtime = _runtime(tmp_path, "vector-runtime")
    metadata_runtime = _runtime(tmp_path, "metadata-runtime")
    schema_runtime = _runtime(tmp_path, "schema-runtime")
    execution = ExecutionConfig(sort_buffer_mb=32)
    request = _build_request(
        tmp_path,
        [vector_inputs, metadata, schema],
        [
            BuildJob(vector_inputs, _artifact_settings()),
            BuildJob(metadata, _artifact_settings()),
            BuildJob(schema, _artifact_settings("FORCE")),
        ],
        execution,
    )
    calls: list[dict[str, object]] = []
    execution_specs: list[ExecutionSpec] = []
    runtimes = iter((vector_runtime, metadata_runtime, schema_runtime))

    def build(_project, **kwargs):
        calls.append(dict(kwargs))
        kwargs["resolved_artifacts"].update(kwargs["required_artifacts"])

    def execute(spec, work):
        execution_specs.append(spec)
        return work()

    monkeypatch.setattr(
        "datapipeline.profiles.execution.load_dataset",
        lambda *_args: _dataset(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.bootstrap_build_runtime",
        lambda _project: next(runtimes),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        execute,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_build_if_needed",
        build,
    )

    run_profiles(request)

    assert [call["required_artifacts"] for call in calls] == [
        {VECTOR_INPUTS},
        {VECTOR_METADATA},
        {VECTOR_SCHEMA},
    ]
    assert [call["runtime"].marker for call in calls] == [
        "vector-runtime",
        "metadata-runtime",
        "schema-runtime",
    ]
    assert [call["settings"].mode for call in calls] == ["AUTO", "AUTO", "FORCE"]
    assert calls[0]["resolved_artifacts"] is calls[2]["resolved_artifacts"]
    assert calls[2]["resolved_artifacts"] == {
        VECTOR_INPUTS,
        VECTOR_METADATA,
        VECTOR_SCHEMA,
    }
    assert {call["expected_config_hash"] for call in calls} == {"hash-1"}
    assert [spec.runtime for spec in execution_specs] == [
        vector_runtime,
        metadata_runtime,
        schema_runtime,
    ]
    assert vector_runtime.execution == execution
    assert metadata_runtime.execution == execution
    assert schema_runtime.execution == execution


def test_runtime_artifact_union_is_prepared_once_before_jobs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    vector_inputs = VectorInputsTask(id="vector_inputs")
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    pipeline = PipelineTask(id="pipeline")
    first_runtime = _runtime(tmp_path, "first-job")
    second_runtime = _runtime(tmp_path, "second-job")
    canonical_runtime = _runtime(tmp_path, "canonical-build")
    execution = ExecutionConfig(sort_buffer_mb=32)
    artifact_settings = _artifact_settings("FORCE", 0)
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[vector_inputs, schema, metadata],
        jobs=[
            _runtime_job(
                "samples-preview",
                pipeline,
                first_runtime,
                _dataset(),
                preview="samples",
            ),
            _runtime_job(
                "postprocess-preview",
                pipeline,
                second_runtime,
                _dataset(),
                preview="postprocess",
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
        events.append(("execution started", spec.runtime))
        result = work()
        events.append(("execution finished", spec.runtime))
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
        "datapipeline.profiles.orchestration.execute_runtime_job",
        lambda _command, _project, _graph, plan: events.append(
            ("job", plan.job.runtime)
        ),
    )

    run_profiles(request)

    assert events == [
        ("execution started", canonical_runtime),
        ("build", canonical_runtime),
        ("execution finished", canonical_runtime),
        ("execution started", first_runtime),
        ("job", first_runtime),
        ("execution finished", first_runtime),
        ("execution started", second_runtime),
        ("job", second_runtime),
        ("execution finished", second_runtime),
    ]
    assert [spec.runtime for spec in execution_specs] == [
        canonical_runtime,
        first_runtime,
        second_runtime,
    ]
    assert len(build_calls) == 1
    assert build_calls[0]["required_artifacts"] == {
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
        VECTOR_METADATA,
    }
    assert build_calls[0]["settings"] is artifact_settings
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
    request = _runtime_request(
        tmp_path,
        command="inspect",
        artifact_tasks=[snapshot],
        jobs=[_runtime_job("report", report, _runtime(tmp_path), _dataset())],
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
        "datapipeline.profiles.orchestration.run_execution",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_runtime_job",
        lambda *_args: None,
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
    request = _runtime_request(
        tmp_path,
        command="inspect",
        artifact_tasks=[],
        jobs=[_runtime_job("report", report, _runtime(tmp_path), _dataset())],
    )

    _assert_preflight_rejected(request)


def test_artifact_free_runtime_rejects_config_drift_before_execution(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report = OperationTask(id="report", entrypoint="plugin.runtime.report")
    request = _runtime_request(
        tmp_path,
        command="inspect",
        artifact_tasks=[],
        jobs=[_runtime_job("report", report, _runtime(tmp_path), _dataset())],
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.compute_config_hash",
        lambda *_args: "changed",
    )

    _assert_preflight_rejected(request)


def test_missing_runtime_artifact_producer_is_rejected_before_execution(
    tmp_path: Path,
) -> None:
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    pipeline = PipelineTask(id="pipeline")
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[schema, metadata],
        jobs=[_runtime_job("serve", pipeline, _runtime(tmp_path), _dataset())],
    )

    _assert_preflight_rejected(request)


def test_invalid_preview_is_rejected_before_starting_run(tmp_path: Path) -> None:
    pipeline = PipelineTask(id="pipeline")
    run_paths = _run_paths(tmp_path)
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=[
            _runtime_job(
                "preview",
                pipeline,
                _runtime(tmp_path),
                _dataset(),
                preview="unknown",  # type: ignore[arg-type]
            )
        ],
        serve_run_plans=(
            ServeRunPlan(run_paths, "unknown"),  # type: ignore[arg-type]
        ),
    )

    _assert_preflight_rejected(request)
    assert not run_paths.run_root.exists()


def test_runtime_jobs_keep_order_and_apply_execution_settings(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    execution = ExecutionConfig(sort_buffer_mb=16)
    jobs = [
        _runtime_job(
            name,
            task,
            _runtime(tmp_path, name),
            _dataset(),
            heartbeat_interval_seconds=heartbeat,
            splits=splits,
        )
        for name, heartbeat, splits in (
            ("first", 10, ("train",)),
            ("second", 20, ("val",)),
            ("third", None, ()),
        )
    ]
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=jobs,
        execution=execution,
    )
    observed: list[tuple[str, object, object, object]] = []
    execution_specs: list[ExecutionSpec] = []

    def execute(spec, work):
        execution_specs.append(spec)
        return work()

    def execute_job(_command, _project, _graph, plan):
        runtime = plan.job.runtime
        observed.append(
            (
                plan.job.name,
                runtime.heartbeat_interval_seconds,
                runtime.split_labels,
                runtime.execution,
            )
        )

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        execute,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_runtime_job",
        execute_job,
    )

    run_profiles(request)

    assert observed == [
        ("first", 10, ("train",), execution),
        ("second", 20, ("val",), execution),
        ("third", None, (), execution),
    ]
    assert [spec.runtime for spec in execution_specs] == [job.runtime for job in jobs]


def test_runtime_job_emits_resolved_config_at_debug(
    monkeypatch, tmp_path: Path
) -> None:
    runtime = _runtime(tmp_path)
    runtime.execution = ExecutionConfig(sort_buffer_mb=24)
    task = OperationTask(id="report", entrypoint="plugin.runtime.report")
    job = _runtime_job("coverage", task, runtime, _dataset())
    messages: list[tuple[str, int]] = []

    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.emit_execution_message",
        lambda message, level, logger: messages.append((message, level)),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **_kwargs: None,
    )

    execute_runtime_job(
        "inspect",
        tmp_path / "project.yaml",
        build_artifact_graph([]),
        RuntimeJobPlan(job, ()),
    )

    assert len(messages) == 1
    message, level = messages[0]
    assert level == logging.DEBUG
    assert message.startswith("Config:\n")
    config = json.loads(message.removeprefix("Config:\n"))
    assert config["target"] == "report"
    assert "dataset" not in config
    assert config["execution"]["sort_buffer_mb"] == 24
    assert config["observability"]["log_level"] == "INFO"


def test_runtime_job_does_not_hide_plugin_value_errors(
    monkeypatch, tmp_path: Path
) -> None:
    task = OperationTask(id="report", entrypoint="plugin.runtime.report")
    job = _runtime_job("coverage", task, _runtime(tmp_path), _dataset())
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_project",
        lambda *_args, **_kwargs: (),
    )

    def fail(**_kwargs):
        raise ValueError("plugin bug")

    monkeypatch.setattr("datapipeline.profiles.execution.execute_operation", fail)

    with pytest.raises(ValueError, match="plugin bug"):
        execute_runtime_job(
            "inspect",
            tmp_path / "project.yaml",
            build_artifact_graph([]),
            RuntimeJobPlan(job, ()),
        )


def test_shared_serve_run_is_finalized_once(monkeypatch, tmp_path: Path) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    run_paths = _run_paths(tmp_path)
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=[
            _runtime_job(name, task, _runtime(tmp_path, name), _dataset())
            for name in ("train", "val")
        ],
        serve_run_plans=(ServeRunPlan(run_paths, None),),
    )
    calls = {"start": 0, "success": 0, "failed": 0, "latest": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_runtime_job",
        lambda *_args: None,
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


def test_job_failure_marks_shared_run_failed(monkeypatch, tmp_path: Path) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    run_paths = _run_paths(tmp_path)
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=[
            _runtime_job(name, task, _runtime(tmp_path, name), _dataset())
            for name in ("train", "val")
        ],
        serve_run_plans=(ServeRunPlan(run_paths, None),),
    )
    calls = 0
    failed: list[RunPaths] = []

    def execute(*_args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_runtime_job",
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


def test_preview_run_exists_at_job_boundary_and_is_not_latest(
    monkeypatch,
    tmp_path: Path,
) -> None:
    pipeline = PipelineTask(id="pipeline")
    run_paths = _run_paths(tmp_path)
    runtime = _runtime(tmp_path)
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=[
            _runtime_job(
                "preview",
                pipeline,
                runtime,
                _dataset(),
                preview="records",
                heartbeat_interval_seconds=15,
            )
        ],
        serve_run_plans=(ServeRunPlan(run_paths, "records"),),
    )
    observed: dict[str, object] = {}

    def execute(spec, work):
        run = json.loads(run_paths.metadata_path.read_text(encoding="utf-8"))
        observed.update(
            status=run["status"],
            preview=run["preview"],
            heartbeat=spec.runtime.heartbeat_interval_seconds,
        )
        return work()

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        execute,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_runtime_job",
        lambda *_args: None,
    )
    latest: list[RunPaths] = []
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        latest.append,
    )

    run_profiles(request)

    assert observed == {
        "status": "running",
        "preview": "records",
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
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=[_runtime_job("serve", task, _runtime(tmp_path), _dataset())],
        serve_run_plans=(ServeRunPlan(first, None), ServeRunPlan(second, None)),
    )
    starts: list[RunPaths] = []
    failed: list[RunPaths] = []

    def start(paths, *, preview):
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
