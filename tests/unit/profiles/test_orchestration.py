import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.artifacts.registry import ArtifactRegistry
from datapipeline.artifacts.settings import BuildSettings
from datapipeline.artifacts.specs import (
    VECTOR_INPUTS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
)
from datapipeline.build.state import (
    ArtifactFileFingerprint,
    BuildState,
    save_build_state,
)
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.config.streams import StreamsConfig
from datapipeline.config.tasks import (
    ArtifactTask,
    CoverageTask,
    MatrixTask,
    MetadataTask,
    OperationTask,
    PipelineTask,
    SchemaTask,
    TicksTask,
    VectorInputsTask,
)
from datapipeline.execution.settings import (
    LogLevelDecision,
    LogOutputSettings,
    ObservabilitySettings,
)
from datapipeline.io.output import OutputTarget
from datapipeline.io.runs import RunPaths
from datapipeline.profiles.execution import (
    RuntimeJobPlan,
    execute_runtime_job,
    plan_runtime_job,
    run_runtime_operation,
)
from datapipeline.profiles.executor import ExecutionSpec
from datapipeline.profiles.models import (
    BuildJob,
    BuildRunRequest,
    MaterializeJob,
    MaterializeRunRequest,
    RuntimeJob,
    RuntimeRunRequest,
    ServeRunPlan,
)
from datapipeline.profiles.orchestration import _validate_build_order, run_profiles
from tests.unit.profiles.helpers import pipeline_definition

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


def _dataset(*, scale: bool = False) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(
                id="feature",
                stream="stream",
                field="value",
                scale=scale,
            )
        ],
    )


def _stream_catalog() -> StreamsConfig:
    return StreamsConfig.model_validate(
        {
            "sources": {
                "test.source": {
                    "id": "test.source",
                    "parser": {"entrypoint": "identity"},
                    "loader": {"entrypoint": "identity"},
                }
            },
            "streams": {
                stream_id: {
                    "id": stream_id,
                    "from": {"source": "test.source"},
                    "map": {"entrypoint": "identity"},
                }
                for stream_id in ("stream", "prices")
            },
        }
    )


def _runtime(tmp_path: Path, marker: str = "runtime") -> SimpleNamespace:
    return SimpleNamespace(
        marker=marker,
        artifacts=ArtifactRegistry(tmp_path / marker / "artifacts"),
        dataset=_dataset(),
        execution=ExecutionConfig(),
        heartbeat_interval_seconds=None,
        output_splits=(),
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
    *,
    limit: int | None = None,
    preview: PreviewStage | None = None,
    heartbeat_interval_seconds: float | None = None,
    output_splits: tuple[str, ...] = (),
) -> RuntimeJob:
    return RuntimeJob(
        name=name,
        task=task,
        runtime=runtime,
        output=_output(),
        observability=_observability(heartbeat_interval_seconds),
        limit=limit,
        throttle_ms=None,
        preview=preview,
        output_splits=output_splits,
    )


def _build_request(
    tmp_path: Path,
    artifact_tasks,
    jobs,
    execution: ExecutionConfig | None = None,
) -> BuildRunRequest:
    return BuildRunRequest(
        definition=pipeline_definition(
            tmp_path / "project.yaml",
            dataset=_dataset(),
            streams=_stream_catalog(),
            artifact_operations=artifact_tasks,
        ),
        jobs=jobs,
        execution=ExecutionConfig() if execution is None else execution,
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
        definition=pipeline_definition(
            tmp_path / "project.yaml",
            dataset=_dataset(),
            streams=_stream_catalog(),
            artifact_operations=artifact_tasks,
        ),
        jobs=jobs,
        execution=ExecutionConfig() if execution is None else execution,
        artifact_settings=artifact_settings or _artifact_settings(),
        serve_run_plans=serve_run_plans,
    )


def _materialize_request(
    tmp_path: Path,
    artifact_tasks,
    jobs,
    runtime,
    execution: ExecutionConfig | None = None,
) -> MaterializeRunRequest:
    return MaterializeRunRequest(
        definition=pipeline_definition(
            tmp_path / "project.yaml",
            dataset=_dataset(),
            streams=_stream_catalog(),
            artifact_operations=artifact_tasks,
        ),
        jobs=jobs,
        execution=ExecutionConfig() if execution is None else execution,
        artifact_settings=_artifact_settings(),
        runtime=runtime,
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


def test_build_order_rejects_duplicate_operations() -> None:
    schema = SchemaTask(id="schema")

    with pytest.raises(ValueError, match="unique artifact operations"):
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
        "datapipeline.profiles.orchestration.compile_runtime",
        lambda _definition: next(runtimes),
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
    pipeline = PipelineTask(id="dataset")
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
                preview="samples",
            ),
            _runtime_job(
                "postprocess-preview",
                pipeline,
                second_runtime,
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
        "datapipeline.profiles.orchestration.compile_runtime",
        lambda _definition: canonical_runtime,
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
        jobs=[_runtime_job("report", report, _runtime(tmp_path))],
    )
    build_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.compile_runtime",
        lambda _definition: _runtime(tmp_path, "artifact-build"),
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
        jobs=[_runtime_job("report", report, _runtime(tmp_path))],
    )

    _assert_preflight_rejected(request)


def test_missing_runtime_artifact_producer_is_rejected_before_execution(
    tmp_path: Path,
) -> None:
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    pipeline = PipelineTask(id="dataset")
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[schema, metadata],
        jobs=[_runtime_job("serve", pipeline, _runtime(tmp_path))],
    )

    _assert_preflight_rejected(request)


def test_invalid_preview_is_rejected_before_starting_run(tmp_path: Path) -> None:
    pipeline = PipelineTask(id="dataset")
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
    task = PipelineTask(id="dataset")
    execution = ExecutionConfig(sort_buffer_mb=16)
    jobs = [
        _runtime_job(
            name,
            task,
            _runtime(tmp_path, name),
            heartbeat_interval_seconds=heartbeat,
            output_splits=output_splits,
        )
        for name, heartbeat, output_splits in (
            ("first", 10, ("train",)),
            ("second", 20, ("val",)),
            ("third", None, ()),
        )
    ]
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[
            VectorInputsTask(id="vector_inputs"),
            MetadataTask(id="metadata"),
            SchemaTask(id="schema"),
        ],
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
                runtime.output_splits,
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
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration._prepare_runtime_artifacts",
        lambda *_args: None,
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
    job = _runtime_job("coverage", task, runtime)
    messages: list[tuple[str, int]] = []

    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_pipeline",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.emit_execution_message",
        lambda message, level, logger: messages.append((message, level)),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.load_operation_runner",
        lambda *_args: lambda *_runner_args: None,
    )

    execute_runtime_job(
        "inspect",
        pipeline_definition(tmp_path / "project.yaml", dataset=runtime.dataset),
        build_artifact_graph([]),
        RuntimeJobPlan(job, ()),
    )

    assert len(messages) == 1
    message, level = messages[0]
    assert level == logging.DEBUG
    assert message.startswith("Config:\n")
    config = json.loads(message.removeprefix("Config:\n"))
    assert config["operation"]["id"] == "report"
    assert config["operation"]["entrypoint"] == "plugin.runtime.report"
    assert "dataset" not in config
    assert config["execution"]["sort_buffer_mb"] == 24
    assert config["observability"]["log_level"] == "INFO"


def test_runtime_job_does_not_hide_plugin_value_errors(
    monkeypatch, tmp_path: Path
) -> None:
    task = OperationTask(id="report", entrypoint="plugin.runtime.report")
    job = _runtime_job("coverage", task, _runtime(tmp_path))
    monkeypatch.setattr(
        "datapipeline.profiles.execution.hydrate_runtime_artifacts_for_pipeline",
        lambda *_args, **_kwargs: (),
    )

    def fail(*_args):
        def run(*_runner_args):
            raise ValueError("plugin bug")

        return run

    monkeypatch.setattr("datapipeline.profiles.execution.load_operation_runner", fail)

    with pytest.raises(ValueError, match="plugin bug"):
        execute_runtime_job(
            "inspect",
            pipeline_definition(tmp_path / "project.yaml"),
            build_artifact_graph([]),
            RuntimeJobPlan(job, ()),
        )


def test_runtime_plugin_receives_the_documented_contract(
    monkeypatch, tmp_path: Path
) -> None:
    task = OperationTask(id="report", entrypoint="plugin.runtime.report")
    job = _runtime_job("report", task, _runtime(tmp_path), limit=7)
    received = None

    def load_runner(operation, group):
        assert operation is task
        assert group == "datapipeline.operations.runtime"

        def run(runtime, operation_task, limit):
            nonlocal received
            received = runtime, operation_task, limit
            return "result"

        return run

    monkeypatch.setattr(
        "datapipeline.profiles.execution.load_operation_runner",
        load_runner,
    )

    assert run_runtime_operation(job) == "result"
    assert received == (job.runtime, task, 7)


def test_matrix_operation_uses_its_core_runner(monkeypatch, tmp_path: Path) -> None:
    task = MatrixTask(id="matrix")
    job = _runtime_job("matrix", task, _runtime(tmp_path), limit=5)
    received = None

    def run_matrix(runtime, operation_task, limit):
        nonlocal received
        received = runtime, operation_task, limit
        return "matrix"

    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_matrix_operation",
        run_matrix,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.load_operation_runner",
        lambda *_args: pytest.fail("core operations must not load plugin entry points"),
    )

    assert run_runtime_operation(job) == "matrix"
    assert received == (job.runtime, task, 5)


def test_coverage_operation_rejects_limit_before_planning(tmp_path: Path) -> None:
    job = _runtime_job(
        "coverage",
        CoverageTask(id="coverage"),
        _runtime(tmp_path),
        limit=1,
    )

    with pytest.raises(ValueError, match="coverage operation does not support"):
        plan_runtime_job(job, SimpleNamespace(), SimpleNamespace())


def test_shared_serve_run_is_finalized_once(monkeypatch, tmp_path: Path) -> None:
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime")
    run_paths = _run_paths(tmp_path)
    request = _runtime_request(
        tmp_path,
        command="serve",
        artifact_tasks=[],
        jobs=[
            _runtime_job(name, task, _runtime(tmp_path, name))
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
            _runtime_job(name, task, _runtime(tmp_path, name))
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
    pipeline = PipelineTask(id="dataset")
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
        jobs=[_runtime_job("serve", task, _runtime(tmp_path))],
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


def test_materialize_uses_shared_artifact_and_execution_lifecycle(
    monkeypatch,
    tmp_path: Path,
) -> None:
    ticks = TicksTask(id="market_ticks", stream="prices", output="ticks.jsonl")
    execution = ExecutionConfig(sort_buffer_mb=32)
    runtime = SimpleNamespace(
        execution=ExecutionConfig(),
        heartbeat_interval_seconds=None,
        streams={"adv.20": object(), "adv.63": object()},
        artifacts_root=tmp_path / "artifacts",
    )
    jobs = [
        MaterializeJob(
            name="adv-20",
            stream="adv.20",
            output=tmp_path / "adv-20.jsonl",
            overwrite=False,
            observability=_observability(10),
        ),
        MaterializeJob(
            name="adv-63",
            stream="adv.63",
            output=tmp_path / "adv-63.jsonl",
            overwrite=False,
            observability=_observability(20),
        ),
    ]
    request = _materialize_request(
        tmp_path,
        [ticks],
        jobs,
        runtime,
        execution,
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.planning.stream_tick_artifacts",
        lambda stream, streams: {"market_ticks"},
    )
    build_calls: list[dict] = []
    materialized: list[tuple[str, float | None]] = []

    def build(project_path, **kwargs):
        assert runtime.execution == execution
        build_calls.append(kwargs)

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_build_if_needed",
        build,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_materialize_job",
        lambda job, active_runtime: materialized.append(
            (job.name, active_runtime.heartbeat_interval_seconds)
        ),
    )

    run_profiles(request)

    assert len(build_calls) == 1
    assert build_calls[0]["required_artifacts"] == {"market_ticks"}
    assert materialized == [("adv-20", 10), ("adv-63", 20)]


@pytest.mark.parametrize("mode", ["AUTO", "OFF"])
def test_materialize_hydrates_current_tick_artifact_when_build_skips(
    monkeypatch,
    tmp_path: Path,
    mode: str,
) -> None:
    ticks = TicksTask(id="market_ticks", stream="prices", output="ticks.jsonl")
    job = MaterializeJob(
        name="adv-20",
        stream="adv.20",
        output=tmp_path / "adv-20.jsonl",
        overwrite=False,
        observability=_observability(),
    )
    definition = pipeline_definition(
        tmp_path / "project.yaml",
        dataset=_dataset(),
        streams=_stream_catalog(),
        artifact_operations=[ticks],
    )
    artifacts_root = definition.project.artifacts_root
    artifact_path = artifacts_root / ticks.output
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_text("{}\n", encoding="utf-8")
    state = BuildState()
    state.register(
        ticks.id,
        ticks.output,
        artifact_hash=definition.artifact_hashes.for_artifact(ticks.id),
        files=(ArtifactFileFingerprint.from_path(ticks.output, artifact_path),),
    )
    save_build_state(
        state,
        artifacts_root / "_system" / "build" / "state.json",
    )
    runtime = SimpleNamespace(
        execution=ExecutionConfig(),
        heartbeat_interval_seconds=None,
        streams={"adv.20": object()},
        artifacts=ArtifactRegistry(artifacts_root),
        artifacts_root=artifacts_root,
    )
    request = MaterializeRunRequest(
        definition=definition,
        jobs=[job],
        execution=ExecutionConfig(),
        artifact_settings=_artifact_settings(mode),
        runtime=runtime,
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.planning.stream_tick_artifacts",
        lambda stream, streams: {"market_ticks"},
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_execution",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_materialize_job",
        lambda job, active_runtime: active_runtime.artifacts.require("market_ticks"),
    )

    run_profiles(request)

    assert runtime.artifacts.has("market_ticks")


@pytest.mark.parametrize(
    ("artifact_tasks", "message"),
    [
        ([], "requires a declared ticks task"),
        (
            [
                ArtifactTask(
                    id="market_ticks",
                    entrypoint="plugin.snapshot",
                    output="snapshot.json",
                )
            ],
            "not a ticks task",
        ),
    ],
)
def test_materialize_rejects_invalid_tick_artifact_producer(
    monkeypatch,
    tmp_path: Path,
    artifact_tasks,
    message,
) -> None:
    runtime = SimpleNamespace(
        execution=ExecutionConfig(),
        heartbeat_interval_seconds=None,
        streams={"adv.20": object()},
        artifacts_root=tmp_path / "artifacts",
    )
    job = MaterializeJob(
        name="adv-20",
        stream="adv.20",
        output=tmp_path / "adv-20.jsonl",
        overwrite=False,
        observability=_observability(),
    )
    request = _materialize_request(tmp_path, artifact_tasks, [job], runtime)
    monkeypatch.setattr(
        "datapipeline.artifacts.planning.stream_tick_artifacts",
        lambda stream, streams: {"market_ticks"},
    )

    with pytest.raises(SystemExit, match="2"):
        run_profiles(request)
