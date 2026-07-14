import logging

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.artifacts.planning import ArtifactGraph, build_artifact_graph
from datapipeline.artifacts.validation import stream_tick_artifacts
from datapipeline.config.tasks import TicksTask, VectorInputsTask
from datapipeline.profiles.executor import ExecutionSpec, run_execution
from datapipeline.profiles.materialize import (
    execute_materialize_job,
    preflight_materialize_jobs,
)
from datapipeline.services.execution_lock import (
    ProjectExecutionBusyError,
    project_execution_lock,
)
from datapipeline.services.runs import (
    finish_run_failed,
    finish_run_success,
    set_latest_run,
    start_run,
)
from datapipeline.services.runtime_compiler import compile_runtime
from datapipeline.vector_inputs.store import prune_vector_input_cache

from .execution import (
    RuntimeJobPlan,
    execute_runtime_job,
    plan_runtime_job,
    validate_build_job,
)
from .models import (
    BuildJob,
    BuildRunRequest,
    MaterializeRunRequest,
    ProfileRunRequest,
    RuntimeRunRequest,
    ServeRunPlan,
)

logger = logging.getLogger(__name__)


def run_profiles(request: ProfileRunRequest) -> None:
    try:
        with project_execution_lock(request.definition.project.artifacts_root):
            if isinstance(request, BuildRunRequest):
                _run_build_profiles(request)
            elif isinstance(request, RuntimeRunRequest):
                _run_runtime_profiles(request)
            else:
                _run_materialize_profiles(request)
            _prune_vector_input_caches(request)
    except ProjectExecutionBusyError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc


def _prune_vector_input_caches(request: ProfileRunRequest) -> None:
    root = request.definition.project.artifacts_root
    for task in request.definition.artifact_operations:
        if isinstance(task, VectorInputsTask):
            prune_vector_input_cache(root / task.output)


def _run_build_profiles(request: BuildRunRequest) -> None:
    jobs = list(request.jobs)
    if not jobs:
        return
    try:
        graph = build_artifact_graph(
            request.definition.artifact_operations,
            request.definition.dataset,
            request.definition.streams,
        )
        _validate_build_order(jobs, graph)
        for job in jobs:
            validate_build_job(job.task, graph, request.definition)
    except (OSError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    resolved_artifacts: set[str] = set()
    for job in jobs:
        runtime = compile_runtime(request.definition)
        runtime.execution = request.execution
        spec = ExecutionSpec(
            observability=job.settings.observability,
            runtime=runtime,
        )

        def build(job=job, runtime=runtime) -> None:
            run_build_if_needed(
                request.definition,
                graph=graph,
                required_artifacts={job.task.id},
                settings=job.settings,
                runtime=runtime,
                resolved_artifacts=resolved_artifacts,
            )

        run_execution(spec, build)


def _run_runtime_profiles(request: RuntimeRunRequest) -> None:
    jobs = list(request.jobs)
    if not jobs:
        return
    try:
        graph = build_artifact_graph(
            request.definition.artifact_operations,
            request.definition.dataset,
            request.definition.streams,
        )
        plans = [plan_runtime_job(job, graph, request.definition) for job in jobs]
    except (OSError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    started_runs: list[ServeRunPlan] = []
    succeeded = False
    try:
        _prepare_runtime_artifacts(request, graph, plans)
        for run_plan in request.serve_run_plans:
            start_run(run_plan.paths, preview=run_plan.preview)
            started_runs.append(run_plan)

        for plan in plans:
            job = plan.job
            job.runtime.execution = request.execution
            job.runtime.heartbeat_interval_seconds = (
                job.observability.heartbeat_interval_seconds
            )
            job.runtime.split_labels = job.splits
            spec = ExecutionSpec(
                observability=job.observability,
                runtime=job.runtime,
            )

            def execute(plan=plan) -> None:
                execute_runtime_job(
                    request.command,
                    request.definition,
                    graph,
                    plan,
                )

            run_execution(spec, execute)
        succeeded = True
    finally:
        _finalize_serve_runs(started_runs, succeeded)


def _run_materialize_profiles(request: MaterializeRunRequest) -> None:
    jobs = list(request.jobs)
    if not jobs:
        return
    request.runtime.execution = request.execution
    try:
        preflight_materialize_jobs(request.runtime, jobs)
        graph = build_artifact_graph(
            request.definition.artifact_operations,
            request.definition.dataset,
            request.definition.streams,
        )
        required_artifacts = {
            artifact
            for job in jobs
            for artifact in stream_tick_artifacts(
                job.stream, request.definition.streams
            )
        }
        for artifact in sorted(required_artifacts):
            task = graph.tasks_by_id.get(artifact)
            if task is None:
                raise ValueError(
                    f"Tick artifact '{artifact}' requires a declared ticks "
                    "operation with the same id."
                )
            if not isinstance(task, TicksTask):
                raise ValueError(
                    f"Tick artifact '{artifact}' references operation "
                    f"entrypoint '{task.entrypoint}', not a ticks operation."
                )
    except (FileExistsError, OSError, RuntimeError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    _prepare_materialize_artifacts(request, graph, required_artifacts)
    for job in jobs:
        request.runtime.heartbeat_interval_seconds = (
            job.observability.heartbeat_interval_seconds
        )
        spec = ExecutionSpec(
            observability=job.observability,
            runtime=request.runtime,
        )

        def execute(job=job) -> None:
            execute_materialize_job(job, request.runtime)

        run_execution(spec, execute)


def _validate_build_order(jobs: list[BuildJob], graph: ArtifactGraph) -> None:
    targets = [job.task.id for job in jobs]
    if len(targets) != len(set(targets)):
        raise ValueError("Build profiles must have unique artifact targets.")

    positions = {target: index for index, target in enumerate(targets)}
    for target, position in positions.items():
        for dependency in graph.dependency_closure({target}):
            dependency_position = positions.get(dependency)
            if dependency_position is not None and dependency_position > position:
                raise ValueError(
                    f"Build profile target '{dependency}' must be ordered before "
                    f"dependent target '{target}'."
                )


def _finalize_serve_runs(plans: list[ServeRunPlan], succeeded: bool) -> None:
    for plan in plans:
        if not succeeded:
            finish_run_failed(plan.paths)
            continue
        finish_run_success(plan.paths)
        if plan.preview is None:
            set_latest_run(plan.paths)


def _prepare_runtime_artifacts(
    request: RuntimeRunRequest,
    graph: ArtifactGraph,
    plans: list[RuntimeJobPlan],
) -> None:
    required_artifacts = {
        artifact for plan in plans for artifact in plan.required_artifacts
    }
    if not required_artifacts:
        return

    settings = request.artifact_settings
    runtime = compile_runtime(request.definition)
    runtime.execution = request.execution
    spec = ExecutionSpec(
        observability=settings.observability,
        runtime=runtime,
    )

    def prepare() -> None:
        run_build_if_needed(
            request.definition,
            graph=graph,
            required_artifacts=required_artifacts,
            settings=settings,
            runtime=runtime,
        )

    run_execution(spec, prepare)


def _prepare_materialize_artifacts(
    request: MaterializeRunRequest,
    graph: ArtifactGraph,
    required_artifacts: set[str],
) -> None:
    if not required_artifacts:
        return
    spec = ExecutionSpec(
        observability=request.artifact_settings.observability,
        runtime=request.runtime,
    )

    def prepare() -> None:
        run_build_if_needed(
            request.definition,
            graph=graph,
            required_artifacts=required_artifacts,
            settings=request.artifact_settings,
            runtime=request.runtime,
        )

    run_execution(spec, prepare)
