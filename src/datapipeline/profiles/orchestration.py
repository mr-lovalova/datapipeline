import logging
from pathlib import Path

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.artifacts.planning import ArtifactGraph, build_artifact_graph
from datapipeline.artifacts.validation import stream_tick_artifacts
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.config.tasks import TicksTask
from datapipeline.profiles.executor import ExecutionSpec, run_execution
from datapipeline.profiles.materialize import (
    execute_materialize_job,
    preflight_materialize_jobs,
)
from datapipeline.services.bootstrap import bootstrap_build_runtime
from datapipeline.services.bootstrap.core import load_streams
from datapipeline.services.project_paths import tasks_dir
from datapipeline.services.runs import (
    finish_run_failed,
    finish_run_success,
    set_latest_run,
    start_run,
)

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
    if isinstance(request, BuildRunRequest):
        _run_build_profiles(request)
    elif isinstance(request, RuntimeRunRequest):
        _run_runtime_profiles(request)
    else:
        _run_materialize_profiles(request)


def _run_build_profiles(request: BuildRunRequest) -> None:
    jobs = list(request.jobs)
    if not jobs:
        return
    try:
        _verify_config_hash(request.project_path, request.config_hash)
        graph = build_artifact_graph(request.artifact_task_configs)
        _validate_build_order(jobs, graph)
        for job in jobs:
            validate_build_job(job.task, graph, request.project_path)
    except (OSError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    resolved_artifacts: set[str] = set()
    for job in jobs:
        runtime = bootstrap_build_runtime(request.project_path)
        runtime.execution = request.execution
        spec = ExecutionSpec(
            observability=job.settings.observability,
            runtime=runtime,
        )

        def build(job=job, runtime=runtime) -> None:
            run_build_if_needed(
                request.project_path,
                graph=graph,
                required_artifacts={job.task.id},
                settings=job.settings,
                runtime=runtime,
                resolved_artifacts=resolved_artifacts,
                expected_config_hash=request.config_hash,
            )

        run_execution(spec, build)


def _run_runtime_profiles(request: RuntimeRunRequest) -> None:
    jobs = list(request.jobs)
    if not jobs:
        return
    try:
        _verify_config_hash(request.project_path, request.config_hash)
        graph = build_artifact_graph(request.artifact_task_configs)
        plans = [plan_runtime_job(job, graph, request.project_path) for job in jobs]
    except (OSError, ValueError) as exc:
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
                    request.project_path,
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
        _verify_config_hash(request.project_path, request.config_hash)
        preflight_materialize_jobs(request.runtime, jobs)
        graph = build_artifact_graph(request.artifact_task_configs)
        streams = load_streams(request.project_path)
        required_artifacts = {
            artifact
            for job in jobs
            for artifact in stream_tick_artifacts(job.stream, streams)
        }
        for artifact in sorted(required_artifacts):
            task = graph.tasks_by_id.get(artifact)
            if task is None:
                raise ValueError(
                    f"Tick artifact '{artifact}' requires a declared ticks "
                    "task with the same id."
                )
            if not isinstance(task, TicksTask):
                raise ValueError(
                    f"Tick artifact '{artifact}' references task "
                    f"entrypoint '{task.entrypoint}', not a ticks task."
                )
    except (FileExistsError, OSError, ValueError) as exc:
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


def _verify_config_hash(project_path: Path, expected_hash: str) -> None:
    if compute_config_hash(project_path, tasks_dir(project_path)) != expected_hash:
        raise ValueError(
            "Pipeline inputs changed after profiles were resolved; rerun the command."
        )


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
    runtime = bootstrap_build_runtime(request.project_path)
    runtime.execution = request.execution
    spec = ExecutionSpec(
        observability=settings.observability,
        runtime=runtime,
    )

    def prepare() -> None:
        run_build_if_needed(
            request.project_path,
            graph=graph,
            required_artifacts=required_artifacts,
            settings=settings,
            runtime=runtime,
            expected_config_hash=request.config_hash,
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
            request.project_path,
            graph=graph,
            required_artifacts=required_artifacts,
            settings=request.artifact_settings,
            runtime=request.runtime,
            expected_config_hash=request.config_hash,
        )

    run_execution(spec, prepare)
