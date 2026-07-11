import logging

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.artifacts.planning import ArtifactGraph, build_artifact_graph
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.config.tasks import ArtifactTask, OperationTask, Task
from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.services.bootstrap import bootstrap_build_runtime
from datapipeline.services.project_paths import tasks_dir
from datapipeline.services.runs import (
    finish_run_failed,
    finish_run_success,
    set_latest_run,
    start_run,
)
from .execution import (
    ProfileTaskPlan,
    RuntimeProfileTaskPlan,
    execute_profile,
    plan_profile_task,
    resolve_profile_task,
)
from .models import ProfileRunRequest, ServeRunPlan

logger = logging.getLogger(__name__)


def run_profiles(request: ProfileRunRequest) -> None:
    tasks_by_id = {task.id: task for task in request.tasks}
    profiles = list(request.profiles)
    if not profiles:
        return
    try:
        if (
            compute_config_hash(request.project_path, tasks_dir(request.project_path))
            != request.config_hash
        ):
            raise ValueError(
                "Pipeline inputs changed after profiles were resolved; rerun the command."
            )
        graph = build_artifact_graph(request.artifact_task_configs)
        profile_tasks = [
            resolve_profile_task(profile, tasks_by_id) for profile in profiles
        ]
        if request.command == "build":
            _validate_build_profile_order(profile_tasks, graph)
        elif any(not isinstance(task, OperationTask) for task in profile_tasks):
            raise ValueError(
                f"{request.command.capitalize()} profiles must target runtime tasks."
            )
        profile_task_plans = [
            plan_profile_task(
                profile,
                task,
                graph,
                request.project_path,
            )
            for profile, task in zip(profiles, profile_tasks)
        ]
        profile_runtimes = []
        for profile in profiles:
            if profile.runtime is not None:
                profile_runtimes.append(profile.runtime)
            else:
                profile_runtimes.append(bootstrap_build_runtime(request.project_path))
    except (OSError, TypeError, ValueError) as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    started_serve_runs: list[ServeRunPlan] = []
    resolved_artifacts: set[str] | None = set() if request.command == "build" else None
    succeeded = False
    try:
        if request.command != "build":
            _prepare_runtime_artifacts(
                request,
                graph,
                profile_task_plans,
            )
        for plan in request.serve_run_plans:
            start_run(plan.paths, preview_index=plan.preview_index)
            started_serve_runs.append(plan)

        total = len(profiles)
        for idx, profile in enumerate(profiles, start=1):
            runtime = profile_runtimes[idx - 1]
            runtime.heartbeat_interval_seconds = profile.heartbeat_interval_seconds
            spec = ProfileExecutionSpec(
                command=request.command,
                name=profile.name,
                index=idx,
                total=total,
                visuals=profile.visuals,
                log_decision=profile.log_decision,
                log_output=profile.log_output,
                runtime=runtime,
            )

            task_plan = profile_task_plans[idx - 1]

            def work(profile=profile, runtime=runtime, task_plan=task_plan):
                scope_target = profile.target_id
                with execution_scope(
                    profile_kind=request.command,
                    profile_name=profile.name,
                    target_id=scope_target,
                ):
                    return execute_profile(
                        profile=profile,
                        request=request,
                        task_plan=task_plan,
                        graph=graph,
                        runtime_override=runtime,
                        resolved_artifacts=resolved_artifacts,
                        expected_config_hash=request.config_hash,
                    )

            run_profile(spec=spec, work=work)
        succeeded = True
    finally:
        _finalize_serve_runs(started_serve_runs, succeeded)


def _finalize_serve_runs(
    plans: list[ServeRunPlan],
    succeeded: bool,
) -> None:
    for plan in plans:
        if not succeeded:
            finish_run_failed(plan.paths)
            continue
        finish_run_success(plan.paths)
        if plan.preview_index is None:
            set_latest_run(plan.paths)


def _validate_build_profile_order(
    tasks: list[Task],
    graph: ArtifactGraph,
) -> None:
    targets: list[str] = []
    for task in tasks:
        if not isinstance(task, ArtifactTask):
            raise ValueError("Build profiles must target artifact tasks.")
        targets.append(task.id)
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


def _prepare_runtime_artifacts(
    request: ProfileRunRequest,
    graph: ArtifactGraph,
    plans: list[ProfileTaskPlan],
) -> None:
    required_artifacts = {
        artifact
        for plan in plans
        if isinstance(plan, RuntimeProfileTaskPlan)
        for artifact in plan.required_artifacts
    }
    if not required_artifacts:
        return
    if request.artifact_mode is None:
        raise ValueError("Runtime profiles require a resolved artifact_mode.")
    run_build_if_needed(
        request.project_path,
        graph=graph,
        required_artifacts=required_artifacts,
        mode=request.artifact_mode,
        runtime=bootstrap_build_runtime(request.project_path),
        profile_name=request.command,
        heartbeat_interval_seconds=request.artifact_heartbeat_interval_seconds,
        expected_config_hash=request.config_hash,
    )
