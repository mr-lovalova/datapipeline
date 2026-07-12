import logging
from dataclasses import dataclass
from pathlib import Path

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_project
from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.artifacts.validation import validate_artifact_plan
from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.tasks import ArtifactTask, OperationTask, Task
from datapipeline.execution.observability import operation_scope
from datapipeline.operations.dispatch import execute_operation
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.plugins import RUNTIME_OPERATIONS_EP
from datapipeline.runtime import Runtime

from .models import ExecutionProfile, ProfileRunRequest

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeProfileTaskPlan:
    task: OperationTask
    required_artifacts: tuple[str, ...]


ProfileTaskPlan = ArtifactTask | RuntimeProfileTaskPlan


def resolve_profile_task(
    profile: ExecutionProfile,
    tasks_by_id: dict[str, Task],
) -> Task:
    try:
        return tasks_by_id[profile.target_id]
    except KeyError as exc:
        raise ValueError(f"Unknown task target '{profile.target_id}'") from exc


def plan_profile_task(
    profile: ExecutionProfile,
    task: Task,
    graph: ArtifactGraph,
    project_path: Path,
) -> ProfileTaskPlan:
    if isinstance(task, ArtifactTask):
        roots = {task.id}
        artifact_keys = set(graph.dependency_closure(roots))
        if graph.requires_dataset(artifact_keys):
            dataset = load_dataset(project_path, "vectors")
            artifact_keys = set(graph.active_dependency_closure(roots, dataset))
        validate_artifact_plan(project_path, graph, artifact_keys)
        return task
    if not isinstance(task, OperationTask):
        raise TypeError(f"Unsupported task type: {type(task).__name__}")
    if profile.dataset is None:
        raise ValueError(
            f"Runtime profile '{profile.name}' is missing dataset context."
        )

    feature_dataset = (
        profile.dataset if isinstance(profile.dataset, FeatureDatasetConfig) else None
    )
    runtime_artifacts = graph.runtime_dependency_closure(
        task,
        preview_index=profile.preview_index,
        dataset=feature_dataset,
    )
    validate_artifact_plan(project_path, graph, set(runtime_artifacts))
    return RuntimeProfileTaskPlan(
        task=task,
        required_artifacts=runtime_artifacts,
    )


def run_build_profile(
    request: ProfileRunRequest,
    profile: ExecutionProfile,
    graph: ArtifactGraph,
    task: ArtifactTask,
    runtime_override: Runtime | None = None,
    resolved_artifacts: set[str] | None = None,
    expected_config_hash: str | None = None,
) -> None:
    settings = profile.build_settings
    if settings is None:
        raise ValueError(
            f"Build profile '{profile.name}' has no resolved build settings."
        )
    if runtime_override is None:
        raise ValueError(f"Build profile '{profile.name}' has no build runtime.")
    run_build_if_needed(
        request.project_path,
        graph=graph,
        required_artifacts={task.id},
        mode=settings.mode,
        runtime=runtime_override,
        profile_name=profile.name,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        resolved_artifacts=resolved_artifacts,
        expected_config_hash=expected_config_hash,
    )


def run_runtime_task(
    task: OperationTask,
    profile: ExecutionProfile,
    runtime: Runtime,
    command: str = "runtime",
) -> None:
    if profile.dataset is None:
        raise ValueError(
            f"Runtime profile '{profile.name}' is missing dataset context."
        )
    try:
        with operation_scope(f"{command}:{profile.name}"):
            execute_operation(
                operation=task,
                operation_group=RUNTIME_OPERATIONS_EP,
                persist=lambda result: persist_runtime_result(
                    result,
                    target=profile.output,
                    heartbeat_interval_seconds=runtime.heartbeat_interval_seconds,
                    logger=logger,
                ),
                operation_task=task,
                runtime=runtime,
                dataset=profile.dataset,
                limit=profile.limit,
                target=profile.output,
                throttle_ms=profile.throttle_ms,
                preview_index=profile.preview_index,
                visuals=profile.visuals,
            )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc


def execute_profile(
    profile: ExecutionProfile,
    request: ProfileRunRequest,
    task_plan: ProfileTaskPlan,
    graph: ArtifactGraph,
    runtime_override: Runtime | None = None,
    resolved_artifacts: set[str] | None = None,
    expected_config_hash: str | None = None,
) -> None:
    runtime = runtime_override if runtime_override is not None else profile.runtime

    if isinstance(task_plan, ArtifactTask):
        run_build_profile(
            request=request,
            profile=profile,
            graph=graph,
            task=task_plan,
            runtime_override=runtime,
            resolved_artifacts=resolved_artifacts,
            expected_config_hash=expected_config_hash,
        )
        return

    task = task_plan.task
    if runtime is None:
        logger.error(
            "Profile '%s' resolves runtime task '%s' but has no runtime context.",
            profile.name,
            task.id,
        )
        raise SystemExit(2)

    feature_dataset = (
        profile.dataset if isinstance(profile.dataset, FeatureDatasetConfig) else None
    )
    required_artifacts = task_plan.required_artifacts

    current_artifacts = set(
        hydrate_runtime_artifacts_for_project(
            runtime,
            request.project_path,
            graph=graph,
            dataset=feature_dataset,
        )
    )
    unavailable = [key for key in required_artifacts if key not in current_artifacts]
    if unavailable:
        logger.error(
            "Runtime task '%s' requires missing or stale artifacts: %s.",
            task.id,
            ", ".join(unavailable),
        )
        raise SystemExit(2)

    run_runtime_task(task, profile, runtime, command=request.command)


__all__ = [
    "execute_profile",
    "plan_profile_task",
    "ProfileTaskPlan",
    "resolve_profile_task",
    "RuntimeProfileTaskPlan",
    "run_build_profile",
    "run_runtime_task",
]
