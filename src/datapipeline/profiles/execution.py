import logging
from dataclasses import dataclass
from pathlib import Path

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_project
from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.artifacts.validation import validate_artifact_plan
from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.config.build_resolution import BuildSettings
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
class ArtifactProfileTaskPlan:
    task: ArtifactTask
    artifact_key: str


@dataclass(frozen=True)
class RuntimeProfileTaskPlan:
    task: OperationTask
    required_artifacts: tuple[str, ...]


ProfileTaskPlan = ArtifactProfileTaskPlan | RuntimeProfileTaskPlan


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
        artifact_key = graph.key_for_task(task.id)
        if artifact_key is None:
            raise ValueError(f"Artifact task '{task.id}' has no artifact definition.")
        roots = {artifact_key}
        artifact_keys = set(graph.dependency_closure(roots))
        if graph.requires_dataset(artifact_keys):
            dataset = load_dataset(project_path, "vectors")
            artifact_keys = set(graph.active_dependency_closure(roots, dataset))
        validate_artifact_plan(project_path, graph, artifact_keys)
        return ArtifactProfileTaskPlan(task=task, artifact_key=artifact_key)
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


def run_selected_artifacts(
    request: ProfileRunRequest,
    profile: ExecutionProfile,
    graph: ArtifactGraph,
    required_artifacts: set[str],
    runtime_override: Runtime | None = None,
) -> None:
    if not required_artifacts:
        return
    settings = profile.build_settings
    if settings is None:
        mode = str(profile.build_mode or "AUTO").upper()
        settings = BuildSettings(
            visuals=profile.visuals,
            log_decision=profile.log_decision,
            log_output=profile.log_output,
            mode=mode,
            force=(mode == "FORCE"),
            profile_name=profile.name,
        )
    run_build_if_needed(
        request.project_path,
        required_artifacts=required_artifacts,
        artifact_graph=graph,
        settings=settings,
        skip_logging_setup=True,
        runtime_override=runtime_override,
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
        with operation_scope(
            f"{command}:{profile.name}",
            task.entrypoint,
        ):
            execute_operation(
                operation=task,
                operation_group=RUNTIME_OPERATIONS_EP,
                persist=lambda result: persist_runtime_result(
                    result,
                    target=profile.output,
                    visuals=profile.visuals,
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
) -> None:
    runtime = runtime_override if runtime_override is not None else profile.runtime

    if isinstance(task_plan, ArtifactProfileTaskPlan):
        if profile.build_settings is not None or not request.skip_build:
            run_selected_artifacts(
                request=request,
                profile=profile,
                graph=graph,
                required_artifacts={task_plan.artifact_key},
                runtime_override=runtime,
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

    if required_artifacts and (
        profile.build_settings is not None or not request.skip_build
    ):
        run_selected_artifacts(
            request=request,
            profile=profile,
            graph=graph,
            required_artifacts=set(required_artifacts),
            runtime_override=runtime,
        )

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
    "ArtifactProfileTaskPlan",
    "execute_profile",
    "plan_profile_task",
    "ProfileTaskPlan",
    "resolve_profile_task",
    "RuntimeProfileTaskPlan",
    "run_selected_artifacts",
    "run_runtime_task",
]
