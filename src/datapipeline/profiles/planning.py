from typing import Sequence

from datapipeline.artifacts.planning import build_planning_context
from datapipeline.artifacts.specs import artifact_keys_for_task_ids
from datapipeline.config.tasks import ArtifactTask, OperationTask, Task

from .models import BaseExecutionProfile, ProfileRunRequest


def target_ids_for_profile(
    profile: BaseExecutionProfile,
) -> list[str]:
    return [profile.target_id]


def ordered_task_ids(
    target_ids: Sequence[str],
    tasks_by_id: dict[str, Task],
) -> list[str]:
    ordered: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> None:
        if task_id in visited:
            return
        if task_id in visiting:
            raise ValueError(f"Dependency cycle detected at task '{task_id}'")
        task = tasks_by_id.get(task_id)
        if task is None:
            raise ValueError(f"Unknown task dependency '{task_id}'")
        visiting.add(task_id)
        for dependency_id in task.dependencies:
            visit(dependency_id)
        visiting.remove(task_id)
        visited.add(task_id)
        ordered.append(task_id)

    for target_id in target_ids:
        visit(target_id)
    return ordered


def resolve_task_order(
    profile: BaseExecutionProfile,
    tasks_by_id: dict[str, Task],
) -> list[str]:
    return ordered_task_ids(target_ids_for_profile(profile), tasks_by_id)


def artifact_task_ids_for_order(
    ordered_ids: Sequence[str],
    tasks_by_id: dict[str, Task],
) -> list[str]:
    return [task_id for task_id in ordered_ids if isinstance(tasks_by_id[task_id], ArtifactTask)]


def runtime_task_ids_for_order(
    ordered_ids: Sequence[str],
    tasks_by_id: dict[str, Task],
) -> list[str]:
    return [task_id for task_id in ordered_ids if isinstance(tasks_by_id[task_id], OperationTask)]


def required_artifacts_for_task_ids(
    request: ProfileRunRequest,
    artifact_task_ids: Sequence[str],
) -> set[str]:
    context = build_planning_context(request.artifact_task_configs)
    return set(
        artifact_keys_for_task_ids(
            set(artifact_task_ids),
            context.definitions,
        )
    )


__all__ = [
    "artifact_task_ids_for_order",
    "required_artifacts_for_task_ids",
    "resolve_task_order",
    "runtime_task_ids_for_order",
]
