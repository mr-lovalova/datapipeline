from typing import Sequence

from datapipeline.artifacts.planning import build_planning_context
from datapipeline.artifacts.specs import artifact_keys_for_task_ids
from datapipeline.config.tasks import ArtifactTask, OperationTask, Task

from .models import ExecutionProfile, ProfileRunRequest


def resolve_task_order(
    profile: ExecutionProfile,
    tasks_by_id: dict[str, Task],
) -> list[str]:
    return ordered_task_ids(profile.target_id, tasks_by_id)


def ordered_task_ids(
    target_id: str,
    tasks_by_id: dict[str, Task],
) -> list[str]:
    if target_id not in tasks_by_id:
        raise ValueError(f"Unknown task target '{target_id}'")
    return [target_id]


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
    "ordered_task_ids",
    "required_artifacts_for_task_ids",
    "resolve_task_order",
    "runtime_task_ids_for_order",
]
