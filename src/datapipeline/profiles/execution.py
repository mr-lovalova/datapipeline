import logging
from pathlib import Path
from typing import cast

from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.build.state import load_build_state
from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.tasks import ArtifactTask, OperationTask, Task
from datapipeline.execution.observability import operation_scope
from datapipeline.operations.dispatch import execute_operation
from datapipeline.operations.persistence import (
    persist_runtime_result,
)
from datapipeline.plugins import RUNTIME_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import build_state_path
from datapipeline.services.constants import VECTOR_INPUTS

from .models import (
    ExecutionProfile,
    ProfileRunRequest,
)

logger = logging.getLogger(__name__)


def resolve_task_order(
    profile: ExecutionProfile,
    tasks_by_id: dict[str, Task],
) -> list[str]:
    target_id = profile.target_id
    if target_id not in tasks_by_id:
        raise ValueError(f"Unknown task target '{target_id}'")
    return [target_id]


def artifact_task_ids_for_order(
    ordered_ids: list[str],
    tasks_by_id: dict[str, Task],
) -> list[str]:
    return [
        task_id
        for task_id in ordered_ids
        if isinstance(tasks_by_id[task_id], ArtifactTask)
    ]


def runtime_task_ids_for_order(
    ordered_ids: list[str],
    tasks_by_id: dict[str, Task],
) -> list[str]:
    return [
        task_id
        for task_id in ordered_ids
        if isinstance(tasks_by_id[task_id], OperationTask)
    ]


def required_artifacts_for_task_ids(
    request: ProfileRunRequest,
    artifact_task_ids: list[str],
) -> set[str]:
    graph = build_artifact_graph(request.artifact_task_configs)
    return graph.keys_for_tasks(artifact_task_ids)


def run_selected_artifacts(
    request: ProfileRunRequest,
    profile: ExecutionProfile,
    artifact_task_ids: list[str],
    required_artifacts: set[str] | None = None,
    runtime_override: Runtime | None = None,
) -> None:
    selected_artifacts = set(required_artifacts or ())
    selected_artifacts.update(
        required_artifacts_for_task_ids(request, artifact_task_ids)
    )
    if not selected_artifacts:
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
        required_artifacts=selected_artifacts,
        artifact_task_configs=list(request.artifact_task_configs),
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
            f"Runtime profile '{profile.name}' is missing runtime context."
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


def sync_runtime_artifacts_from_state(
    runtime: Runtime,
    project_path: Path,
) -> None:
    artifacts = getattr(runtime, "artifacts", None)
    if artifacts is None or not hasattr(artifacts, "register"):
        return
    try:
        state_path = build_state_path(project_path)
    except Exception:
        return
    state = load_build_state(state_path)
    if state is None:
        return
    for key, info in state.artifacts.items():
        artifacts.register(
            key,
            relative_path=info.relative_path,
            meta=info.meta,
        )


def execute_profile(
    profile: ExecutionProfile,
    request: ProfileRunRequest,
    tasks_by_id: dict[str, Task],
    ordered_ids: list[str],
    runtime_override: Runtime | None = None,
) -> None:
    artifact_task_ids = artifact_task_ids_for_order(ordered_ids, tasks_by_id)
    runtime_task_ids = runtime_task_ids_for_order(ordered_ids, tasks_by_id)
    runtime_required_artifacts: set[str] = set()
    for task_id in runtime_task_ids:
        task = tasks_by_id[task_id]
        if (
            isinstance(task, OperationTask)
            and task.entrypoint == "core.runtime.pipeline"
        ):
            runtime_required_artifacts.add(VECTOR_INPUTS)
    runtime = runtime_override or profile.runtime

    should_build_artifacts = bool(
        (artifact_task_ids or runtime_required_artifacts)
        and (profile.build_settings is not None or not request.skip_build)
    )
    if should_build_artifacts:
        run_selected_artifacts(
            request=request,
            profile=profile,
            artifact_task_ids=artifact_task_ids,
            required_artifacts=runtime_required_artifacts,
            runtime_override=runtime,
        )

    if not runtime_task_ids:
        return

    if runtime is None:
        logger.error(
            "Profile '%s' resolves runtime tasks but has no runtime context: %s",
            profile.name,
            ", ".join(runtime_task_ids),
        )
        raise SystemExit(2)
    sync_runtime_artifacts_from_state(runtime, request.project_path)
    for task_id in runtime_task_ids:
        task = cast(OperationTask, tasks_by_id[task_id])
        run_runtime_task(task, profile, runtime, command=request.command)


__all__ = [
    "artifact_task_ids_for_order",
    "execute_profile",
    "required_artifacts_for_task_ids",
    "resolve_task_order",
    "run_selected_artifacts",
    "run_runtime_task",
    "runtime_task_ids_for_order",
    "sync_runtime_artifacts_from_state",
]
