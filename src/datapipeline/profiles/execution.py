import logging
from pathlib import Path
from typing import Any

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.build.state import load_build_state
from datapipeline.config.tasks import OperationTask, Task
from datapipeline.operations.dispatch import dispatch_operation
from datapipeline.plugins import INSPECT_OPERATIONS_EP, SERVE_OPERATIONS_EP
from datapipeline.services.bootstrap import build_state_path

from .models import (
    BuildExecutionProfile,
    ProfileRunRequest,
    RuntimeBuildOptions,
    RuntimeExecutionProfile,
)
from .planning import (
    artifact_task_ids_for_order,
    required_artifacts_for_task_ids,
    resolve_task_order,
    runtime_task_ids_for_order,
)

logger = logging.getLogger(__name__)


def run_selected_artifacts(
    *,
    request: ProfileRunRequest,
    artifact_task_ids: list[str],
    settings: Any | None = None,
    build_options: RuntimeBuildOptions | None = None,
) -> None:
    if not artifact_task_ids:
        return
    required_artifacts = required_artifacts_for_task_ids(request, artifact_task_ids)
    if not required_artifacts:
        return
    run_build_if_needed(
        request.project_path,
        cli_log_level=(build_options.cli_log_level if build_options is not None else None),
        cli_visuals=(build_options.cli_visuals if build_options is not None else None),
        cli_log_outputs=list(build_options.cli_log_outputs) if build_options is not None else [],
        workspace=(build_options.workspace if build_options is not None else None),
        required_artifacts=required_artifacts,
        artifact_task_configs=list(request.artifact_task_configs),
        settings=settings,
        skip_logging_setup=True,
    )


def run_runtime_task(
    task: OperationTask,
    profile: RuntimeExecutionProfile,
) -> None:
    try:
        operation_group = (
            INSPECT_OPERATIONS_EP
            if profile.kind == "inspect"
            else SERVE_OPERATIONS_EP
        )
        dispatch_operation(
            operation=task,
            operation_group=operation_group,
            operation_type="operation",
            operation_task=task,
            runtime=profile.runtime,
            dataset=profile.dataset,
            limit=profile.limit,
            target=profile.output,
            throttle_ms=profile.throttle_ms,
            stage=profile.stage,
            visuals=profile.visuals,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc


def sync_runtime_artifacts_from_state(
    profile: RuntimeExecutionProfile,
    project_path: Path,
) -> None:
    artifacts = getattr(profile.runtime, "artifacts", None)
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


def execute_runtime_profile(
    profile: RuntimeExecutionProfile,
    request: ProfileRunRequest,
    tasks_by_id: dict[str, Task],
) -> None:
    try:
        ordered_ids = resolve_task_order(profile, tasks_by_id)
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    artifact_task_ids = artifact_task_ids_for_order(ordered_ids, tasks_by_id)
    if not profile.skip_artifacts:
        run_selected_artifacts(
            request=request,
            artifact_task_ids=artifact_task_ids,
            build_options=profile.build_options,
        )
    sync_runtime_artifacts_from_state(profile, request.project_path)

    for task_id in runtime_task_ids_for_order(ordered_ids, tasks_by_id):
        task = tasks_by_id[task_id]
        if not isinstance(task, OperationTask):
            continue
        run_runtime_task(task, profile)


def execute_build_profile(
    profile: BuildExecutionProfile,
    request: ProfileRunRequest,
    tasks_by_id: dict[str, Task],
) -> None:
    try:
        ordered_ids = resolve_task_order(profile, tasks_by_id)
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    runtime_task_ids = runtime_task_ids_for_order(ordered_ids, tasks_by_id)
    if runtime_task_ids:
        logger.error(
            "Build profile '%s' resolves runtime tasks: %s",
            profile.name,
            ", ".join(runtime_task_ids),
        )
        raise SystemExit(2)

    run_selected_artifacts(
        request=request,
        artifact_task_ids=artifact_task_ids_for_order(ordered_ids, tasks_by_id),
        settings=profile.build_settings,
    )


__all__ = [
    "execute_build_profile",
    "execute_runtime_profile",
    "run_selected_artifacts",
    "run_runtime_task",
    "sync_runtime_artifacts_from_state",
]
