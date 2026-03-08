import logging
from pathlib import Path
from typing import cast

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.build.state import load_build_state
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.config.tasks import OperationTask, Task
from datapipeline.operations.dispatch import execute_operation
from datapipeline.operations.persistence import (
    persist_runtime_result,
)
from datapipeline.plugins import RUNTIME_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import build_state_path

from .models import (
    ExecutionProfile,
    ProfileRunRequest,
)
from .planning import (
    artifact_task_ids_for_order,
    required_artifacts_for_task_ids,
    resolve_task_order,
    runtime_task_ids_for_order,
)

logger = logging.getLogger(__name__)


def run_selected_artifacts(
    request: ProfileRunRequest,
    profile: ExecutionProfile,
    artifact_task_ids: list[str],
    runtime_override: Runtime | None = None,
) -> None:
    if not artifact_task_ids:
        return
    required_artifacts = required_artifacts_for_task_ids(request, artifact_task_ids)
    if not required_artifacts:
        return
    settings = profile.build_settings
    runtime_build_mode = profile.build_mode
    cli_log_level = request.cli_log_level
    cli_visuals = request.cli_visuals
    cli_log_outputs = request.cli_log_outputs
    workspace = request.workspace
    if settings is not None:
        runtime_build_mode = None
        cli_log_level = None
        cli_visuals = None
        cli_log_outputs = ()
        workspace = None
    run_build_if_needed(
        request.project_path,
        runtime_build_mode=runtime_build_mode,
        cli_log_level=cli_log_level,
        cli_visuals=cli_visuals,
        cli_log_outputs=list(cli_log_outputs or ()),
        workspace=workspace,
        required_artifacts=required_artifacts,
        artifact_task_configs=list(request.artifact_task_configs),
        settings=settings,
        profile_name_override=profile.name,
        skip_logging_setup=True,
        runtime_override=runtime_override,
    )


def run_runtime_task(
    task: OperationTask,
    profile: ExecutionProfile,
    runtime: Runtime,
) -> None:
    if profile.dataset is None:
        raise ValueError(
            f"Runtime profile '{profile.name}' is missing runtime context."
        )
    try:
        execute_operation(
            operation=task,
            operation_group=RUNTIME_OPERATIONS_EP,
            persist=lambda result: persist_runtime_result(
                result,
                target=profile.output,
                visuals=profile.visuals,
                logger=logger,
            ),
            operation_task=task,
            runtime=runtime,
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


def _execute_runtime_tasks(
    profile: ExecutionProfile,
    runtime: Runtime,
    runtime_task_ids: list[str],
    tasks_by_id: dict[str, Task],
) -> None:
    total_runtime_tasks = len(runtime_task_ids)
    for idx, task_id in enumerate(runtime_task_ids, start=1):
        task = cast(OperationTask, tasks_by_id[task_id])
        with execution_scope(
            task_id=task.id,
            item_index=idx,
            item_total=total_runtime_tasks,
            announce=total_runtime_tasks > 1,
        ):
            run_runtime_task(task, profile, runtime)


def execute_profile(
    profile: ExecutionProfile,
    request: ProfileRunRequest,
    tasks_by_id: dict[str, Task],
    runtime_override: Runtime | None = None,
) -> None:
    try:
        ordered_ids = resolve_task_order(profile, tasks_by_id)
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    artifact_task_ids = artifact_task_ids_for_order(ordered_ids, tasks_by_id)
    runtime_task_ids = runtime_task_ids_for_order(ordered_ids, tasks_by_id)
    runtime = runtime_override or profile.runtime

    should_build_artifacts = bool(
        artifact_task_ids and (profile.build_settings is not None or not request.skip_build)
    )
    if should_build_artifacts:
        run_selected_artifacts(
            request=request,
            profile=profile,
            artifact_task_ids=artifact_task_ids,
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
    _execute_runtime_tasks(profile, runtime, runtime_task_ids, tasks_by_id)


__all__ = [
    "execute_profile",
    "run_selected_artifacts",
    "run_runtime_task",
    "sync_runtime_artifacts_from_state",
]
