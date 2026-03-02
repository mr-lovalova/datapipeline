import logging
from pathlib import Path

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.build.state import load_build_state
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.tasks import OperationTask, Task
from datapipeline.operations.dispatch import dispatch_operation
from datapipeline.plugins import RUNTIME_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import build_state_path

from .executor import ProfileExecutionSpec, run_profile
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
    settings: BuildSettings | None = None,
    build_options: RuntimeBuildOptions | None = None,
    runtime_override: Runtime | None = None,
    profile_name_override: str | None = None,
) -> None:
    if not artifact_task_ids:
        return
    required_artifacts = required_artifacts_for_task_ids(request, artifact_task_ids)
    if not required_artifacts:
        return
    runtime_build_mode = None
    cli_log_level = None
    cli_visuals = None
    cli_log_outputs: list = []
    workspace = None
    if build_options is not None:
        runtime_build_mode = build_options.build_mode
        cli_log_level = build_options.cli_log_level
        cli_visuals = build_options.cli_visuals
        cli_log_outputs = list(build_options.cli_log_outputs)
        workspace = build_options.workspace
    run_build_if_needed(
        request.project_path,
        runtime_build_mode=runtime_build_mode,
        cli_log_level=cli_log_level,
        cli_visuals=cli_visuals,
        cli_log_outputs=cli_log_outputs,
        workspace=workspace,
        required_artifacts=required_artifacts,
        artifact_task_configs=list(request.artifact_task_configs),
        settings=settings,
        profile_name_override=profile_name_override,
        skip_logging_setup=True,
        runtime_override=runtime_override,
    )


def run_runtime_task(
    task: OperationTask,
    profile: RuntimeExecutionProfile,
) -> None:
    try:
        dispatch_operation(
            operation=task,
            operation_group=RUNTIME_OPERATIONS_EP,
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


def run_runtime_artifact_dependencies(
    *,
    profile: RuntimeExecutionProfile,
    request: ProfileRunRequest,
    artifact_task_ids: list[str],
) -> None:
    if profile.skip_artifacts or not artifact_task_ids:
        return

    dependency_profile = f"{profile.name}.dependencies"
    spec = ProfileExecutionSpec(
        kind="build",
        name=dependency_profile,
        idx=1,
        total=1,
        visuals=profile.visuals or "on",
        log_decision=profile.log_decision,
        log_output=profile.log_output,
        use_visual_runner=False,
        render_header=False,
    )
    run_profile(
        spec=spec,
        work=lambda: _run_runtime_dependencies_in_scope(
            profile=profile,
            request=request,
            artifact_task_ids=artifact_task_ids,
            dependency_profile=dependency_profile,
        ),
    )


def _run_runtime_dependencies_in_scope(
    *,
    profile: RuntimeExecutionProfile,
    request: ProfileRunRequest,
    artifact_task_ids: list[str],
    dependency_profile: str,
) -> None:
    with execution_scope(
        phase="dependencies",
        profile_kind="build",
        profile_name=dependency_profile,
        target_id="dependencies",
    ):
        run_selected_artifacts(
            request=request,
            artifact_task_ids=artifact_task_ids,
            build_options=profile.build_options,
            runtime_override=profile.runtime,
            profile_name_override=dependency_profile,
        )


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
    run_runtime_artifact_dependencies(
        profile=profile,
        request=request,
        artifact_task_ids=artifact_task_ids,
    )
    sync_runtime_artifacts_from_state(profile, request.project_path)

    runtime_task_ids = runtime_task_ids_for_order(ordered_ids, tasks_by_id)
    total_runtime_tasks = len(runtime_task_ids)
    for idx, task_id in enumerate(runtime_task_ids, start=1):
        task = tasks_by_id[task_id]
        if not isinstance(task, OperationTask):
            continue
        with execution_scope(
            phase="runtime",
            task_id=task.id,
            item_index=idx,
            item_total=total_runtime_tasks,
            announce=True,
        ):
            run_runtime_task(task, profile)


def execute_build_profile(
    profile: BuildExecutionProfile,
    request: ProfileRunRequest,
    tasks_by_id: dict[str, Task],
    runtime_override: Runtime | None = None,
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
        runtime_override=runtime_override,
    )


__all__ = [
    "execute_build_profile",
    "execute_runtime_profile",
    "run_runtime_artifact_dependencies",
    "run_selected_artifacts",
    "run_runtime_task",
    "sync_runtime_artifacts_from_state",
]
