import logging

from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.executions import start_execution
from datapipeline.services.runs import (
    finish_run_failed,
    finish_run_success,
    set_latest_run,
    start_run,
)
from .execution import execute_profile, resolve_task_order
from .models import ProfileRunRequest, ServeRunPlan
from .reporting import persist_profile_report

logger = logging.getLogger(__name__)


def run_profiles(request: ProfileRunRequest) -> None:
    tasks_by_id = {task.id: task for task in request.tasks}
    profiles = list(request.profiles)
    if not profiles:
        return
    try:
        task_orders = [resolve_task_order(profile, tasks_by_id) for profile in profiles]
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    started_serve_runs: list[ServeRunPlan] = []
    succeeded = False
    try:
        start_execution(
            request.execution,
            project_yaml=request.project_path,
            command=request.command,
        )
        for plan in request.serve_run_plans:
            start_run(plan.paths, preview_index=plan.preview_index)
            started_serve_runs.append(plan)

        total = len(profiles)
        for idx, profile in enumerate(profiles, start=1):
            runtime = profile.runtime or bootstrap(request.project_path)
            runtime.heartbeat_interval_seconds = profile.heartbeat_interval_seconds
            profile_path = persist_profile_report(
                profile_kind=request.command,
                profile=profile,
                payload=profile.profile_report,
                execution=request.execution,
            )
            spec = ProfileExecutionSpec(
                command=request.command,
                name=profile.name,
                idx=idx,
                total=total,
                visuals=profile.visuals or "on",
                log_decision=profile.log_decision,
                log_output=profile.log_output,
                sections=profile.sections,
                label=profile.label or profile.name,
                runtime=runtime,
                use_visual_runner=True,
                profile_path=profile_path,
            )

            ordered_ids = task_orders[idx - 1]

            def work(profile=profile, runtime=runtime, ordered_ids=ordered_ids):
                scope_target = profile.target_id
                with execution_scope(
                    profile_kind=request.command,
                    profile_name=profile.name,
                    target_id=scope_target,
                    announce=False,
                ):
                    return execute_profile(
                        profile=profile,
                        request=request,
                        tasks_by_id=tasks_by_id,
                        ordered_ids=ordered_ids,
                        runtime_override=runtime,
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
