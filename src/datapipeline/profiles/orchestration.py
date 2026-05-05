import shutil
import tempfile
from pathlib import Path

from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.services.bootstrap import bootstrap
from datapipeline.runtime import Runtime
from datapipeline.services.runs import (
    RunPaths,
    finish_run_failed,
    finish_run_success,
    set_latest_run,
)
from .execution import execute_profile
from .models import ProfileRunRequest
from .reporting import persist_profile_report


def run_profiles(request: ProfileRunRequest) -> None:
    tasks_by_id = {task.id: task for task in request.tasks}
    profiles = list(request.profiles)
    if not profiles:
        return

    shared_cache_root = _shared_cache_root(request, profiles)
    managed_serve_runs = _managed_serve_runs(request, profiles)
    succeeded = False
    try:
        total = len(profiles)
        for idx, profile in enumerate(profiles, start=1):
            runtime = profile.runtime or bootstrap(request.project_path)
            if shared_cache_root is not None and isinstance(runtime, Runtime):
                runtime.set_cache_root(shared_cache_root, owned=False)
            profile_path = persist_profile_report(
                profile_kind=request.command,
                profile=profile,
                payload=profile.profile_report,
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

            def work(profile=profile, runtime=runtime):
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
                        runtime_override=runtime,
                    )

            run_profile(spec=spec, work=work)
        succeeded = True
    finally:
        _finalize_managed_serve_runs(managed_serve_runs, succeeded)
        if shared_cache_root is not None:
            shutil.rmtree(shared_cache_root, ignore_errors=True)


def _shared_cache_root(
    request: ProfileRunRequest,
    profiles,
) -> Path | None:
    if request.command not in {"serve", "inspect"}:
        return None
    if len(profiles) <= 1:
        return None
    if not any(getattr(profile, "cache_enabled", True) for profile in profiles):
        return None
    project_name = request.project_path.stem or "project"
    return Path(
        tempfile.mkdtemp(prefix=f"datapipeline-cache-{project_name}-")
    ).resolve()


def _managed_serve_runs(
    request: ProfileRunRequest,
    profiles,
) -> dict[RunPaths, bool]:
    if request.command != "serve":
        return {}
    runs: dict[RunPaths, bool] = {}
    for profile in profiles:
        run = getattr(getattr(profile, "output", None), "run", None)
        if run is None:
            continue
        runs[run] = runs.get(run, True) and profile.preview_index is None
    return runs


def _finalize_managed_serve_runs(
    runs: dict[RunPaths, bool],
    succeeded: bool,
) -> None:
    for run, publish_latest in runs.items():
        if not succeeded:
            finish_run_failed(run)
            continue
        finish_run_success(run)
        if publish_latest:
            set_latest_run(run)
