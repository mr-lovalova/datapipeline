from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.services.bootstrap import bootstrap
from .execution import execute_profile
from .models import ProfileRunRequest
from .reporting import persist_profile_report


def run_profiles(request: ProfileRunRequest) -> None:
    tasks_by_id = {task.id: task for task in request.tasks}
    profiles = list(request.profiles)
    if not profiles:
        return

    total = len(profiles)
    for idx, profile in enumerate(profiles, start=1):
        runtime = profile.runtime or bootstrap(request.project_path)
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
                announce=True,
            ):
                return execute_profile(
                    profile=profile,
                    request=request,
                    tasks_by_id=tasks_by_id,
                    runtime_override=runtime,
                )

        run_profile(spec=spec, work=work)
