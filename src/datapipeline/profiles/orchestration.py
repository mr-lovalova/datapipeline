from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.services.bootstrap import bootstrap
from .execution import execute_build_profile, execute_runtime_profile
from .models import ProfileRunRequest, RuntimeExecutionProfile


def run_profiles(request: ProfileRunRequest) -> None:
    tasks_by_id = {task.id: task for task in request.tasks}
    profiles = list(request.profiles)
    if not profiles:
        return

    total = len(profiles)
    for idx, profile in enumerate(profiles, start=1):
        is_runtime_profile = isinstance(profile, RuntimeExecutionProfile)
        runtime = (
            profile.runtime
            if is_runtime_profile
            else bootstrap(request.project_path)
        )
        spec = ProfileExecutionSpec(
            kind=profile.kind,
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
            artifact_payload=profile.artifact_payload,
            artifact_writer=profile.artifact_writer,
        )
        if is_runtime_profile:
            def work(profile=profile):
                with execution_scope(
                    profile_kind=profile.kind,
                    profile_name=profile.name,
                    target_id=profile.target_id,
                ):
                    return execute_runtime_profile(
                        profile=profile,
                        request=request,
                        tasks_by_id=tasks_by_id,
                    )
        else:
            def work(profile=profile, runtime=runtime):
                with execution_scope(
                    profile_kind=profile.kind,
                    profile_name=profile.name,
                    target_id=profile.target_id,
                ):
                    return execute_build_profile(
                        profile=profile,
                        request=request,
                        tasks_by_id=tasks_by_id,
                        runtime_override=runtime,
                    )
        run_profile(spec=spec, work=work)
