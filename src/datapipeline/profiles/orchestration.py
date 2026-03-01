from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from .execution import execute_build_profile, execute_runtime_profile
from .models import ProfileRunRequest, RuntimeExecutionProfile


def run_profiles(request: ProfileRunRequest) -> None:
    tasks_by_id = {task.id: task for task in request.tasks}
    profiles = list(request.profiles)
    if not profiles:
        return

    total = len(profiles)
    for idx, profile in enumerate(profiles, start=1):
        runtime = profile.runtime if isinstance(
            profile, RuntimeExecutionProfile) else None
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
            use_visual_runner=(runtime is not None),
            artifact_payload=profile.artifact_payload,
            artifact_writer=profile.artifact_writer,
        )
        if isinstance(profile, RuntimeExecutionProfile):
            def work(profile=profile):
                return execute_runtime_profile(
                    profile=profile,
                    request=request,
                    tasks_by_id=tasks_by_id,
                )
        else:
            def work(profile=profile):
                return execute_build_profile(
                    profile=profile,
                    request=request,
                    tasks_by_id=tasks_by_id,
                )
        run_profile(spec=spec, work=work)
