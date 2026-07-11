from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.loaders.profiles import (
    apply_profile_defaults,
    profile_specs_with_defaults,
)
from datapipeline.config.profiles import MaterializeProfile
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    logging_value,
    materialize_log_output_for_execution,
    observability_value,
    resolve_heartbeat_interval_seconds,
    resolve_log_level,
    resolve_log_output,
    resolve_project_log_outputs,
    resolve_visuals,
)
from datapipeline.profiles.executor import ProfileExecutionSpec, run_profile
from datapipeline.profiles.selection import select_profiles
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.executions import get_execution_paths, start_execution
from datapipeline.services.materialize import (
    MaterializeDestinationPaths,
    MaterializeResult,
    check_materialize_destinations,
    materialize_destination_paths,
    materialize_stream_to_path,
)


@dataclass(frozen=True)
class ResolvedMaterializeProfile:
    profile: MaterializeProfile
    output: Path
    overwrite: bool
    visuals: str
    heartbeat_interval_seconds: float | None
    log_decision: LogLevelDecision
    log_output: LogOutputSettings


class MaterializeProfileError(ValueError):
    """Invalid materialize profile selection or configuration."""


def run_materialize_profiles(
    project_path: Path,
    run_name: str | None,
    overwrite: bool | None,
    cli_visuals: str | None,
    cli_heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: Sequence[LogOutputTarget],
    base_log_level: str,
) -> list[MaterializeResult]:
    try:
        profiles, defaults = profile_specs_with_defaults(project_path, "materialize")
    except (OSError, TypeError, ValueError) as exc:
        raise MaterializeProfileError(
            f"Failed to load materialize profiles: {exc}"
        ) from exc
    if not profiles:
        raise MaterializeProfileError("Project does not define materialize profiles.")

    try:
        selected = select_profiles(profiles, run_name, "materialize")
        selected = [apply_profile_defaults(profile, defaults) for profile in selected]
    except ValueError as exc:
        raise MaterializeProfileError(str(exc)) from exc
    materialize_profiles: list[MaterializeProfile] = []
    for profile in selected:
        if not isinstance(profile, MaterializeProfile):
            raise TypeError(
                "Materialize profile loading returned the wrong profile type"
            )
        materialize_profiles.append(profile)
    if not materialize_profiles:
        return []

    runtime = bootstrap(project_path)
    dataset = load_dataset(project_path, "vectors")
    execution = get_execution_paths(project_path)
    try:
        runs = [
            _resolve_profile(
                profile,
                project_path,
                execution.root,
                overwrite,
                cli_visuals,
                cli_heartbeat_interval_seconds,
                cli_log_level,
                cli_log_outputs,
                base_log_level,
            )
            for profile in materialize_profiles
        ]
    except ValueError as exc:
        raise MaterializeProfileError(
            f"Invalid materialize profile settings: {exc}"
        ) from exc
    _preflight_profiles(runtime, runs)

    start_execution(execution, project_yaml=project_path, command="materialize")
    results: list[MaterializeResult] = []
    total = len(runs)
    for index, run in enumerate(runs, start=1):
        runtime.heartbeat_interval_seconds = run.heartbeat_interval_seconds
        profile = run.profile
        spec = ProfileExecutionSpec(
            command="materialize",
            name=profile.name,
            idx=index,
            total=total,
            visuals=run.visuals,
            log_decision=run.log_decision,
            log_output=run.log_output,
            sections=("Materialize Profiles",),
            label=profile.name,
            runtime=runtime,
            profile_path=profile.source_path,
        )

        def work(
            run: ResolvedMaterializeProfile = run,
        ) -> MaterializeResult:
            return materialize_stream_to_path(
                runtime=runtime,
                stream_id=run.profile.stream,
                output=run.output,
                as_stream_id=run.profile.as_stream_id,
                overwrite=run.overwrite,
                dataset=dataset,
            )

        results.append(run_profile(spec, work))
    return results


def _resolve_profile(
    profile: MaterializeProfile,
    project_path: Path,
    execution_dir: Path,
    overwrite: bool | None,
    cli_visuals: str | None,
    cli_heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: Sequence[LogOutputTarget],
    base_log_level: str,
) -> ResolvedMaterializeProfile:
    observability = profile.observability
    configured_outputs = resolve_project_log_outputs(
        logging_value(observability, "outputs"),
        project_path,
    )
    log_output = resolve_log_output(
        output_candidates=(cli_log_outputs, configured_outputs),
        allow_execution_scope=True,
    )
    log_output = materialize_log_output_for_execution(
        log_output,
        execution_dir,
        command="materialize",
        label=profile.name,
    )
    output = profile.output
    if not output.is_absolute():
        output = project_path.parent / output
    return ResolvedMaterializeProfile(
        profile=profile,
        output=output.resolve(),
        overwrite=profile.overwrite if overwrite is None else overwrite,
        visuals=resolve_visuals(
            cli_visuals,
            observability_value(observability, "visuals"),
        ).visuals,
        heartbeat_interval_seconds=resolve_heartbeat_interval_seconds(
            cli_heartbeat_interval_seconds,
            observability_value(observability, "heartbeat_interval_seconds"),
        ),
        log_decision=resolve_log_level(
            cli_log_level,
            logging_value(observability, "level"),
            base_log_level,
        ),
        log_output=log_output,
    )


def _preflight_profiles(
    runtime: Runtime,
    runs: Sequence[ResolvedMaterializeProfile],
) -> None:
    destinations: list[
        tuple[ResolvedMaterializeProfile, MaterializeDestinationPaths]
    ] = []
    owners: dict[Path, str] = {}
    available_streams = set(runtime.registries.stream_specs.keys())
    for run in runs:
        if run.profile.stream not in available_streams:
            raise MaterializeProfileError(
                f"Materialize profile '{run.profile.name}' references unknown "
                f"stream '{run.profile.stream}'."
            )
        paths = materialize_destination_paths(
            runtime,
            run.output,
            run.profile.as_stream_id,
        )
        destinations.append((run, paths))
        for path in paths.paths:
            owner = owners.get(path)
            if owner is not None:
                raise MaterializeProfileError(
                    f"Materialize profiles '{owner}' and '{run.profile.name}' "
                    f"write the same path: {path}"
                )
            owners[path] = run.profile.name

    for run, paths in destinations:
        check_materialize_destinations(paths, run.overwrite)
