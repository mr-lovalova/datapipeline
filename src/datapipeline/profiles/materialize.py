from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.cli.visuals.execution import (
    emit_execution_message,
    execution_scope,
)
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
    MaterializeResult,
    check_materialize_destinations,
    materialize_destination_paths,
    materialize_stream_to_path,
)


@dataclass(frozen=True)
class MaterializeJob:
    name: str
    stream: str
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
    cli_output: Path | None,
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
        jobs = [
            _resolve_profile(
                profile,
                project_path,
                execution.root,
                overwrite,
                cli_output,
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
    _preflight_jobs(runtime, jobs)

    start_execution(execution, project_yaml=project_path, command="materialize")
    results: list[MaterializeResult] = []
    total = len(jobs)
    for index, job in enumerate(jobs, start=1):
        runtime.heartbeat_interval_seconds = job.heartbeat_interval_seconds
        spec = ProfileExecutionSpec(
            command="materialize",
            name=job.name,
            index=index,
            total=total,
            visuals=job.visuals,
            log_decision=job.log_decision,
            log_output=job.log_output,
            runtime=runtime,
        )

        def work() -> MaterializeResult:
            with execution_scope(
                profile_kind="materialize",
                profile_name=job.name,
                target_id=job.stream,
            ):
                result = materialize_stream_to_path(
                    runtime=runtime,
                    stream_id=job.stream,
                    output=job.output,
                    overwrite=job.overwrite,
                    dataset=dataset,
                )
                emit_execution_message(f"Result: {result.count:,} records")
                emit_execution_message(f"Output: {result.output}")
                emit_execution_message(f"Metadata: {result.metadata}")
                return result

        results.append(run_profile(spec, work))
    return results


def _resolve_profile(
    profile: MaterializeProfile,
    project_path: Path,
    execution_dir: Path,
    overwrite: bool | None,
    cli_output: Path | None,
    cli_visuals: str | None,
    cli_heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: Sequence[LogOutputTarget],
    base_log_level: str,
) -> MaterializeJob:
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
    output = cli_output if cli_output is not None else profile.output
    if not output.is_absolute():
        output = project_path.parent / output
    return MaterializeJob(
        name=profile.name,
        stream=profile.stream,
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


def _preflight_jobs(
    runtime: Runtime,
    jobs: Sequence[MaterializeJob],
) -> None:
    destinations: list[tuple[MaterializeJob, tuple[Path, Path]]] = []
    owners: dict[Path, str] = {}
    available_streams = set(runtime.registries.stream_specs.keys())
    for job in jobs:
        if job.stream not in available_streams:
            raise MaterializeProfileError(
                f"Materialize profile '{job.name}' references unknown "
                f"stream '{job.stream}'."
            )
        paths = materialize_destination_paths(job.output)
        destinations.append((job, paths))
        for path in paths:
            owner = owners.get(path)
            if owner is not None:
                raise MaterializeProfileError(
                    f"Materialize profiles '{owner}' and '{job.name}' "
                    f"write the same path: {path}"
                )
            owners[path] = job.name

    for job, paths in destinations:
        check_materialize_destinations(paths, job.overwrite)
