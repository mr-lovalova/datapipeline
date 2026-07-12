import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.artifacts.executor import run_build_if_needed
from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.artifacts.validation import stream_cadence_artifacts
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import (
    apply_profile_defaults,
    profile_specs_with_defaults,
)
from datapipeline.config.profiles import (
    MaterializeProfile,
    MaterializeProfileDefaults,
    normalize_artifact_mode,
)
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    resolve_execution_log_outputs,
    resolve_observability_settings,
)
from datapipeline.config.tasks import TicksTask
from datapipeline.execution.observability import (
    emit_file_result,
    operation_scope,
)
from datapipeline.profiles.executor import ExecutionSpec, run_execution
from datapipeline.profiles.selection import select_profiles
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.bootstrap.core import load_streams
from datapipeline.services.executions import execution_root
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
    cli_artifact_mode: str | None,
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
    if not isinstance(defaults, MaterializeProfileDefaults):
        raise TypeError("Materialize profile loading returned the wrong defaults type")
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
    runtime.execution = defaults.execution
    dataset = load_dataset(project_path, "vectors")
    execution_dir = execution_root(project_path)
    try:
        jobs = [
            _resolve_profile(
                profile,
                project_path,
                execution_dir,
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
        artifact_observability = resolve_observability_settings(
            project_path,
            defaults.observability,
            cli_visuals=cli_visuals,
            cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
    except ValueError as exc:
        raise MaterializeProfileError(
            f"Invalid materialize profile settings: {exc}"
        ) from exc
    _preflight_jobs(runtime, jobs)
    artifact_mode = (
        normalize_artifact_mode(cli_artifact_mode) or defaults.artifact_mode or "AUTO"
    )
    artifact_settings = BuildSettings(
        visuals=artifact_observability.visuals,
        heartbeat_interval_seconds=artifact_observability.heartbeat_interval_seconds,
        log_decision=artifact_observability.log_decision,
        log_output=resolve_execution_log_outputs(
            artifact_observability.log_output,
            execution_dir,
            command="materialize",
            label="artifacts",
        ),
        mode=artifact_mode,
    )
    _prepare_materialize_artifacts(
        project_path,
        runtime,
        jobs,
        artifact_settings,
    )

    results: list[MaterializeResult] = []
    total = len(jobs)
    for index, job in enumerate(jobs, start=1):
        runtime.heartbeat_interval_seconds = job.heartbeat_interval_seconds
        spec = ExecutionSpec(
            visuals=job.visuals,
            log_decision=job.log_decision,
            log_output=job.log_output,
            runtime=runtime,
        )

        def work() -> MaterializeResult:
            emit_execution_message(
                f"Profile: materialize {job.name} ({index}/{total}) "
                f"stream={job.stream} output={job.output} "
                f"overwrite={str(job.overwrite).lower()}",
                level=logging.DEBUG,
            )
            with operation_scope(f"materialize:{job.name}"):
                result = materialize_stream_to_path(
                    runtime=runtime,
                    stream_id=job.stream,
                    output=job.output,
                    overwrite=job.overwrite,
                    dataset=dataset,
                )
                emit_file_result("Output", result.output)
                emit_file_result("Metadata", result.metadata)
                return result

        results.append(run_execution(spec, work))
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
    observability = resolve_observability_settings(
        project_path,
        profile.observability,
        cli_visuals=cli_visuals,
        cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
    )
    log_output = resolve_execution_log_outputs(
        observability.log_output,
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
        visuals=observability.visuals,
        heartbeat_interval_seconds=observability.heartbeat_interval_seconds,
        log_decision=observability.log_decision,
        log_output=log_output,
    )


def _preflight_jobs(
    runtime: Runtime,
    jobs: Sequence[MaterializeJob],
) -> None:
    destinations: list[tuple[MaterializeJob, tuple[Path, Path]]] = []
    owners: dict[Path, str] = {}
    available_streams = set(runtime.registries.stream_specs.keys())
    artifacts_root = runtime.artifacts_root.resolve()
    for job in jobs:
        if job.stream not in available_streams:
            raise MaterializeProfileError(
                f"Materialize profile '{job.name}' references unknown "
                f"stream '{job.stream}'."
            )
        paths = materialize_destination_paths(job.output)
        destinations.append((job, paths))
        for path in paths:
            if path.is_relative_to(artifacts_root):
                raise MaterializeProfileError(
                    f"Materialize profile '{job.name}' writes inside the managed "
                    f"artifacts root: {path}"
                )
            owner = owners.get(path)
            if owner is not None:
                raise MaterializeProfileError(
                    f"Materialize profiles '{owner}' and '{job.name}' "
                    f"write the same path: {path}"
                )
            owners[path] = job.name

    for job, paths in destinations:
        check_materialize_destinations(paths, job.overwrite)


def _prepare_materialize_artifacts(
    project_path: Path,
    runtime: Runtime,
    jobs: Sequence[MaterializeJob],
    settings: BuildSettings,
) -> None:
    try:
        streams = load_streams(project_path)
        required_artifacts = {
            artifact
            for job in jobs
            for artifact in stream_cadence_artifacts(job.stream, streams)
        }
        if not required_artifacts:
            return
        artifact_tasks, _ = operation_specs(project_path)
        graph = build_artifact_graph(artifact_tasks)
    except (OSError, TypeError, ValueError) as exc:
        raise MaterializeProfileError(
            f"Failed to prepare materialize artifacts: {exc}"
        ) from exc

    for artifact in sorted(required_artifacts):
        task = graph.tasks_by_id.get(artifact)
        if task is None:
            raise MaterializeProfileError(
                f"Artifact-backed cadence '{artifact}' requires a declared ticks "
                "task with the same id."
            )
        if not isinstance(task, TicksTask):
            raise MaterializeProfileError(
                f"Artifact-backed cadence '{artifact}' references task "
                f"entrypoint '{task.entrypoint}', not a ticks task."
            )

    spec = ExecutionSpec(
        visuals=settings.visuals,
        log_decision=settings.log_decision,
        log_output=settings.log_output,
        runtime=runtime,
    )

    def prepare() -> None:
        run_build_if_needed(
            project_path,
            graph=graph,
            required_artifacts=required_artifacts,
            mode=settings.mode,
            runtime=runtime,
            heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
        )

    run_execution(spec, prepare)
