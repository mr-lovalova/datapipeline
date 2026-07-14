import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Sequence

from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.profiles import MaterializeProfile
from datapipeline.execution.settings import (
    LogOutputTarget,
    resolve_execution_log_outputs,
    resolve_observability_settings,
)
from datapipeline.execution.observability import emit_file_result, operation_scope
from datapipeline.profiles.models import MaterializeJob
from datapipeline.runtime import Runtime
from datapipeline.services.materialize import (
    check_materialize_destinations,
    materialize_destination_paths,
    materialize_stream_to_path,
)
from datapipeline.services.path_policy import sanitize_path_segment


def resolve_materialize_jobs(
    profiles: Sequence[MaterializeProfile],
    project_path: Path,
    execution_dir: Path,
    overwrite: bool | None,
    cli_output: Path | None,
    cli_visuals: str | None,
    cli_heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: Sequence[LogOutputTarget],
    base_log_level: str,
) -> list[MaterializeJob]:
    if cli_output is not None and len(profiles) != 1:
        raise ValueError("A materialize output override requires one selected profile.")

    jobs: list[MaterializeJob] = []
    for profile in profiles:
        observability = resolve_observability_settings(
            project_path,
            profile.observability,
            cli_visuals=cli_visuals,
            cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
        observability = replace(
            observability,
            log_output=resolve_execution_log_outputs(
                observability.log_output,
                execution_dir,
                default_path=(
                    Path("logs")
                    / f"materialize.{sanitize_path_segment(profile.name)}.log"
                ),
            ),
        )
        output = cli_output if cli_output is not None else profile.output
        if not output.is_absolute():
            output = project_path.parent / output
        jobs.append(
            MaterializeJob(
                name=profile.name,
                stream=profile.stream,
                output=output.resolve(),
                overwrite=profile.overwrite if overwrite is None else overwrite,
                observability=observability,
            )
        )
    return jobs


def preflight_materialize_jobs(
    runtime: Runtime,
    jobs: Sequence[MaterializeJob],
) -> None:
    destinations: list[tuple[MaterializeJob, tuple[Path, Path]]] = []
    owners: dict[Path, str] = {}
    available_streams = set(runtime.streams)
    artifacts_root = runtime.artifacts_root.resolve()
    for job in jobs:
        if job.stream not in available_streams:
            raise ValueError(
                f"Materialize profile '{job.name}' references unknown "
                f"stream '{job.stream}'."
            )
        paths = materialize_destination_paths(job.output)
        destinations.append((job, paths))
        for path in paths:
            if path.is_relative_to(artifacts_root):
                raise ValueError(
                    f"Materialize profile '{job.name}' writes inside the managed "
                    f"artifacts root: {path}"
                )
            owner = owners.get(path)
            if owner is not None:
                raise ValueError(
                    f"Materialize profiles '{owner}' and '{job.name}' "
                    f"write the same path: {path}"
                )
            owners[path] = job.name

    for job, paths in destinations:
        check_materialize_destinations(paths, job.overwrite)


def execute_materialize_job(
    job: MaterializeJob,
    runtime: Runtime,
) -> None:
    with operation_scope(f"materialize:{job.name}"):
        emit_execution_message(
            "Config:\n"
            + json.dumps(
                {
                    "stream": job.stream,
                    "output": str(job.output),
                    "overwrite": job.overwrite,
                    "execution": runtime.execution.model_dump(mode="json"),
                    "observability": job.observability.effective_config(),
                },
                indent=2,
            ),
            level=logging.DEBUG,
        )
        result = materialize_stream_to_path(
            runtime=runtime,
            stream_id=job.stream,
            output=job.output,
            overwrite=job.overwrite,
        )
        emit_file_result("Output", result.output)
        emit_file_result("Metadata", result.metadata)
