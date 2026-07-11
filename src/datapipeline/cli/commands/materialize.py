import logging
from pathlib import Path

from datapipeline.cli.visuals.runner import run_with_backend
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.profiles.materialize import (
    MaterializeProfileError,
    run_materialize_profiles,
)
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.materialize import (
    MaterializeResult,
    materialize_stream_to_path,
    validate_materialize_output_path,
)
from datapipeline.services.path_policy import resolve_workspace_path

logger = logging.getLogger(__name__)


def handle_stream(
    project: str,
    stream_id: str,
    output: str,
    as_stream_id: str | None,
    overwrite: bool | None,
    visuals: str | None,
    heartbeat_interval_seconds: float | None,
    workspace: WorkspaceContext | None,
) -> None:
    project_path = Path(project).resolve()
    output_path = resolve_workspace_path(
        output,
        workspace.root if workspace is not None else None,
    )
    try:
        validate_materialize_output_path(output_path)
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from None
    runtime = bootstrap(project_path)
    if stream_id not in set(runtime.registries.stream_specs.keys()):
        logger.error("Unknown stream '%s'.", stream_id)
        raise SystemExit(2)
    if heartbeat_interval_seconds is not None:
        runtime.heartbeat_interval_seconds = heartbeat_interval_seconds
    dataset = load_dataset(project_path, "vectors")
    try:
        result = run_with_backend(
            visuals=visuals or "on",
            runtime=runtime,
            level=logger.getEffectiveLevel(),
            work=lambda: materialize_stream_to_path(
                runtime=runtime,
                stream_id=stream_id,
                output=output_path,
                as_stream_id=as_stream_id,
                overwrite=bool(overwrite),
                dataset=dataset,
            ),
        )
    except FileExistsError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from None
    _print_result(stream_id, result)


def handle_profiles(
    project: str,
    run_name: str | None,
    overwrite: bool | None,
    visuals: str | None,
    heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: list[LogOutputTarget],
    base_log_level: str,
) -> None:
    try:
        results = run_materialize_profiles(
            project_path=Path(project).resolve(),
            run_name=run_name,
            overwrite=overwrite,
            cli_visuals=visuals,
            cli_heartbeat_interval_seconds=heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
    except (FileExistsError, MaterializeProfileError) as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from None
    if not results:
        logger.info("No enabled materialize profiles; skipping materialize.")
        return
    for result in results:
        print(f"Materialized: {result.output} ({result.count} records)")
        _print_written_files(result)


def _print_result(stream_id: str, result: MaterializeResult) -> None:
    print(f"Materialized {stream_id}: {result.output} ({result.count} records)")
    _print_written_files(result)


def _print_written_files(result: MaterializeResult) -> None:
    print(f"Wrote metadata: {result.metadata}")
    if result.source_config is not None:
        print(f"Wrote source config: {result.source_config}")
    if result.ingest_config is not None:
        print(f"Wrote ingest config: {result.ingest_config}")
