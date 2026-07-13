import logging
from pathlib import Path

from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.profiles.materialize import (
    MaterializeProfileError,
    run_materialize_profiles,
)
from datapipeline.services.materialize import validate_materialize_output_path
from datapipeline.services.path_policy import resolve_workspace_path

logger = logging.getLogger(__name__)


def handle(
    project: str,
    run_name: str | None,
    output: str | None,
    overwrite: bool | None,
    artifact_mode: str | None,
    visuals: str | None,
    heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: list[LogOutputTarget],
    base_log_level: str,
    workspace: WorkspaceContext | None,
) -> None:
    if run_name is None and output is not None:
        logger.error("--output requires --run")
        raise SystemExit(2)

    output_path = None
    try:
        if output is not None:
            output_path = resolve_workspace_path(
                output,
                workspace.root if workspace is not None else None,
            )
            validate_materialize_output_path(output_path)
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from None

    try:
        results = run_materialize_profiles(
            project_path=Path(project).resolve(),
            run_name=run_name,
            overwrite=overwrite,
            cli_artifact_mode=artifact_mode,
            cli_output=output_path,
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
