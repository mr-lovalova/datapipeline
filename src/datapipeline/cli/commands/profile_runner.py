import argparse
import logging

from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.execution.settings import LogOutputTarget
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.profiles.models import TaskProfileKind
from datapipeline.profiles.request_builder import build_profile_run_request

logger = logging.getLogger(__name__)


def handle_profile_command(
    *,
    kind: TaskProfileKind,
    args: argparse.Namespace,
    workspace_context: WorkspaceContext | None,
    cli_level_arg: str | None,
    base_level_name: str,
    cli_log_outputs: list[LogOutputTarget],
) -> bool:
    is_runtime = kind != "build"
    request = build_profile_run_request(
        kind=kind,
        project=args.project,
        run_name=args.run,
        force=args.force if kind == "build" else False,
        artifact_mode=args.artifact_mode if is_runtime else None,
        limit=args.limit if is_runtime else None,
        preview=args.preview if kind == "serve" else None,
        output_transport=args.output_transport if is_runtime else None,
        output_format=args.output_format if is_runtime else None,
        output_directory=args.output_directory if is_runtime else None,
        output_encoding=args.output_encoding if is_runtime else None,
        output_view=args.output_view if is_runtime else None,
        cli_log_level=cli_level_arg,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_level_name,
        cli_visuals=args.visuals,
        cli_heartbeat_interval_seconds=args.heartbeat_interval_seconds,
        workspace=workspace_context,
    )
    if request is None:
        logger.info("No enabled %s profiles; skipping %s.", kind, kind)
        return True
    run_profiles(request)
    return True
