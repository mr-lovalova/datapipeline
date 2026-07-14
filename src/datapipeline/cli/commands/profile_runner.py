import argparse
import logging

from datapipeline.cli.output_options import build_cli_output_config
from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.execution.settings import LogOutputTarget
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.profiles.request_builder import (
    build_build_run_request,
    build_runtime_run_request,
)

logger = logging.getLogger(__name__)


def handle_build(
    args: argparse.Namespace,
    cli_log_level: str | None,
    base_log_level: str,
    cli_log_outputs: list[LogOutputTarget],
) -> None:
    request = build_build_run_request(
        project=args.project,
        profile_name=args.profile,
        force=args.force,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
        cli_visuals=args.visuals,
        cli_heartbeat_interval_seconds=args.heartbeat_interval_seconds,
    )
    if request is None:
        logger.info("No enabled build profiles; skipping build.")
        return
    run_profiles(request)


def handle_serve(
    args: argparse.Namespace,
    workspace: WorkspaceContext | None,
    cli_log_level: str | None,
    base_log_level: str,
    cli_log_outputs: list[LogOutputTarget],
) -> None:
    workspace_root = workspace.root if workspace is not None else None
    output = build_cli_output_config(
        transport=args.output_transport,
        fmt=args.output_format,
        directory=args.output_directory,
        output_encoding=args.output_encoding,
        workspace_root=workspace_root,
        view=args.output_view,
    )
    request = build_runtime_run_request(
        command="serve",
        project=args.project,
        profile_name=args.profile,
        artifact_mode=args.artifact_mode,
        limit=args.limit,
        preview=args.preview,
        cli_output=output,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
        cli_visuals=args.visuals,
        cli_heartbeat_interval_seconds=args.heartbeat_interval_seconds,
    )
    if request is None:
        logger.info("No enabled serve profiles; skipping serve.")
        return
    run_profiles(request)


def handle_inspect(
    args: argparse.Namespace,
    workspace: WorkspaceContext | None,
    cli_log_level: str | None,
    base_log_level: str,
    cli_log_outputs: list[LogOutputTarget],
) -> None:
    workspace_root = workspace.root if workspace is not None else None
    output = build_cli_output_config(
        transport=args.output_transport,
        fmt=args.output_format,
        directory=args.output_directory,
        output_encoding=args.output_encoding,
        workspace_root=workspace_root,
        view=args.output_view,
    )
    request = build_runtime_run_request(
        command="inspect",
        project=args.project,
        profile_name=args.profile,
        artifact_mode=args.artifact_mode,
        limit=args.limit,
        cli_output=output,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
        cli_visuals=args.visuals,
        cli_heartbeat_interval_seconds=args.heartbeat_interval_seconds,
    )
    if request is None:
        logger.info("No enabled inspect profiles; skipping inspect.")
        return
    run_profiles(request)
