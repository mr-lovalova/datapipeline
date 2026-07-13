import argparse
import logging
from typing import Literal

from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.profiles.request_builder import build_profile_run_request

logger = logging.getLogger(__name__)
ProfileKind = Literal["serve", "build", "inspect"]


def handle_profile_command(
    *,
    kind: ProfileKind,
    args: argparse.Namespace,
    workspace_context: WorkspaceContext | None,
    cli_level_arg: str | None,
    base_level_name: str,
    cli_log_outputs: list[LogOutputTarget],
) -> bool:
    request = build_profile_run_request(
        kind=kind,
        project=args.project,
        run_name=getattr(args, "run", None),
        force=getattr(args, "force", False),
        artifact_mode=getattr(args, "artifact_mode", None),
        limit=getattr(args, "limit", None),
        preview=getattr(args, "preview", None),
        output_transport=getattr(args, "output_transport", None),
        output_format=getattr(args, "output_format", None),
        output_directory=getattr(args, "output_directory", None),
        output_encoding=getattr(args, "output_encoding", None),
        output_view=getattr(args, "output_view", None),
        cli_log_level=cli_level_arg,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_level_name,
        cli_visuals=getattr(args, "visuals", None),
        cli_heartbeat_interval_seconds=getattr(
            args,
            "heartbeat_interval_seconds",
            None,
        ),
        workspace=workspace_context,
    )
    if request is None:
        logger.info("No enabled %s profiles; skipping %s.", kind, kind)
        return True
    run_profiles(request)
    return True
