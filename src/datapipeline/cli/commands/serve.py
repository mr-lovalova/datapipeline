import argparse

from datapipeline.cli.commands.profile_runner import handle_profile_command
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.workspace import WorkspaceContext


def handle(
    *,
    args: argparse.Namespace,
    workspace_context: WorkspaceContext | None,
    cli_level_arg: str | None,
    base_level_name: str,
    cli_log_outputs: list[LogOutputTarget],
) -> bool:
    return handle_profile_command(
        kind="serve",
        args=args,
        workspace_context=workspace_context,
        cli_level_arg=cli_level_arg,
        base_level_name=base_level_name,
        cli_log_outputs=cli_log_outputs,
    )

