import argparse
from pathlib import Path

from datapipeline.cli.commands.clean import handle as handle_clean
from datapipeline.cli.commands.profile_runner import handle_profile_command
from datapipeline.cli.commands.demo import handle as handle_demo
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.dto import handle as handle_dto
from datapipeline.cli.commands.filter import handle as handle_filter
from datapipeline.cli.commands.inflow import handle as handle_inflow
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.loader import handle as handle_loader
from datapipeline.cli.commands.mapper import handle as handle_mapper
from datapipeline.cli.commands.materialize import handle as handle_materialize
from datapipeline.cli.commands.parser import handle as handle_parser
from datapipeline.cli.commands.plugin import bar as handle_bar
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.stream import handle as handle_stream_create
from datapipeline.cli.commands.version import handle as handle_version
from datapipeline.cli.commands.version import handle_env
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.workspace import WorkspaceContext

NAMED_HANDLERS = {
    "dto": handle_dto,
    "parser": handle_parser,
    "mapper": handle_mapper,
    "loader": handle_loader,
}

def execute_command(
    args: argparse.Namespace,
    plugin_root: Path | None,
    workspace_context: WorkspaceContext | None,
    cli_level_arg: str | None,
    base_level_name: str,
    cli_log_outputs: list[LogOutputTarget],
) -> bool:
    if args.cmd == "version":
        handle_version()
        return True
    if args.cmd == "env":
        handle_env()
        return True
    if args.cmd in {"serve", "build", "inspect"}:
        return handle_profile_command(
            kind=args.cmd,
            args=args,
            workspace_context=workspace_context,
            cli_level_arg=cli_level_arg,
            base_level_name=base_level_name,
            cli_log_outputs=cli_log_outputs,
        )
    if args.cmd == "clean":
        handle_clean(
            yes=getattr(args, "yes", False),
            older_than=getattr(args, "older_than", None),
        )
        return True
    if args.cmd == "materialize":
        handle_materialize(
            project=args.project,
            stream_id=args.stream_id,
            output=args.output,
            as_stream_id=getattr(args, "as_stream_id", None),
            force=getattr(args, "force", False),
            visuals=getattr(args, "visuals", None),
            heartbeat_interval_seconds=getattr(
                args,
                "heartbeat_interval_seconds",
                None,
            ),
            workspace=workspace_context,
        )
        return True
    if args.cmd == "source":
        if args.source_cmd == "list":
            handle_list(subcmd="sources", workspace=workspace_context)
            return True
        handle_source(
            subcmd=args.source_cmd,
            provider=(getattr(args, "provider", None) or getattr(args, "provider_opt", None)),
            dataset=(getattr(args, "dataset", None) or getattr(args, "dataset_opt", None)),
            transport=getattr(args, "transport", None),
            format=getattr(args, "format", None),
            alias=getattr(args, "alias", None),
            identity=getattr(args, "identity", False),
            loader=getattr(args, "loader", None),
            parser=getattr(args, "parser", None),
            plugin_root=plugin_root,
            workspace=workspace_context,
        )
        return True
    if args.cmd == "list":
        handle_list(subcmd=args.list_cmd, workspace=workspace_context)
        return True
    if args.cmd == "domain":
        if args.domain_cmd == "list":
            handle_list(subcmd="domains")
            return True
        handle_domain(
            subcmd=args.domain_cmd,
            domain=getattr(args, "domain", None),
            plugin_root=plugin_root,
        )
        return True
    handler = NAMED_HANDLERS.get(args.cmd)
    if handler is not None:
        handler(name=getattr(args, "name", None), plugin_root=plugin_root)
        return True
    if args.cmd == "inflow":
        handle_inflow(plugin_root=plugin_root, workspace=workspace_context)
        return True
    if args.cmd == "stream":
        handle_stream_create(
            plugin_root=plugin_root,
            use_identity=getattr(args, "identity", False),
            workspace=workspace_context,
        )
        return True
    if args.cmd == "plugin":
        handle_bar(
            subcmd=args.bar_cmd,
            name=getattr(args, "name", None),
            out=getattr(args, "out", "."),
            workspace=workspace_context,
        )
        return True
    if args.cmd == "demo":
        handle_demo(
            subcmd=args.demo_cmd,
            out=getattr(args, "out", None),
            workspace=workspace_context,
        )
        return True
    if args.cmd == "filter":
        handle_filter(subcmd=args.filter_cmd, name=getattr(args, "name", None))
        return True
    return False
