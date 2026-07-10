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
from datapipeline.cli.commands.plugin import handle as handle_plugin
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.stream import handle as handle_stream_create
from datapipeline.cli.commands.version import handle as handle_version
from datapipeline.cli.commands.version import handle_env
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.workspace import WorkspaceContext


def execute_command(
    args: argparse.Namespace,
    plugin_root: Path | None,
    workspace_context: WorkspaceContext | None,
    cli_level_arg: str | None,
    base_level_name: str,
    cli_log_outputs: list[LogOutputTarget],
) -> bool:
    match args.cmd:
        case "version":
            handle_version()
            return True
        case "env":
            handle_env()
            return True
        case "serve" | "build" | "inspect":
            return handle_profile_command(
                kind=args.cmd,
                args=args,
                workspace_context=workspace_context,
                cli_level_arg=cli_level_arg,
                base_level_name=base_level_name,
                cli_log_outputs=cli_log_outputs,
            )
        case "clean":
            handle_clean(yes=args.yes, older_than=args.older_than)
            return True
        case "materialize":
            handle_materialize(
                project=args.project,
                stream_id=args.stream_id,
                output=args.output,
                as_stream_id=args.as_stream_id,
                force=args.force,
                visuals=args.visuals,
                heartbeat_interval_seconds=args.heartbeat_interval_seconds,
                workspace=workspace_context,
            )
            return True
        case "source":
            if args.source_cmd == "list":
                handle_list(
                    subcmd="sources",
                    plugin_root=plugin_root,
                    workspace=workspace_context,
                )
                return True
            handle_source(
                subcmd=args.source_cmd,
                provider=args.provider or args.provider_opt,
                dataset=args.dataset or args.dataset_opt,
                transport=args.transport,
                format=args.format,
                alias=args.alias,
                identity=args.identity,
                loader=args.loader,
                parser=args.parser,
                plugin_root=plugin_root,
                workspace=workspace_context,
            )
            return True
        case "list":
            handle_list(
                subcmd=args.list_cmd,
                plugin_root=plugin_root,
                workspace=workspace_context,
            )
            return True
        case "domain":
            if args.domain_cmd == "list":
                handle_list(
                    subcmd="domains",
                    plugin_root=plugin_root,
                    workspace=workspace_context,
                )
                return True
            handle_domain(
                subcmd=args.domain_cmd,
                domain=args.domain_name or args.domain_name_option,
                plugin_root=plugin_root,
            )
            return True
        case "dto":
            handle_dto(name=args.name, plugin_root=plugin_root)
            return True
        case "parser":
            handle_parser(name=args.name, plugin_root=plugin_root)
            return True
        case "mapper":
            handle_mapper(name=args.name, plugin_root=plugin_root)
            return True
        case "loader":
            handle_loader(name=args.name, plugin_root=plugin_root)
            return True
        case "inflow":
            handle_inflow(plugin_root=plugin_root, workspace=workspace_context)
            return True
        case "stream":
            handle_stream_create(
                plugin_root=plugin_root,
                use_identity=args.identity,
                workspace=workspace_context,
            )
            return True
        case "plugin":
            handle_plugin(
                subcmd=args.plugin_cmd,
                name=args.plugin_name or args.plugin_name_option,
                out=args.out,
                workspace=workspace_context,
            )
            return True
        case "demo":
            handle_demo(
                subcmd=args.demo_cmd,
                out=args.out,
                workspace=workspace_context,
            )
            return True
        case "filter":
            handle_filter(
                subcmd=args.filter_cmd,
                name=args.name,
                plugin_root=plugin_root,
            )
            return True
        case _:
            return False
