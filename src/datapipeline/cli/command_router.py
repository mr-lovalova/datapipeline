from __future__ import annotations

import argparse
from pathlib import Path

from datapipeline.cli.commands.build import handle as handle_build
from datapipeline.cli.commands.contract import handle as handle_contract
from datapipeline.cli.commands.demo import handle as handle_demo
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.dto import handle as handle_dto
from datapipeline.cli.commands.filter import handle as handle_filter
from datapipeline.cli.commands.inspect import report as handle_inspect_report
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.loader import handle as handle_loader
from datapipeline.cli.commands.mapper import handle as handle_mapper
from datapipeline.cli.commands.parser import handle as handle_parser
from datapipeline.cli.commands.plugin import bar as handle_bar
from datapipeline.cli.commands.run import handle_serve
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.stream import handle as handle_stream
from datapipeline.config.resolution import LogOutputTarget, resolve_visuals
from datapipeline.config.workspace import WorkspaceContext


def _run_inspect_command(
    *,
    args: argparse.Namespace,
    shared_defaults,
    base_level: int,
    cli_log_outputs: list[LogOutputTarget],
    workspace_context: WorkspaceContext | None,
) -> None:
    subcmd = getattr(args, "inspect_cmd", None)
    shared_observability = shared_defaults.observability if shared_defaults else None
    shared_visuals_default = (
        shared_observability.visuals if shared_observability else None
    )
    inspect_visuals = resolve_visuals(
        cli_visuals=getattr(args, "visuals", None),
        config_visuals=None,
        workspace_visuals=shared_visuals_default,
    )
    inspect_visual_provider = inspect_visuals.visuals or "on"
    project = getattr(args, "project", None)
    base_kwargs = {
        "project": project,
        "threshold": getattr(args, "threshold", 0.95),
        "apply_postprocess": (getattr(args, "mode", "final") == "final"),
        "visuals": inspect_visual_provider,
        "log_level": base_level,
        "sort": getattr(args, "sort", "missing"),
        "cli_log_outputs": cli_log_outputs,
        "workspace": workspace_context,
    }
    if subcmd in (None, "report"):
        handle_inspect_report(
            **base_kwargs,
            match_partition=getattr(args, "match_partition", "base"),
            matrix="none",
            rows=20,
            cols=10,
            quiet=False,
        )
        return
    if subcmd == "matrix":
        handle_inspect_report(
            **base_kwargs,
            match_partition="base",
            matrix=getattr(args, "format", "html"),
            output=getattr(args, "output", None),
            rows=getattr(args, "rows", 20),
            cols=getattr(args, "cols", 10),
            quiet=getattr(args, "quiet", False),
        )
        return
    if subcmd == "partitions":
        from datapipeline.cli.commands.inspect import partitions as handle_inspect_partitions

        handle_inspect_partitions(
            project=project,
            output=getattr(args, "output", None),
            visuals=inspect_visual_provider,
            log_level=base_level,
            cli_log_outputs=cli_log_outputs,
            workspace=workspace_context,
        )


def _dispatch_non_project_command(
    *,
    args: argparse.Namespace,
    plugin_root: Path | None,
    workspace_context: WorkspaceContext | None,
) -> bool:
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
    named_handlers = {
        "dto": handle_dto,
        "parser": handle_parser,
        "mapper": handle_mapper,
        "loader": handle_loader,
    }
    handler = named_handlers.get(args.cmd)
    if handler is not None:
        handler(name=getattr(args, "name", None), plugin_root=plugin_root)
        return True
    if args.cmd == "inflow":
        handle_stream(plugin_root=plugin_root, workspace=workspace_context)
        return True
    if args.cmd == "contract":
        handle_contract(
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


def execute_command(
    *,
    args: argparse.Namespace,
    plugin_root: Path | None,
    workspace_context: WorkspaceContext | None,
    shared_defaults,
    base_level: int,
    cli_level_arg: str | None,
    base_level_name: str,
    cli_log_outputs: list[LogOutputTarget],
) -> bool:
    if args.cmd == "serve":
        handle_serve(
            project=args.project,
            limit=getattr(args, "limit", None),
            keep=getattr(args, "keep", None),
            run_name=getattr(args, "run", None),
            stage=getattr(args, "stage", None),
            output_transport=getattr(args, "output_transport", None),
            output_format=getattr(args, "output_format", None),
            output_directory=getattr(args, "output_directory", None),
            output_encoding=getattr(args, "output_encoding", None),
            output_view=getattr(args, "output_view", None),
            skip_build=getattr(args, "skip_build", False),
            cli_log_level=cli_level_arg,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_level_name,
            cli_visuals=getattr(args, "visuals", None),
            workspace=workspace_context,
        )
        return True
    if args.cmd == "build":
        handle_build(
            project=args.project,
            run_name=getattr(args, "run", None),
            force=getattr(args, "force", False),
            cli_log_level=cli_level_arg,
            cli_visuals=getattr(args, "visuals", None),
            cli_log_outputs=cli_log_outputs,
            workspace=workspace_context,
        )
        return True
    if args.cmd == "inspect":
        _run_inspect_command(
            args=args,
            shared_defaults=shared_defaults,
            base_level=base_level,
            cli_log_outputs=cli_log_outputs,
            workspace_context=workspace_context,
        )
        return True
    return _dispatch_non_project_command(
        args=args,
        plugin_root=plugin_root,
        workspace_context=workspace_context,
    )
