import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

from datapipeline.cli.command_router import execute_command
from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.parser_builder import build_parser
from datapipeline.config.resolution import (
    LogOutputTarget,
    parse_log_output_specs,
    resolve_log_output,
)
from datapipeline.config.workspace import WorkspaceContext, load_workspace_context
from datapipeline.services.path_policy import resolve_workspace_path, workspace_cwd
from datapipeline.utils.rich_compat import suppress_file_proxy_shutdown_errors

suppress_file_proxy_shutdown_errors()


def _dataset_to_project_path(
    dataset: str,
    workspace: Optional[WorkspaceContext],
) -> str:
    """Resolve a dataset selector (alias, folder, or file) into a project.yaml path."""
    if workspace is not None:
        resolved = workspace.resolve_dataset_alias(dataset)
        if resolved is not None:
            return str(resolved)

    path = Path(dataset)
    if path.suffix in {".yaml", ".yml"}:
        return str(
            resolve_workspace_path(
                path,
                workspace.root if workspace is not None else None,
            )
        )

    candidate_dir = resolve_workspace_path(
        path,
        workspace.root if workspace is not None else None,
    )
    if candidate_dir.is_dir():
        candidate = candidate_dir / "project.yaml"
        return str(candidate.resolve())

    raise SystemExit(
        f"Unknown dataset '{dataset}'. Define it under datasets: in jerry.yaml or pass a valid path."
    )


def _resolve_project_from_args(
    project: Optional[str],
    dataset: Optional[str],
    workspace: Optional[WorkspaceContext],
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve final project path from --project / --dataset / jerry.yaml defaults."""
    explicit_project = project is not None
    explicit_dataset = dataset is not None

    if explicit_project and explicit_dataset:
        raise SystemExit("Cannot use both --project and --dataset; pick one.")

    if explicit_dataset:
        resolved = _dataset_to_project_path(dataset, workspace)
        return resolved, dataset

    if not explicit_project and workspace is not None:
        default_ds = getattr(workspace.config, "default_dataset", None)
        if default_ds:
            resolved = _dataset_to_project_path(default_ds, workspace)
            return resolved, default_ds

    if explicit_project:
        return project, dataset

    raise SystemExit(
        "No dataset/project selected. Use --dataset <name|path>, --project <path>, "
        "or define default_dataset in jerry.yaml."
    )


def _resolve_project_arguments(
    
    args: argparse.Namespace,
    workspace_context: WorkspaceContext | None,
) -> None:
    if args.cmd not in {"serve", "build", "inspect", "materialize"}:
        return
    if not (hasattr(args, "project") or hasattr(args, "dataset")):
        return
    raw_project = getattr(args, "project", None)
    raw_dataset = getattr(args, "dataset", None)
    resolved_project, resolved_dataset = _resolve_project_from_args(
        raw_project,
        raw_dataset,
        workspace_context,
    )
    if hasattr(args, "project"):
        args.project = resolved_project
    if hasattr(args, "dataset"):
        args.dataset = resolved_dataset


def _configure_cli_logging(
    
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    workspace_context: WorkspaceContext | None,
) -> tuple[str | None, str, list[LogOutputTarget]]:
    cli_level_arg = getattr(args, "log_level", None)
    cli_log_output_specs = getattr(args, "log_output", None)

    base_level_name = (cli_level_arg or "INFO").upper()
    base_level = logging._nameToLevel.get(base_level_name, logging.WARNING)

    try:
        cli_log_outputs = parse_log_output_specs(
            cli_log_output_specs,
            resolve_global_path=lambda value: resolve_workspace_path(
                value,
                workspace_context.root if workspace_context is not None else None,
            ),
        )
        base_log_output = resolve_log_output(
            output_candidates=(
                [item for item in cli_log_outputs if item.scope != "execution"],
            ),
            allow_execution_scope=False,
        )
    except ValueError as exc:
        parser.error(str(exc))
        raise SystemExit(2) from exc

    configure_root_logging(level=base_level, output=base_log_output)
    return cli_level_arg, base_level_name, cli_log_outputs


def main() -> None:
    parser = build_parser()
    workspace_context = load_workspace_context(workspace_cwd())
    args = parser.parse_args()
    _resolve_project_arguments(args=args, workspace_context=workspace_context)
    (
        cli_level_arg,
        base_level_name,
        cli_log_outputs,
    ) = _configure_cli_logging(
        parser=parser,
        args=args,
        workspace_context=workspace_context,
    )
    plugin_root = (
        workspace_context.resolve_plugin_root() if workspace_context else None
    )

    try:
        handled = execute_command(
            args=args,
            plugin_root=plugin_root,
            workspace_context=workspace_context,
            cli_level_arg=cli_level_arg,
            base_level_name=base_level_name,
            cli_log_outputs=cli_log_outputs,
        )
        if handled:
            return
    except KeyboardInterrupt:
        message = (
            "Serve interrupted by user" if args.cmd == "serve" else "Interrupted by user"
        )
        print(message, file=sys.stderr)
        raise SystemExit(130) from None
