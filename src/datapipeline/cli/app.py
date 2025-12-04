import argparse
import logging
from pathlib import Path
from typing import Optional, Tuple

from datapipeline.cli.commands.run import handle_serve
from datapipeline.cli.commands.plugin import bar as handle_bar
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.contract import handle as handle_contract
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.filter import handle as handle_filter
from datapipeline.cli.commands.inspect import (
    report as handle_inspect_report,
)
from datapipeline.cli.commands.build import handle as handle_build
from datapipeline.config.workspace import (
    WorkspaceContext,
    load_workspace_context,
)
from datapipeline.config.resolution import resolve_visuals
from datapipeline.utils.rich_compat import suppress_file_proxy_shutdown_errors

suppress_file_proxy_shutdown_errors()


def _dataset_to_project_path(
    dataset: str,
    workspace: Optional[WorkspaceContext],
) -> str:
    """Resolve a dataset selector (alias, folder, or file) into a project.yaml path."""
    # 1) Alias via jerry.yaml datasets (wins over local folders with same name)
    if workspace is not None:
        datasets = getattr(workspace.config, "datasets", {}) or {}
        raw = datasets.get(dataset)
        if raw:
            base = workspace.root
            candidate = Path(raw)
            candidate = candidate if candidate.is_absolute() else (base / candidate)
            if candidate.is_dir():
                candidate = candidate / "project.yaml"
            return str(candidate.resolve())

    # 2) Direct file path
    path = Path(dataset)
    if path.suffix in {".yaml", ".yml"}:
        return str(path if path.is_absolute() else (Path.cwd() / path).resolve())

    # 3) Directory: assume project.yaml inside
    if path.is_dir():
        candidate = path / "project.yaml"
        return str(candidate.resolve())

    raise SystemExit(f"Unknown dataset '{dataset}'. Define it under datasets: in jerry.yaml or pass a valid path.")


def _resolve_project_from_args(
    project: Optional[str],
    dataset: Optional[str],
    workspace: Optional[WorkspaceContext],
) -> Tuple[Optional[str], Optional[str]]:
    """Resolve final project path from --project / --dataset / jerry.yaml defaults.

    Rules:
    - If both project and dataset are explicitly given (and project != DEFAULT_PROJECT_PATH), error.
    - If dataset is given, resolve it to a project path (alias, dir, or file).
    - If neither is given (or project==DEFAULT_PROJECT_PATH), and jerry.yaml declares default_dataset,
      resolve that alias.
    - Otherwise fall back to legacy DEFAULT_PROJECT_PATH resolution.
    """
    explicit_project = project is not None
    explicit_dataset = dataset is not None

    if explicit_project and explicit_dataset:
        raise SystemExit("Cannot use both --project and --dataset; pick one.")

    # Prefer dataset when provided
    if explicit_dataset:
        resolved = _dataset_to_project_path(dataset, workspace)
        return resolved, dataset

    # No explicit dataset; use default_dataset from workspace when project is not explicitly set
    if not explicit_project and workspace is not None:
        default_ds = getattr(workspace.config, "default_dataset", None)
        if default_ds:
            resolved = _dataset_to_project_path(default_ds, workspace)
            return resolved, default_ds

    # If project was given explicitly, use it as-is (caller is responsible for validity).
    if explicit_project:
        return project, dataset

    # Nothing resolved: require explicit selection.
    raise SystemExit(
        "No dataset/project selected. Use --dataset <name|path>, --project <path>, "
        "or define default_dataset in jerry.yaml."
    )


def main() -> None:
    # Common options shared by top-level and subcommands
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        help="set logging level (default: WARNING)",
    )

    parser = argparse.ArgumentParser(
        prog="jerry",
        description="Mixology-themed CLI for building and serving data pipelines.",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # serve (production run, configurable logging)
    p_serve = sub.add_parser(
        "serve",
        help="produce vectors with configurable logging",
        parents=[common],
    )
    p_serve.add_argument(
        "--dataset",
        "-d",
        help="dataset alias, folder, or project.yaml path",
    )
    p_serve.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )
    p_serve.add_argument(
        "--limit", "-n", type=int, default=None,
        help="optional cap on the number of vectors to emit",
    )
    p_serve.add_argument(
        "--out-transport",
        choices=["stdout", "fs"],
        help="output transport (stdout or fs) for serve runs",
    )
    p_serve.add_argument(
        "--out-format",
        choices=["print", "json-lines", "json", "csv", "pickle"],
        help="output format (print/json-lines/csv/pickle) for serve runs",
    )
    p_serve.add_argument(
        "--out-payload",
        choices=["sample", "vector"],
        help="payload structure: full sample (default) or vector-only body",
    )
    p_serve.add_argument(
        "--out-path",
        help="destination file path when using fs transport",
    )
    p_serve.add_argument(
        "--keep",
        help="split label to serve; overrides serve tasks and project globals",
    )
    p_serve.add_argument(
        "--run",
        help="select a serve task by name when project.paths.tasks contains multiple entries",
    )
    p_serve.add_argument(
        "--stage",
        "-s",
        type=int,
        choices=range(0, 8),
        default=None,
        help="preview a specific pipeline stage (0-5 feature stages, 6 assembled vectors, 7 transformed vectors)",
    )
    p_serve.add_argument(
        "--visuals",
        choices=["auto", "tqdm", "rich", "off"],
        default=None,
        help="visuals renderer: auto (default), tqdm, rich, or off",
    )
    p_serve.add_argument(
        "--progress",
        choices=["auto", "spinner", "bars", "off"],
        default=None,
        help="progress display: auto (spinner unless DEBUG), spinner, bars, or off",
    )
    p_serve.add_argument(
        "--skip-build",
        action="store_true",
        help="skip the automatic build step (useful for quick feature previews)",
    )

    # build (materialize artifacts)
    p_build = sub.add_parser(
        "build",
        help="materialize project artifacts (expected ids, hashes, etc.)",
        parents=[common],
    )
    p_build.add_argument(
        "--dataset",
        "-d",
        help="dataset alias, folder, or project.yaml path",
    )
    p_build.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )
    p_build.add_argument(
        "--force",
        action="store_true",
        help="rebuild even when the configuration hash matches the last run",
    )
    p_build.add_argument(
        "--visuals",
        choices=["auto", "tqdm", "rich", "off"],
        default=None,
        help="visuals renderer: auto (default), tqdm, rich, or off",
    )
    p_build.add_argument(
        "--progress",
        choices=["auto", "spinner", "bars", "off"],
        default=None,
        help="progress display: auto (spinner unless DEBUG), spinner, bars, or off",
    )

    # source
    p_source = sub.add_parser(
        "source",
        help="add or list raw sources",
        parents=[common],
    )
    source_sub = p_source.add_subparsers(dest="source_cmd", required=True)
    p_source_add = source_sub.add_parser(
        "add",
        help="create a provider+dataset source",
        description=(
            "Scaffold a source using transport + format.\n\n"
            "Usage:\n"
            "  jerry source add <provider> <dataset> -t fs -f csv\n"
            "  jerry source add <provider>.<dataset> -t http -f json\n"
            "  jerry source add -p <provider> -d <dataset> -t synthetic\n\n"
            "Examples:\n"
            "  fs CSV:        -t fs  -f csv\n"
            "  fs NDJSON:     -t fs  -f json-lines\n"
            "  HTTP JSON:     -t http -f json\n"
            "  Synthetic:     -t synthetic\n\n"
            "Note: set 'glob: true' in the generated YAML if your 'path' contains wildcards."
        ),
    )
    # Support simple positionals, plus flags for compatibility
    # Allow either positionals or flags. Use distinct dest names for flags
    # to avoid ambiguity when both forms are present in some environments.
    p_source_add.add_argument("provider", nargs="?", help="provider name")
    p_source_add.add_argument("dataset", nargs="?", help="dataset slug")
    p_source_add.add_argument("--provider", "-p", dest="provider_opt", metavar="PROVIDER", help="provider name")
    p_source_add.add_argument("--dataset", "-d", dest="dataset_opt", metavar="DATASET", help="dataset slug")
    p_source_add.add_argument("--alias", "-a", help="provider.dataset alias")
    p_source_add.add_argument(
        "--transport", "-t",
        choices=["fs", "http", "synthetic"],
        required=True,
        help="how data is accessed: fs/http/synthetic",
    )
    p_source_add.add_argument(
        "--format", "-f",
        choices=["csv", "json", "json-lines", "pickle"],
        help="data format for fs/http transports (ignored otherwise)",
    )
    p_source_add.add_argument(
        "--identity",
        action="store_true",
        help="use the built-in identity parser (skips DTO/parser scaffolding)",
    )
    source_sub.add_parser("list", help="list known sources")

    # domain
    p_domain = sub.add_parser(
        "domain",
        help="add or list domains",
        parents=[common],
    )
    domain_sub = p_domain.add_subparsers(dest="domain_cmd", required=True)
    p_domain_add = domain_sub.add_parser(
        "add",
        help="create a domain",
        description="Create a time-aware domain package rooted in TemporalRecord.",
    )
    # Accept positional name, plus flags for flexibility and consistency.
    p_domain_add.add_argument("domain", nargs="?", help="domain name")
    p_domain_add.add_argument(
        "--name", "-n", dest="domain", help="domain name"
    )
    domain_sub.add_parser("list", help="list known domains")

    # contract (interactive: ingest or composed)
    p_contract = sub.add_parser(
        "contract",
        help="manage stream contracts (ingest or composed)",
        parents=[common],
    )
    p_contract.add_argument(
        "--identity",
        action="store_true",
        help="use built-in identity mapper (skip mapper scaffolding)",
    )

    # plugin (plugin scaffolding)
    p_bar = sub.add_parser(
        "plugin",
        help="scaffold plugin workspaces",
        parents=[common],
    )
    bar_sub = p_bar.add_subparsers(dest="bar_cmd", required=True)
    p_bar_init = bar_sub.add_parser(
        "init", help="create a plugin skeleton")
    # Accept positional name and flag for flexibility
    p_bar_init.add_argument("name", nargs="?", help="plugin distribution name")
    p_bar_init.add_argument("--name", "-n", dest="name", help="plugin distribution name")
    p_bar_init.add_argument("--out", "-o", default=".")

    # filter (unchanged helper)
    p_filt = sub.add_parser("filter", help="manage filters", parents=[common])
    filt_sub = p_filt.add_subparsers(dest="filter_cmd", required=True)
    p_filt_create = filt_sub.add_parser(
        "create", help="create a filter function")
    p_filt_create.add_argument(
        "--name", "-n", required=True,
        help="filter entrypoint name and function/module name",
    )

    # Shared visuals/progress controls for inspect commands
    inspect_common = argparse.ArgumentParser(add_help=False)
    inspect_common.add_argument(
        "--visuals",
        choices=["auto", "tqdm", "rich", "off"],
        default=None,
        help="visuals renderer: auto (default), tqdm, rich, or off",
    )
    inspect_common.add_argument(
        "--progress",
        choices=["auto", "spinner", "bars", "off"],
        default=None,
        help="progress display: auto (spinner unless DEBUG), spinner, bars, or off",
    )
    inspect_common.add_argument(
        "--dataset",
        "-d",
        help="dataset alias, folder, or project.yaml path",
    )

    # inspect (metadata helpers)
    p_inspect = sub.add_parser(
        "inspect",
        help="inspect dataset metadata: report, matrix, partitions",
        parents=[common, inspect_common],
    )
    inspect_sub = p_inspect.add_subparsers(dest="inspect_cmd", required=False)

    # Report (stdout only)
    p_inspect_report = inspect_sub.add_parser(
        "report",
        help="print a quality report to stdout",
        parents=[inspect_common],
    )
    p_inspect_report.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )
    p_inspect_report.add_argument(
        "--threshold",
        "-t",
        type=float,
        default=0.95,
        help="coverage threshold (0-1) for keep/drop lists",
    )
    p_inspect_report.add_argument(
        "--match-partition",
        choices=["base", "full"],
        default="base",
        help="match features by base id or full partition id",
    )
    p_inspect_report.add_argument(
        "--mode",
        choices=["final", "raw"],
        default="final",
        help="whether to apply postprocess transforms (final) or skip them (raw)",
    )
    p_inspect_report.add_argument(
        "--sort",
        choices=["missing", "nulls"],
        default="missing",
        help="feature ranking metric in the report (missing or nulls)",
    )

    # Matrix export
    p_inspect_matrix = inspect_sub.add_parser(
        "matrix",
        help="export availability matrix",
        parents=[inspect_common],
    )
    p_inspect_matrix.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )
    p_inspect_matrix.add_argument(
        "--threshold",
        "-t",
        type=float,
        default=0.95,
        help="coverage threshold (used in the report)",
    )
    p_inspect_matrix.add_argument(
        "--rows",
        type=int,
        default=20,
        help="max number of group buckets in the matrix (0 = all)",
    )
    p_inspect_matrix.add_argument(
        "--cols",
        type=int,
        default=10,
        help="max number of features/partitions in the matrix (0 = all)",
    )
    p_inspect_matrix.add_argument(
        "--format",
        choices=["csv", "html"],
        default="html",
        help="output format for the matrix",
    )
    p_inspect_matrix.add_argument(
        "--output",
        default=None,
        help="destination for the matrix (defaults to build/matrix.<fmt>)",
    )
    p_inspect_matrix.add_argument(
        "--quiet",
        action="store_true",
        help="suppress detailed console report; only print save messages",
    )
    p_inspect_matrix.add_argument(
        "--mode",
        choices=["final", "raw"],
        default="final",
        help="whether to apply postprocess transforms (final) or skip them (raw)",
    )

    # Partitions manifest subcommand
    p_inspect_parts = inspect_sub.add_parser(
        "partitions",
        help="discover partitions and write a manifest JSON",
        parents=[inspect_common],
    )
    p_inspect_parts.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )
    p_inspect_parts.add_argument(
        "--output",
        "-o",
        default=None,
        help="partitions manifest path (defaults to build/partitions.json)",
    )

    # Expected IDs (newline list)
    p_inspect_expected = inspect_sub.add_parser(
        "expected",
        help="discover full feature ids and write a newline list",
        parents=[inspect_common],
    )
    p_inspect_expected.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )
    p_inspect_expected.add_argument(
        "--output",
        "-o",
        default=None,
        help="expected ids output path (defaults to build/datasets/<name>/expected.txt)",
    )

    workspace_context = load_workspace_context(Path.cwd())
    args = parser.parse_args()

    # Resolve dataset/project selection for commands that use a project.
    if hasattr(args, "project") or hasattr(args, "dataset"):
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

    cli_level_arg = getattr(args, "log_level", None)
    shared_defaults = workspace_context.config.shared if workspace_context else None
    # Default logging level: CLI flag > jerry.yaml shared.log_level > INFO
    default_level_name = (
        shared_defaults.log_level.upper()
        if shared_defaults and shared_defaults.log_level
        else "INFO"
    )
    base_level_name = (cli_level_arg or default_level_name).upper()
    base_level = logging._nameToLevel.get(base_level_name, logging.WARNING)

    logging.basicConfig(level=base_level, format="%(message)s")
    plugin_root = (
        workspace_context.resolve_plugin_root() if workspace_context else None
    )

    if args.cmd == "serve":
        handle_serve(
            project=args.project,
            limit=getattr(args, "limit", None),
            keep=getattr(args, "keep", None),
            run_name=getattr(args, "run", None),
            stage=getattr(args, "stage", None),
            out_transport=getattr(args, "out_transport", None),
            out_format=getattr(args, "out_format", None),
            out_payload=getattr(args, "out_payload", None),
            out_path=getattr(args, "out_path", None),
            skip_build=getattr(args, "skip_build", False),
            cli_log_level=cli_level_arg,
            base_log_level=base_level_name,
            cli_visuals=getattr(args, "visuals", None),
            cli_progress=getattr(args, "progress", None),
            workspace=workspace_context,
        )
        return
    if args.cmd == "build":
        handle_build(
            project=args.project,
            force=getattr(args, "force", False),
            cli_visuals=getattr(args, "visuals", None),
            cli_progress=getattr(args, "progress", None),
            workspace=workspace_context,
        )
        return

    if args.cmd == "inspect":
        # Default to 'report' when no subcommand is given
        subcmd = getattr(args, "inspect_cmd", None)
        shared_visuals_default = shared_defaults.visuals if shared_defaults else None
        shared_progress_default = shared_defaults.progress if shared_defaults else None
        inspect_visuals = resolve_visuals(
            cli_visuals=getattr(args, "visuals", None),
            config_visuals=None,
            workspace_visuals=shared_visuals_default,
            cli_progress=getattr(args, "progress", None),
            config_progress=None,
            workspace_progress=shared_progress_default,
        )
        inspect_visual_provider = inspect_visuals.visuals or "auto"
        inspect_progress_style = inspect_visuals.progress or "auto"
        if subcmd in (None, "report"):
            handle_inspect_report(
                project=args.project,
                output=None,
                threshold=getattr(args, "threshold", 0.95),
                match_partition=getattr(args, "match_partition", "base"),
                matrix="none",
                matrix_output=None,
                rows=20,
                cols=10,
                quiet=False,
                write_coverage=False,
                apply_postprocess=(getattr(args, "mode", "final") == "final"),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
                sort=getattr(args, "sort", "missing"),
                workspace=workspace_context,
            )
        elif subcmd == "matrix":
            handle_inspect_report(
                project=args.project,
                output=None,
                threshold=getattr(args, "threshold", 0.95),
                match_partition="base",
                matrix=getattr(args, "format", "html"),
                matrix_output=getattr(args, "output", None),
                rows=getattr(args, "rows", 20),
                cols=getattr(args, "cols", 10),
                quiet=getattr(args, "quiet", False),
                write_coverage=False,
                apply_postprocess=(getattr(args, "mode", "final") == "final"),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
                sort=getattr(args, "sort", "missing"),
                workspace=workspace_context,
            )
        elif subcmd == "partitions":
            from datapipeline.cli.commands.inspect import partitions as handle_inspect_partitions
            handle_inspect_partitions(
                project=args.project,
                output=getattr(args, "output", None),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
                workspace=workspace_context,
            )
        elif subcmd == "expected":
            from datapipeline.cli.commands.inspect import expected as handle_inspect_expected
            handle_inspect_expected(
                project=args.project,
                output=getattr(args, "output", None),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
                workspace=workspace_context,
            )
        return

    if args.cmd == "source":
        if args.source_cmd == "list":
            handle_list(subcmd="sources")
        else:
            # Merge positionals and flags for provider/dataset
            handle_source(
                subcmd="add",
                provider=(getattr(args, "provider", None) or getattr(args, "provider_opt", None)),
                dataset=(getattr(args, "dataset", None) or getattr(args, "dataset_opt", None)),
                transport=getattr(args, "transport", None),
                format=getattr(args, "format", None),
                alias=getattr(args, "alias", None),
                identity=getattr(args, "identity", False),
                plugin_root=plugin_root,
            )
        return

    if args.cmd == "domain":
        if args.domain_cmd == "list":
            handle_list(subcmd="domains")
        else:
            handle_domain(
                subcmd="add",
                domain=getattr(args, "domain", None),
                plugin_root=plugin_root,
            )
        return

    if args.cmd == "contract":
        handle_contract(
            plugin_root=plugin_root,
            use_identity=args.identity,
        )
        return

    if args.cmd == "plugin":
        handle_bar(
            subcmd=args.bar_cmd,
            name=getattr(args, "name", None),
            out=getattr(args, "out", "."),
        )
        return

    if args.cmd == "filter":
        handle_filter(subcmd=args.filter_cmd, name=getattr(args, "name", None))
        return
