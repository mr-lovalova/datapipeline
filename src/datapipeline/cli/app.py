import argparse
import logging
from pathlib import Path
from typing import Optional

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

DEFAULT_PROJECT_PATH = "config/datasets/default/project.yaml"


def _resolve_project_argument(
    value: Optional[str],
    workspace: Optional[WorkspaceContext],
) -> Optional[str]:
    if value is None or value != DEFAULT_PROJECT_PATH or workspace is None:
        return value
    config_root = workspace.resolve_config_root()
    if config_root is None:
        return value
    if config_root.suffix in {".yaml", ".yml"}:
        return str(config_root)
    candidate = config_root / "project.yaml"
    if candidate.exists():
        return str(candidate)
    fallback = config_root / "datasets" / "default" / "project.yaml"
    if fallback.exists():
        return str(fallback)
    return str(candidate)


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
        "--project",
        "-p",
        default="config/datasets/default/project.yaml",
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
        "--out-path",
        help="destination file path when using fs transport",
    )
    p_serve.add_argument(
        "--include-targets",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="include dataset.targets in served vectors (use --no-include-targets to force disable)",
    )
    p_serve.add_argument(
        "--keep",
        help="split label to serve; overrides run.yaml and project globals",
    )
    p_serve.add_argument(
        "--run",
        help="select a specific run config by filename stem when project.paths.run points to a folder",
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
        "--project",
        "-p",
        default="config/datasets/default/project.yaml",
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
            "  jerry source add <provider>.<dataset> -t url -f json\n"
            "  jerry source add -p <provider> -d <dataset> -t synthetic\n\n"
            "Examples:\n"
            "  fs CSV:        -t fs  -f csv\n"
            "  fs NDJSON:     -t fs  -f json-lines\n"
            "  URL JSON:      -t url -f json\n"
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
        choices=["fs", "url", "synthetic"],
        required=True,
        help="how data is accessed: fs/url/synthetic",
    )
    p_source_add.add_argument(
        "--format", "-f",
        choices=["csv", "json", "json-lines", "pickle"],
        help="data format for fs/url transports (ignored otherwise)",
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

    # inspect (metadata helpers)
    p_inspect = sub.add_parser(
        "inspect",
        help="inspect dataset metadata: report, coverage, matrix, partitions",
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
        default="config/datasets/default/project.yaml",
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
        "--include-targets",
        action="store_true",
        help="include dataset.targets when computing report/matrix/coverage",
    )

    # Coverage (JSON file)
    p_inspect_cov = inspect_sub.add_parser(
        "coverage",
        help="write coverage summary JSON",
        parents=[inspect_common],
    )
    p_inspect_cov.add_argument(
        "--project",
        "-p",
        default="config/datasets/default/project.yaml",
        help="path to project.yaml",
    )
    p_inspect_cov.add_argument(
        "--output",
        "-o",
        default=None,
        help="coverage JSON path (defaults to build/coverage.json)",
    )
    p_inspect_cov.add_argument(
        "--threshold",
        "-t",
        type=float,
        default=0.95,
        help="coverage threshold (0-1) for keep/drop lists",
    )
    p_inspect_cov.add_argument(
        "--match-partition",
        choices=["base", "full"],
        default="base",
        help="match features by base id or full partition id",
    )
    p_inspect_cov.add_argument(
        "--mode",
        choices=["final", "raw"],
        default="final",
        help="whether to apply postprocess transforms (final) or skip them (raw)",
    )
    p_inspect_cov.add_argument(
        "--include-targets",
        action="store_true",
        help="include dataset.targets when computing coverage",
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
        default="config/datasets/default/project.yaml",
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
    p_inspect_matrix.add_argument(
        "--include-targets",
        action="store_true",
        help="include dataset.targets when exporting the matrix",
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
        default="config/datasets/default/project.yaml",
        help="path to project.yaml",
    )
    p_inspect_parts.add_argument(
        "--output",
        "-o",
        default=None,
        help="partitions manifest path (defaults to build/partitions.json)",
    )
    p_inspect_parts.add_argument(
        "--include-targets",
        action="store_true",
        help="include dataset.targets when discovering partitions",
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
        default="config/datasets/default/project.yaml",
        help="path to project.yaml",
    )
    p_inspect_expected.add_argument(
        "--output",
        "-o",
        default=None,
        help="expected ids output path (defaults to build/datasets/<name>/expected.txt)",
    )
    p_inspect_expected.add_argument(
        "--include-targets",
        action="store_true",
        help="include dataset.targets when discovering expected ids",
    )

    workspace_context = load_workspace_context(Path.cwd())
    args = parser.parse_args()
    if hasattr(args, "project"):
        args.project = _resolve_project_argument(args.project, workspace_context)

    cli_level_arg = getattr(args, "log_level", None)
    shared_defaults = workspace_context.config.shared if workspace_context else None
    build_defaults = workspace_context.config.build if workspace_context else None
    default_level_name = (
        shared_defaults.log_level.upper()
        if shared_defaults and shared_defaults.log_level
        else "WARNING"
    )
    if cli_level_arg is None and getattr(args, "cmd", None) == "build":
        default_level_name = (
            build_defaults.log_level.upper()
            if build_defaults and build_defaults.log_level
            else "INFO"
        )
    base_level_name = (cli_level_arg or default_level_name).upper()
    base_level = logging._nameToLevel.get(base_level_name, logging.WARNING)

    logging.basicConfig(level=base_level, format="%(message)s")
    plugin_root = workspace_context.resolve_plugin_root() if workspace_context else None
    config_root = workspace_context.resolve_config_root() if workspace_context else None

    if args.cmd == "serve":
        handle_serve(
            project=args.project,
            limit=getattr(args, "limit", None),
            include_targets=args.include_targets,
            keep=getattr(args, "keep", None),
            run_name=getattr(args, "run", None),
            stage=getattr(args, "stage", None),
            out_transport=getattr(args, "out_transport", None),
            out_format=getattr(args, "out_format", None),
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
        default_project = _resolve_project_argument(
            DEFAULT_PROJECT_PATH, workspace_context
        )
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
                project=getattr(args, "project", default_project),
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
                include_targets=getattr(args, "include_targets", False),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
            )
        elif subcmd == "coverage":
            handle_inspect_report(
                project=getattr(args, "project", default_project),
                output=getattr(args, "output", None),
                threshold=getattr(args, "threshold", 0.95),
                match_partition=getattr(args, "match_partition", "base"),
                matrix="none",
                matrix_output=None,
                rows=20,
                cols=10,
                quiet=True,
                write_coverage=True,
                apply_postprocess=(getattr(args, "mode", "final") == "final"),
                include_targets=getattr(args, "include_targets", False),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
            )
        elif subcmd == "matrix":
            handle_inspect_report(
                project=getattr(args, "project", default_project),
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
                include_targets=getattr(args, "include_targets", False),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
            )
        elif subcmd == "partitions":
            from datapipeline.cli.commands.inspect import partitions as handle_inspect_partitions
            handle_inspect_partitions(
                project=getattr(args, "project", default_project),
                output=getattr(args, "output", None),
                include_targets=getattr(args, "include_targets", False),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
            )
        elif subcmd == "expected":
            from datapipeline.cli.commands.inspect import expected as handle_inspect_expected
            handle_inspect_expected(
                project=getattr(args, "project", default_project),
                output=getattr(args, "output", None),
                include_targets=getattr(args, "include_targets", False),
                visuals=inspect_visual_provider,
                progress=inspect_progress_style,
                log_level=base_level,
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
                config_root=config_root,
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
            config_root=config_root,
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
