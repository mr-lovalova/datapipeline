import argparse
import logging
from pathlib import Path
from typing import Optional, Tuple

from datapipeline.cli.commands.run import handle_serve
from datapipeline.cli.commands.plugin import bar as handle_bar
from datapipeline.cli.commands.demo import handle as handle_demo
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.contract import handle as handle_contract
from datapipeline.cli.commands.dto import handle as handle_dto
from datapipeline.cli.commands.parser import handle as handle_parser
from datapipeline.cli.commands.mapper import handle as handle_mapper
from datapipeline.cli.commands.loader import handle as handle_loader
from datapipeline.cli.commands.stream import handle as handle_stream
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.filter import handle as handle_filter
from datapipeline.cli.commands.inspect import (
    report as handle_inspect_report,
)
from datapipeline.cli.commands.build import handle as handle_build
from datapipeline.config.workspace import (
    WorkspaceContext,
    load_workspace_context,
    resolve_with_workspace,
)
from datapipeline.config.options import (
    OUTPUT_FORMATS,
    OUTPUT_TRANSPORTS,
    PROGRESS_CHOICES,
    SOURCE_FS_FORMATS,
    SOURCE_TRANSPORTS,
    VISUAL_CHOICES,
)
from datapipeline.config.resolution import resolve_visuals
from datapipeline.utils.rich_compat import suppress_file_proxy_shutdown_errors

suppress_file_proxy_shutdown_errors()


def _add_dataset_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dataset",
        "-d",
        help="dataset alias, folder, or project.yaml path",
    )


def _add_project_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )


def _add_visual_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--visuals",
        choices=VISUAL_CHOICES,
        default=None,
        help="visuals renderer: auto (default), tqdm, rich, or off",
    )
    parser.add_argument(
        "--progress",
        choices=PROGRESS_CHOICES,
        default=None,
        help="progress display: auto (spinner unless DEBUG), spinner, bars, or off",
    )


def _dataset_to_project_path(
    dataset: str,
    workspace: Optional[WorkspaceContext],
) -> str:
    """Resolve a dataset selector (alias, folder, or file) into a project.yaml path."""
    # 1) Alias via jerry.yaml datasets (wins over local folders with same name)
    if workspace is not None:
        resolved = workspace.resolve_dataset_alias(dataset)
        if resolved is not None:
            return str(resolved)

    # 2) Direct file path
    path = Path(dataset)
    if path.suffix in {".yaml", ".yml"}:
        return str(resolve_with_workspace(path, workspace))

    # 3) Directory: assume project.yaml inside
    candidate_dir = resolve_with_workspace(path, workspace)
    if candidate_dir.is_dir():
        candidate = candidate_dir / "project.yaml"
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


def _run_inspect_command(
    *,
    args: argparse.Namespace,
    shared_defaults,
    base_level: int,
    workspace_context: WorkspaceContext | None,
) -> None:
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
    base_kwargs = {
        "project": args.project,
        "threshold": getattr(args, "threshold", 0.95),
        "apply_postprocess": (getattr(args, "mode", "final") == "final"),
        "visuals": inspect_visual_provider,
        "progress": inspect_progress_style,
        "log_level": base_level,
        "sort": getattr(args, "sort", "missing"),
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
            project=args.project,
            output=getattr(args, "output", None),
            visuals=inspect_visual_provider,
            progress=inspect_progress_style,
            log_level=base_level,
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
    _add_dataset_flag(p_serve)
    _add_project_flag(p_serve)
    p_serve.add_argument(
        "--limit", "-n", type=int, default=None,
        help="optional cap on the number of vectors to emit",
    )
    p_serve.add_argument(
        "--output-transport",
        choices=OUTPUT_TRANSPORTS,
        help="output transport (stdout or fs) for serve runs",
    )
    p_serve.add_argument(
        "--output-format",
        choices=OUTPUT_FORMATS,
        help="output format (print/jsonl/json/csv/pickle) for serve runs",
    )
    p_serve.add_argument(
        "--output-payload",
        choices=["sample", "vector"],
        help="payload structure: full sample (default) or vector-only body",
    )
    p_serve.add_argument(
        "--output-directory",
        help="destination directory when using fs transport",
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
        choices=range(0, 9),
        default=None,
        help="preview a specific pipeline stage (0-6 record/feature stages, 7 assembled vectors, 8 transformed vectors)",
    )
    _add_visual_flags(p_serve)
    p_serve.add_argument(
        "--skip-build",
        action="store_true",
        help="skip the automatic build step (useful for quick feature previews)",
    )

    # build (materialize artifacts)
    p_build = sub.add_parser(
        "build",
        help="materialize project artifacts (schema, hashes, etc.)",
        parents=[common],
    )
    _add_dataset_flag(p_build)
    _add_project_flag(p_build)
    p_build.add_argument(
        "--force",
        action="store_true",
        help="rebuild even when the configuration hash matches the last run",
    )
    _add_visual_flags(p_build)

    # demo (optional demo dataset)
    p_demo = sub.add_parser(
        "demo",
        help="create an optional demo dataset inside a plugin",
        parents=[common],
    )
    demo_sub = p_demo.add_subparsers(dest="demo_cmd", required=True)
    demo_init = demo_sub.add_parser(
        "init",
        help="create a standalone demo plugin named demo",
        parents=[common],
    )
    demo_init.add_argument(
        "--out",
        "-o",
        help="override parent directory (demo will be created inside)",
    )

    # list
    p_list = sub.add_parser(
        "list",
        help="list known resources",
        parents=[common],
    )
    list_sub = p_list.add_subparsers(dest="list_cmd", required=True)
    list_sub.add_parser("sources", help="list sources")
    list_sub.add_parser("domains", help="list domains")
    list_sub.add_parser("parsers", help="list parsers")
    list_sub.add_parser("dtos", help="list DTOs")
    list_sub.add_parser("mappers", help="list mappers")
    list_sub.add_parser("loaders", help="list loaders")

    # source
    p_source = sub.add_parser(
        "source",
        help="create or list raw sources",
        parents=[common],
    )
    source_sub = p_source.add_subparsers(dest="source_cmd", required=True)
    p_source_create = source_sub.add_parser(
        "create",
        help="create a provider+dataset source (yaml only)",
        description=(
            "Create a source YAML using transport + format or a loader entrypoint.\n\n"
            "Usage:\n"
            "  jerry source create <provider>.<dataset> -t fs -f csv\n"
            "  jerry source create <provider>.<dataset> -t http -f json\n"
            "  jerry source create <provider>.<dataset> -t synthetic\n\n"
            "  jerry source create <provider> <dataset> --loader mypkg.loaders.demo:Loader\n"
            "  jerry source create <provider> <dataset> --parser myparser\n\n"
            "Examples:\n"
            "  fs CSV:        -t fs  -f csv\n"
            "  fs NDJSON:     -t fs  -f jsonl\n"
            "  HTTP JSON:     -t http -f json\n"
            "  Synthetic:     -t synthetic\n\n"
            "Note: set 'glob: true' in the generated YAML if your 'path' contains wildcards."
        ),
    )
    # Support simple positionals, plus flags for compatibility.
    p_source_create.add_argument("provider", nargs="?", help="provider name")
    p_source_create.add_argument("dataset", nargs="?", help="dataset slug")
    p_source_create.add_argument("--provider", "-p", dest="provider_opt", metavar="PROVIDER", help="provider name")
    p_source_create.add_argument("--dataset", "-d", dest="dataset_opt", metavar="DATASET", help="dataset slug")
    p_source_create.add_argument("--alias", "-a", help="provider.dataset alias")
    p_source_create.add_argument(
        "--transport", "-t",
        choices=SOURCE_TRANSPORTS,
        required=False,
        help="how data is accessed: fs/http/synthetic",
    )
    p_source_create.add_argument(
        "--format", "-f",
        choices=SOURCE_FS_FORMATS,
        help="data format for fs/http transports (ignored otherwise)",
    )
    p_source_create.add_argument(
        "--loader",
        help="loader entrypoint (overrides --transport/--format)",
    )
    p_source_create.add_argument(
        "--parser",
        help="parser entrypoint (defaults to identity)",
    )
    p_source_create.add_argument(
        "--identity",
        action="store_true",
        help="use the built-in identity parser (alias for --parser identity)",
    )
    source_sub.add_parser("list", help="list known sources")

    # domain
    p_domain = sub.add_parser(
        "domain",
        help="create or list domains",
        parents=[common],
    )
    domain_sub = p_domain.add_subparsers(dest="domain_cmd", required=True)
    p_domain_add = domain_sub.add_parser(
        "create",
        help="create a domain",
        description="Create a time-aware domain package rooted in TemporalRecord.",
    )
    # Accept positional name, plus flags for flexibility and consistency.
    p_domain_add.add_argument("domain", nargs="?", help="domain name")
    p_domain_add.add_argument(
        "--name", "-n", dest="domain", help="domain name"
    )
    domain_sub.add_parser("list", help="list known domains")

    # dto
    p_dto = sub.add_parser(
        "dto",
        help="create DTOs",
        parents=[common],
    )
    dto_sub = p_dto.add_subparsers(dest="dto_cmd", required=True)
    p_dto_create = dto_sub.add_parser("create", help="create a DTO")
    p_dto_create.add_argument("name", nargs="?", help="DTO class name")

    # parser
    p_parser = sub.add_parser(
        "parser",
        help="create parsers",
        parents=[common],
    )
    parser_sub = p_parser.add_subparsers(dest="parser_cmd", required=True)
    p_parser_create = parser_sub.add_parser("create", help="create a parser")
    p_parser_create.add_argument("name", nargs="?", help="Parser class name")

    # mapper
    p_mapper = sub.add_parser(
        "mapper",
        help="create mappers",
        parents=[common],
    )
    mapper_sub = p_mapper.add_subparsers(dest="mapper_cmd", required=True)
    p_mapper_create = mapper_sub.add_parser("create", help="create a mapper")
    p_mapper_create.add_argument("name", nargs="?", help="Mapper function name")

    # loader
    p_loader = sub.add_parser(
        "loader",
        help="create loaders",
        parents=[common],
    )
    loader_sub = p_loader.add_subparsers(dest="loader_cmd", required=True)
    p_loader_create = loader_sub.add_parser("create", help="create a loader")
    p_loader_create.add_argument("name", nargs="?", help="Loader name")

    # inflow
    p_inflow = sub.add_parser(
        "inflow",
        help="create end-to-end inflow scaffolds",
        parents=[common],
    )
    inflow_sub = p_inflow.add_subparsers(dest="inflow_cmd", required=True)
    inflow_sub.add_parser("create", help="create an inflow")

    # contract (interactive: ingest or composed)
    p_contract = sub.add_parser(
        "contract",
        help="manage stream contracts (ingest or composed)",
        parents=[common],
    )
    contract_sub = p_contract.add_subparsers(dest="contract_cmd", required=True)
    p_contract_create = contract_sub.add_parser("create", help="create a contract")
    p_contract_create.add_argument(
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
    _add_visual_flags(inspect_common)
    _add_dataset_flag(inspect_common)

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
    _add_project_flag(p_inspect_report)
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
    _add_project_flag(p_inspect_matrix)
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
    _add_project_flag(p_inspect_parts)
    p_inspect_parts.add_argument(
        "--output",
        "-o",
        default=None,
        help="partitions manifest path (defaults to build/partitions.json)",
    )


    workspace_context = load_workspace_context(Path.cwd())
    args = parser.parse_args()

    # Resolve dataset/project selection for commands that use a project.
    needs_project_resolution = args.cmd in {"serve", "build", "inspect"}
    if needs_project_resolution and (
        hasattr(args, "project") or hasattr(args, "dataset")
    ):
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
            output_transport=getattr(args, "output_transport", None),
            output_format=getattr(args, "output_format", None),
            output_payload=getattr(args, "output_payload", None),
            output_directory=getattr(args, "output_directory", None),
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
        _run_inspect_command(
            args=args,
            shared_defaults=shared_defaults,
            base_level=base_level,
            workspace_context=workspace_context,
        )
        return

    if _dispatch_non_project_command(
        args=args,
        plugin_root=plugin_root,
        workspace_context=workspace_context,
    ):
        return
