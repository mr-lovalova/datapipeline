import argparse

from datapipeline.config.options import (
    OUTPUT_FORMATS,
    OUTPUT_TRANSPORTS,
    OUTPUT_VIEWS,
    SOURCE_FS_FORMATS,
    SOURCE_TRANSPORTS,
    VISUAL_CHOICES,
)


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
        help="visuals mode: on (default) or off",
    )


def build_parser() -> argparse.ArgumentParser:
    # Common options shared by top-level and subcommands
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        help="set logging level (default: WARNING)",
    )
    common.add_argument(
        "--log-output",
        action="append",
        metavar="TARGET",
        default=None,
        help="repeatable log output target: stderr | stdout | fs:<path> | run[:<relative-path>]",
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
        help="output format (jsonl/csv/pickle) for serve runs",
    )
    p_serve.add_argument(
        "--output-directory",
        help="destination directory when using fs transport",
    )
    p_serve.add_argument(
        "--output-encoding",
        help="text encoding for fs jsonl/csv outputs (default: utf-8)",
    )
    p_serve.add_argument(
        "--output-view",
        choices=OUTPUT_VIEWS,
        help="output representation view (flat/raw/values); csv supports flat|values",
    )
    p_serve.add_argument(
        "--keep",
        help="split label to serve; overrides serve profiles and project globals",
    )
    p_serve.add_argument(
        "--run",
        help="select a serve profile by name when project.paths.tasks contains multiple entries",
    )
    p_serve.add_argument(
        "--stage",
        "-s",
        type=int,
        default=None,
        help="preview up to a 0-based feature-pipeline node index",
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
    p_build.add_argument(
        "--run",
        help="select a build profile by name when project.paths.tasks contains build profiles",
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

    # Shared visuals controls for inspect commands
    inspect_common = argparse.ArgumentParser(add_help=False)
    _add_visual_flags(inspect_common)
    _add_dataset_flag(inspect_common)

    # inspect (metadata helpers)
    p_inspect = sub.add_parser(
        "inspect",
        help="inspect dataset metadata: report, matrix, partitions",
        parents=[common, inspect_common],
    )
    _add_project_flag(p_inspect)
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
    return parser
