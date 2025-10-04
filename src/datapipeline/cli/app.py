import argparse

from datapipeline.cli.commands.run import handle_prep, handle_serve
from datapipeline.cli.commands.plugin import bar as handle_bar
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.link import handle as handle_link
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.filter import handle as handle_filter
from datapipeline.cli.commands.inspect import (
    report as handle_inspect_report,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="jerry",
        description="Mixology-themed CLI for building and serving data pipelines.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # prep (debug mode with visuals)
    p_prep = sub.add_parser(
        "prep",
        help="run pipeline stages with visual progress",
    )
    prep_sub = p_prep.add_subparsers(dest="prep_cmd", required=True)
    prep_steps = {
        "pour": "preview record-stage output",
        "build": "inspect feature-stage output",
        "stir": "examine vector-stage output",
    }
    for step, help_text in prep_steps.items():
        sp = prep_sub.add_parser(step, help=help_text)
        sp.add_argument(
            "--project",
            "-p",
            default="config/recipes/default/project.yaml",
            help="path to project.yaml",
        )
        sp.add_argument("--limit", "-n", type=int, default=20)


    # serve (production run, no visuals)
    p_serve = sub.add_parser(
        "serve",
        help="produce vectors without progress visuals",
    )
    p_serve.add_argument(
        "--project",
        "-p",
        default="config/recipes/default/project.yaml",
        help="path to project.yaml",
    )
    p_serve.add_argument(
        "--limit", "-n", type=int, default=None,
        help="optional cap on the number of vectors to emit",
    )
    p_serve.add_argument(
        "--output", "-o", default="print",
        help="output destination: 'print', 'stream', or a file ending in .pt",
    )

    # distillery (sources)
    p_dist = sub.add_parser(
        "distillery",
        help="add or list raw sources",
    )
    dist_sub = p_dist.add_subparsers(dest="dist_cmd", required=True)
    p_dist_add = dist_sub.add_parser(
        "add",
        help="create a provider+dataset source",
        description=(
            "Scaffold a source using transport + format.\n\n"
            "Examples:\n"
            "  fs CSV:        -t fs  -f csv\n"
            "  fs NDJSON:     -t fs  -f json-lines\n"
            "  URL JSON:      -t url -f json\n"
            "  Synthetic:     -t synthetic\n\n"
            "Note: set 'glob: true' in the generated YAML if your 'path' contains wildcards."
        ),
    )
    p_dist_add.add_argument("--provider", "-p", required=True)
    p_dist_add.add_argument("--dataset", "-d", required=True)
    p_dist_add.add_argument(
        "--transport", "-t",
        choices=["fs", "url", "synthetic"],
        required=True,
        help="how data is accessed: fs/url/synthetic",
    )
    p_dist_add.add_argument(
        "--format", "-f",
        choices=["csv", "json", "json-lines"],
        help="data format for fs/url transports (ignored otherwise)",
    )
    dist_sub.add_parser("list", help="list known sources")

    # spirit (domains)
    p_spirit = sub.add_parser(
        "spirit",
        help="add or list domains",
    )
    spirit_sub = p_spirit.add_subparsers(dest="spirit_cmd", required=True)
    p_spirit_add = spirit_sub.add_parser(
        "add",
        help="create a domain",
        description="Create a time-aware domain package rooted in TimeSeriesRecord.",
    )
    p_spirit_add.add_argument("--domain", "-d", required=True)
    spirit_sub.add_parser("list", help="list known domains")

    # contract (link source ↔ domain)
    p_contract = sub.add_parser(
        "contract",
        help="link a distillery source to a spirit domain",
    )

    # bar (plugin scaffolding)
    p_bar = sub.add_parser(
        "bar",
        help="scaffold plugin workspaces",
    )
    bar_sub = p_bar.add_subparsers(dest="bar_cmd", required=True)
    p_bar_init = bar_sub.add_parser(
        "init", help="create a plugin skeleton")
    p_bar_init.add_argument("--name", "-n", required=True)
    p_bar_init.add_argument("--out", "-o", default=".")

    # filter (unchanged helper)
    p_filt = sub.add_parser("filter", help="manage filters")
    filt_sub = p_filt.add_subparsers(dest="filter_cmd", required=True)
    p_filt_create = filt_sub.add_parser(
        "create", help="create a filter function")
    p_filt_create.add_argument(
        "--name", "-n", required=True,
        help="filter entrypoint name and function/module name",
    )

    # inspect (metadata helpers)
    p_inspect = sub.add_parser(
        "inspect",
        help="inspect dataset metadata: report, coverage, matrix, partitions",
    )
    inspect_sub = p_inspect.add_subparsers(dest="inspect_cmd", required=False)

    # Report (stdout only)
    p_inspect_report = inspect_sub.add_parser(
        "report",
        help="print a quality report to stdout",
    )
    p_inspect_report.add_argument(
        "--project",
        "-p",
        default="config/recipes/default/project.yaml",
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
        help="whether to apply vector transforms (final) or ignore them (raw)",
    )

    # Coverage (JSON file)
    p_inspect_cov = inspect_sub.add_parser(
        "coverage",
        help="write coverage summary JSON",
    )
    p_inspect_cov.add_argument(
        "--project",
        "-p",
        default="config/recipes/default/project.yaml",
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
        help="whether to apply vector transforms (final) or ignore them (raw)",
    )

    # Matrix export
    p_inspect_matrix = inspect_sub.add_parser(
        "matrix",
        help="export availability matrix",
    )
    p_inspect_matrix.add_argument(
        "--project",
        "-p",
        default="config/recipes/default/project.yaml",
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
        default="csv",
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
        help="whether to apply vector transforms (final) or ignore them (raw)",
    )

    # Partitions manifest subcommand
    p_inspect_parts = inspect_sub.add_parser(
        "partitions",
        help="discover partitions and write a manifest JSON",
    )
    p_inspect_parts.add_argument(
        "--project",
        "-p",
        default="config/recipes/default/project.yaml",
        help="path to project.yaml",
    )
    p_inspect_parts.add_argument(
        "--output",
        "-o",
        default=None,
        help="partitions manifest path (defaults to build/partitions.json)",
    )

    args = parser.parse_args()

    if args.cmd == "prep":
        handle_prep(action=args.prep_cmd,
                    project=args.project, limit=args.limit)
        return

    if args.cmd == "serve":
        handle_serve(
            project=args.project,
            limit=getattr(args, "limit", None),
            output=args.output,
        )
        return

    if args.cmd == "inspect":
        # Default to 'report' when no subcommand is given
        subcmd = getattr(args, "inspect_cmd", None)
        if subcmd in (None, "report"):
            handle_inspect_report(
                project=getattr(args, "project", "config/recipes/default/project.yaml"),
                output=None,
                threshold=getattr(args, "threshold", 0.95),
                match_partition=getattr(args, "match_partition", "base"),
                matrix="none",
                matrix_output=None,
                rows=20,
                cols=10,
                quiet=False,
                write_coverage=False,
                apply_vector_transforms=(getattr(args, "mode", "final") == "final"),
            )
        elif subcmd == "coverage":
            handle_inspect_report(
                project=args.project,
                output=getattr(args, "output", None),
                threshold=getattr(args, "threshold", 0.95),
                match_partition=getattr(args, "match_partition", "base"),
                matrix="none",
                matrix_output=None,
                rows=20,
                cols=10,
                quiet=True,
                write_coverage=True,
                apply_vector_transforms=(getattr(args, "mode", "final") == "final"),
            )
        elif subcmd == "matrix":
            handle_inspect_report(
                project=args.project,
                output=None,
                threshold=getattr(args, "threshold", 0.95),
                match_partition="base",
                matrix=getattr(args, "format", "csv"),
                matrix_output=getattr(args, "output", None),
                rows=getattr(args, "rows", 20),
                cols=getattr(args, "cols", 10),
                quiet=getattr(args, "quiet", False),
                write_coverage=False,
                apply_vector_transforms=(getattr(args, "mode", "final") == "final"),
            )
        elif subcmd == "partitions":
            from datapipeline.cli.commands.inspect import partitions as handle_inspect_partitions
            handle_inspect_partitions(
                project=args.project,
                output=getattr(args, "output", None),
            )
        return

    if args.cmd == "distillery":
        if args.dist_cmd == "list":
            handle_list(subcmd="sources")
        else:
            handle_source(
                subcmd="add",
                provider=getattr(args, "provider", None),
                dataset=getattr(args, "dataset", None),
                transport=getattr(args, "transport", None),
                format=getattr(args, "format", None),
            )
        return

    if args.cmd == "spirit":
        if args.spirit_cmd == "list":
            handle_list(subcmd="domains")
        else:
            handle_domain(
                subcmd="add",
                domain=getattr(args, "domain", None),
            )
        return

    if args.cmd == "contract":
        handle_link()
        return

    if args.cmd == "bar":
        handle_bar(
            subcmd=args.bar_cmd,
            name=getattr(args, "name", None),
            out=getattr(args, "out", "."),
        )
        return

    if args.cmd == "filter":
        handle_filter(subcmd=args.filter_cmd, name=getattr(args, "name", None))
        return
