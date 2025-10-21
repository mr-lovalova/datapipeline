import argparse
import logging

from datapipeline.cli.commands.run import handle_serve
from datapipeline.cli.commands.plugin import bar as handle_bar
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.link import handle as handle_link
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.filter import handle as handle_filter
from datapipeline.cli.commands.inspect import (
    report as handle_inspect_report,
)
from datapipeline.cli.commands.build import handle as handle_build


def main() -> None:
    # Common options shared by top-level and subcommands
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="set logging level (default: WARNING)",
    )

    parser = argparse.ArgumentParser(
        prog="jerry",
        description="Mixology-themed CLI for building and serving data pipelines.",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # prep (debug mode with visuals)
    p_prep = sub.add_parser(
        "prep",
        help="run pipeline stages with visual progress",
        parents=[common],
    )
    # Only support numeric stage previews; legacy named stages removed
    p_prep.add_argument(
        "--project",
        "-p",
        default="config/datasets/default/project.yaml",
        help="path to project.yaml",
    )
    p_prep.add_argument("--limit", "-n", type=int, default=20)
    prep_sub = p_prep.add_subparsers(dest="prep_cmd", required=True)
    p_prep_stage = prep_sub.add_parser(
        "stage",
        help="preview a numeric feature stage (0-5)",
        parents=[common],
    )
    p_prep_stage.add_argument("num", type=int, help="feature stage number (0-5)")
    p_prep_stage.add_argument(
        "--project",
        "-p",
        default="config/datasets/default/project.yaml",
        help="path to project.yaml",
    )
    p_prep_stage.add_argument("--limit", "-n", type=int, default=20)


    # serve (production run, no visuals)
    p_serve = sub.add_parser(
        "serve",
        help="produce vectors without progress visuals",
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
        "--output", "-o", default="print",
        help="output destination: 'print', 'stream', or a file ending in .pt",
    )
    p_serve.add_argument(
        "--include-targets",
        action="store_true",
        help="include dataset.targets in served vectors (defaults to features only)",
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

    # source
    p_dist = sub.add_parser(
        "source",
        help="add or list raw sources",
        parents=[common],
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

    # domain
    p_spirit = sub.add_parser(
        "domain",
        help="add or list domains",
        parents=[common],
    )
    spirit_sub = p_spirit.add_subparsers(dest="spirit_cmd", required=True)
    p_spirit_add = spirit_sub.add_parser(
        "add",
        help="create a domain",
        description="Create a time-aware domain package rooted in TemporalRecord.",
    )
    p_spirit_add.add_argument("--domain", "-d", required=True)
    spirit_sub.add_parser("list", help="list known domains")

    # contract (link source ↔ domain)
    p_contract = sub.add_parser(
        "contract",
        help="link a source to a domain",
        parents=[common],
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
    p_bar_init.add_argument("--name", "-n", required=True)
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

    # inspect (metadata helpers)
    p_inspect = sub.add_parser(
        "inspect",
        help="inspect dataset metadata: report, coverage, matrix, partitions",
        parents=[common],
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

    args = parser.parse_args()

    # Initialize logging before dispatching to subcommands
    logging.basicConfig(
        level=getattr(logging, str(getattr(args, "log_level", "WARNING")).upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.cmd == "prep":
        from datapipeline.cli.commands.run import handle_prep_stage
        handle_prep_stage(
            project=getattr(args, "project", "config/datasets/default/project.yaml"),
            stage=getattr(args, "num", 0),
            limit=getattr(args, "limit", 20),
        )
        return

    if args.cmd == "serve":
        handle_serve(
            project=args.project,
            limit=getattr(args, "limit", None),
            output=args.output,
            include_targets=getattr(args, "include_targets", False),
        )
        return
    if args.cmd == "build":
        handle_build(
            project=args.project,
            force=getattr(args, "force", False),
        )
        return

    if args.cmd == "inspect":
        # Default to 'report' when no subcommand is given
        subcmd = getattr(args, "inspect_cmd", None)
        if subcmd in (None, "report"):
            handle_inspect_report(
                project=getattr(args, "project", "config/datasets/default/project.yaml"),
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
                apply_postprocess=(getattr(args, "mode", "final") == "final"),
                include_targets=getattr(args, "include_targets", False),
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
                apply_postprocess=(getattr(args, "mode", "final") == "final"),
                include_targets=getattr(args, "include_targets", False),
            )
        elif subcmd == "partitions":
            from datapipeline.cli.commands.inspect import partitions as handle_inspect_partitions
            handle_inspect_partitions(
                project=args.project,
                output=getattr(args, "output", None),
                include_targets=getattr(args, "include_targets", False),
            )
        elif subcmd == "expected":
            from datapipeline.cli.commands.inspect import expected as handle_inspect_expected
            handle_inspect_expected(
                project=args.project,
                output=getattr(args, "output", None),
                include_targets=getattr(args, "include_targets", False),
            )
        return

    if args.cmd == "source":
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

    if args.cmd == "domain":
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
