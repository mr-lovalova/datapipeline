import argparse

from .common import add_dataset_flag, add_project_flag, add_visual_flags


def add_inspect_command(sub,  common: argparse.ArgumentParser) -> None:
    inspect_common = argparse.ArgumentParser(add_help=False)
    add_visual_flags(inspect_common)
    add_dataset_flag(inspect_common)

    parser = sub.add_parser(
        "inspect",
        help="inspect dataset metadata: report, matrix, partitions",
        parents=[common, inspect_common],
    )
    add_project_flag(parser)
    inspect_sub = parser.add_subparsers(dest="inspect_cmd", required=False)

    report = inspect_sub.add_parser(
        "report",
        help="print a quality report to stdout",
        parents=[inspect_common],
    )
    add_project_flag(report)
    report.add_argument(
        "--threshold",
        "-t",
        type=float,
        default=0.95,
        help="coverage threshold (0-1) for keep/drop lists",
    )
    report.add_argument(
        "--match-partition",
        choices=["base", "full"],
        default="base",
        help="match features by base id or full partition id",
    )
    report.add_argument(
        "--mode",
        choices=["final", "raw"],
        default="final",
        help="whether to apply postprocess transforms (final) or skip them (raw)",
    )
    report.add_argument(
        "--sort",
        choices=["missing", "nulls"],
        default="missing",
        help="feature ranking metric in the report (missing or nulls)",
    )

    matrix = inspect_sub.add_parser(
        "matrix",
        help="export availability matrix",
        parents=[inspect_common],
    )
    add_project_flag(matrix)
    matrix.add_argument(
        "--threshold",
        "-t",
        type=float,
        default=0.95,
        help="coverage threshold (used in the report)",
    )
    matrix.add_argument(
        "--rows",
        type=int,
        default=20,
        help="max number of group buckets in the matrix (0 = all)",
    )
    matrix.add_argument(
        "--cols",
        type=int,
        default=10,
        help="max number of features/partitions in the matrix (0 = all)",
    )
    matrix.add_argument(
        "--format",
        choices=["csv", "html"],
        default="html",
        help="output format for the matrix",
    )
    matrix.add_argument(
        "--output",
        default=None,
        help="destination for the matrix (defaults to build/matrix.<fmt>)",
    )
    matrix.add_argument(
        "--quiet",
        action="store_true",
        help="suppress detailed console report; only print save messages",
    )
    matrix.add_argument(
        "--mode",
        choices=["final", "raw"],
        default="final",
        help="whether to apply postprocess transforms (final) or skip them (raw)",
    )

    parts = inspect_sub.add_parser(
        "partitions",
        help="discover partitions and write a manifest JSON",
        parents=[inspect_common],
    )
    add_project_flag(parts)
    parts.add_argument(
        "--output",
        "-o",
        default=None,
        help="partitions manifest path (defaults to build/partitions.json)",
    )
