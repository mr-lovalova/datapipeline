import argparse

from datapipeline.config.options import OUTPUT_FORMATS, OUTPUT_TRANSPORTS, OUTPUT_VIEWS
from datapipeline.config.profiles import VALID_BUILD_MODES

from .common import add_cache_flags, add_dataset_flag, add_project_flag, add_visual_flags


def add_serve_command(sub,  common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "serve",
        help="produce vectors with configurable logging",
        parents=[common],
    )
    add_dataset_flag(parser)
    add_project_flag(parser)
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="optional cap on the number of vectors to emit",
    )
    parser.add_argument(
        "--output-transport",
        choices=OUTPUT_TRANSPORTS,
        help="output transport (stdout or fs) for serve runs",
    )
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_FORMATS,
        help="output format (jsonl/csv/pickle) for serve runs",
    )
    parser.add_argument(
        "--output-directory",
        help="destination directory when using fs transport",
    )
    parser.add_argument(
        "--output-encoding",
        help="text encoding for fs jsonl/csv outputs (default: utf-8)",
    )
    parser.add_argument(
        "--output-view",
        choices=OUTPUT_VIEWS,
        help="output representation view (jsonl: raw|flat, csv: flat, pickle: raw)",
    )
    parser.add_argument(
        "--keep",
        help="split label to serve; overrides serve profiles and project globals",
    )
    parser.add_argument(
        "--run",
        help="select a serve profile by name when project.paths.tasks contains multiple entries",
    )
    parser.add_argument(
        "--preview-index",
        type=int,
        default=None,
        help="preview a 0-based serve preview index",
    )
    add_visual_flags(parser)
    add_cache_flags(parser)
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="skip artifact execution when the selected profile target is an artifact task",
    )
    parser.add_argument(
        "--build-mode",
        choices=VALID_BUILD_MODES,
        type=str.upper,
        default=None,
        help="artifact build policy when the selected profile target is an artifact task: AUTO | FORCE | OFF",
    )
