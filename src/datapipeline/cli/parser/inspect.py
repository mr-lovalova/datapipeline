import argparse

from datapipeline.config.options import (
    OUTPUT_INSPECT_FORMATS,
    OUTPUT_TRANSPORTS,
    OUTPUT_VIEWS,
)
from datapipeline.config.profiles import VALID_BUILD_MODES

from .common import add_dataset_flag, add_project_flag, add_visual_flags


def add_inspect_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "inspect",
        help="run inspect operations through inspect profiles",
        parents=[common],
    )
    add_dataset_flag(parser)
    add_project_flag(parser)
    parser.add_argument(
        "--run",
        help="select an inspect profile by name (project must declare inspect profiles under profiles/)",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=None,
        help="optional cap for inspect operations that stream records",
    )
    parser.add_argument(
        "--output-transport",
        choices=OUTPUT_TRANSPORTS,
        help="optional output transport override (stdout or fs) for inspect profiles",
    )
    parser.add_argument(
        "--output-format",
        choices=OUTPUT_INSPECT_FORMATS,
        help="optional output format override (jsonl/csv/pickle/txt/html) for inspect profiles",
    )
    parser.add_argument(
        "--output-directory",
        help="destination directory when using fs transport",
    )
    parser.add_argument(
        "--output-encoding",
        help="text encoding for fs jsonl/csv/txt outputs (default: utf-8)",
    )
    parser.add_argument(
        "--output-view",
        choices=OUTPUT_VIEWS,
        help="output representation view (flat/raw/values); csv supports flat|values",
    )
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
    add_visual_flags(parser)
