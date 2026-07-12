import argparse

from .common import (
    add_artifact_mode_flag,
    add_dataset_flag,
    add_project_flag,
    add_visual_flags,
)


def add_materialize_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "materialize",
        help="materialize reusable stream outputs",
        description="Run configured materialize profiles.",
        parents=[common],
    )
    add_dataset_flag(parser)
    add_project_flag(parser)
    parser.add_argument(
        "--run",
        help="select one materialize profile by name",
    )
    parser.add_argument(
        "--output",
        help="override one profile's destination .jsonl file (requires --run)",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="overwrite existing materialized outputs",
    )
    add_artifact_mode_flag(parser)
    add_visual_flags(parser)
