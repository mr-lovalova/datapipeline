import argparse

from .common import add_dataset_flag, add_project_flag, add_visual_flags


def add_build_command(sub,  common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "build",
        help="materialize project artifacts (schema, hashes, etc.)",
        parents=[common],
    )
    add_dataset_flag(parser)
    add_project_flag(parser)
    parser.add_argument(
        "--force",
        action="store_true",
        help="rebuild even when the configuration hash matches the last run",
    )
    parser.add_argument(
        "--run",
        help="select a build profile by name (project must declare build profiles under profiles/)",
    )
    add_visual_flags(parser)
