import argparse

from datapipeline.config.options import VISUAL_CHOICES


def add_dataset_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dataset",
        "-d",
        help="dataset alias, folder, or project.yaml path",
    )


def add_project_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--project",
        "-p",
        default=None,
        help="path to project.yaml",
    )


def add_visual_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--visuals",
        choices=VISUAL_CHOICES,
        default=None,
        help="visuals mode: on (default) or off",
    )


def add_cache_flags(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--cache",
        dest="cache",
        action="store_true",
        default=None,
        help="enable runtime record-stream caching",
    )
    group.add_argument(
        "--no-cache",
        dest="cache",
        action="store_false",
        help="disable runtime record-stream caching",
    )


def build_common_parent() -> argparse.ArgumentParser:
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
        help="repeatable log output target: stderr | stdout | fs:<path> | execution[:<relative-path>]",
    )
    return common
