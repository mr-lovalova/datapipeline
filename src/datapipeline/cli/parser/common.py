import argparse

from datapipeline.config.options import VISUAL_CHOICES
from datapipeline.config.profiles import ARTIFACT_MODES


def _heartbeat_interval_seconds(value: str) -> float:
    try:
        interval = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--heartbeat-interval must be a non-negative number of seconds"
        ) from exc
    if interval < 0:
        raise argparse.ArgumentTypeError(
            "--heartbeat-interval must be a non-negative number of seconds"
        )
    return interval


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


def add_artifact_mode_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--artifact-mode",
        choices=ARTIFACT_MODES,
        type=str.upper,
        default=None,
        help="prerequisite artifact policy: AUTO | FORCE | OFF",
    )


def add_visual_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--visuals",
        choices=VISUAL_CHOICES,
        default=None,
        help="visuals mode: on (default) or off",
    )


def build_common_parent() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        default=None,
        help="set logging level (default: INFO)",
    )
    common.add_argument(
        "--log-output",
        action="append",
        metavar="TARGET",
        default=None,
        help="repeatable log output target: stderr | stdout | fs:<path> | execution[:<relative-path>]",
    )
    common.add_argument(
        "--heartbeat-interval",
        dest="heartbeat_interval_seconds",
        type=_heartbeat_interval_seconds,
        default=None,
        metavar="SECONDS",
        help="node heartbeat interval in seconds; set to 0 to disable",
    )
    return common


def build_command_common_parent() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        default=argparse.SUPPRESS,
        help="set logging level (default: INFO)",
    )
    common.add_argument(
        "--log-output",
        action="append",
        metavar="TARGET",
        default=argparse.SUPPRESS,
        help="repeatable log output target: stderr | stdout | fs:<path> | execution[:<relative-path>]",
    )
    common.add_argument(
        "--heartbeat-interval",
        dest="heartbeat_interval_seconds",
        type=_heartbeat_interval_seconds,
        default=argparse.SUPPRESS,
        metavar="SECONDS",
        help="node heartbeat interval in seconds; set to 0 to disable",
    )
    return common
