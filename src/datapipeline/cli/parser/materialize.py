import argparse

from datapipeline.config.options import VISUAL_CHOICES

from .common import add_dataset_flag, add_project_flag, add_visual_flags


def add_materialize_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "materialize",
        help="materialize reusable stream outputs",
        description=(
            "Run enabled materialize profiles, or use 'stream' for one ad-hoc output."
        ),
        parents=[common],
    )
    add_dataset_flag(parser)
    add_project_flag(parser)
    parser.add_argument(
        "--run",
        help="select one materialize profile by name",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="overwrite existing materialized outputs",
    )
    add_visual_flags(parser)

    materialize_sub = parser.add_subparsers(dest="materialize_kind")
    stream = materialize_sub.add_parser("stream", help="materialize a record stream")
    stream.add_argument("stream_id", help="stream id to materialize")
    stream.add_argument("--output", required=True, help="destination .jsonl file")
    stream.add_argument("--as", dest="as_stream_id", help="generated ingest stream id")
    # Suppress subparser defaults so options parsed before `stream` survive.
    stream.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=argparse.SUPPRESS,
        help="overwrite existing materialized outputs",
    )
    stream.add_argument(
        "--visuals",
        choices=VISUAL_CHOICES,
        default=argparse.SUPPRESS,
        help="visuals mode: on (default) or off",
    )
