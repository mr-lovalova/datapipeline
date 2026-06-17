import argparse

from .common import add_dataset_flag, add_project_flag, add_visual_flags


def add_materialize_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "materialize",
        help="materialize reusable stream outputs",
        parents=[common],
    )
    add_dataset_flag(parser)
    add_project_flag(parser)
    materialize_sub = parser.add_subparsers(dest="materialize_kind", required=True)
    stream = materialize_sub.add_parser("stream", help="materialize a record stream")
    stream.add_argument("stream_id", help="stream id to materialize")
    stream.add_argument("--output", required=True, help="destination .jsonl file")
    stream.add_argument("--as", dest="as_stream_id", help="generated ingest stream id")
    stream.add_argument(
        "--force",
        action="store_true",
        help="overwrite output and generated config files",
    )
    add_visual_flags(stream)
