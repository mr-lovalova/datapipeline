import argparse


def add_filter_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser("filter", help="manage filters", parents=[common])
    filter_sub = parser.add_subparsers(dest="filter_cmd", required=True)
    create = filter_sub.add_parser("create", help="create a filter function")
    create.add_argument(
        "--name",
        "-n",
        required=True,
        help="filter entrypoint name and function/module name",
    )
