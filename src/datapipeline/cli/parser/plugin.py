import argparse


def add_plugin_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "plugin",
        help="scaffold plugin workspaces",
        parents=[common],
    )
    plugin_sub = parser.add_subparsers(dest="plugin_cmd", required=True)
    init = plugin_sub.add_parser("init", help="create a plugin skeleton")
    name = init.add_mutually_exclusive_group()
    name.add_argument(
        "plugin_name",
        nargs="?",
        help="plugin distribution name",
    )
    name.add_argument(
        "--name",
        "-n",
        dest="plugin_name_option",
        help="plugin distribution name",
    )
    init.add_argument("--out", "-o", default=".")
