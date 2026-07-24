import argparse


def add_plugin_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "plugin",
        help="scaffold plugin workspaces",
        parents=[common],
    )
    plugin_sub = parser.add_subparsers(required=True)
    init = plugin_sub.add_parser("init", help="create a plugin skeleton")
    init.add_argument(
        "plugin_name",
        help="plugin distribution name",
    )
    init.add_argument("--out", "-o", default=".")
