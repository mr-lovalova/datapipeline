import argparse


def add_domain_command(sub, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "domain",
        help="create or list domains",
        parents=[common],
    )
    domain_sub = parser.add_subparsers(dest="domain_cmd", required=True)
    create = domain_sub.add_parser(
        "create",
        help="create a domain",
        description="Create a time-aware domain package rooted in TemporalRecord.",
    )
    name = create.add_mutually_exclusive_group()
    name.add_argument("domain_name", nargs="?", help="domain name")
    name.add_argument(
        "--name",
        "-n",
        dest="domain_name_option",
        help="domain name",
    )
    domain_sub.add_parser("list", help="list known domains")
