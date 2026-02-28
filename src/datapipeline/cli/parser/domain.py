import argparse


def add_domain_command(sub, *, common: argparse.ArgumentParser) -> None:
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
    create.add_argument("domain", nargs="?", help="domain name")
    create.add_argument("--name", "-n", dest="domain", help="domain name")
    domain_sub.add_parser("list", help="list known domains")
