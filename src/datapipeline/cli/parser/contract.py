import argparse


def add_contract_command(sub,  common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "contract",
        help="manage stream contracts (ingest or composed)",
        parents=[common],
    )
    contract_sub = parser.add_subparsers(dest="contract_cmd", required=True)
    create = contract_sub.add_parser("create", help="create a contract")
    create.add_argument(
        "--identity",
        action="store_true",
        help="use built-in identity mapper (skip mapper scaffolding)",
    )
