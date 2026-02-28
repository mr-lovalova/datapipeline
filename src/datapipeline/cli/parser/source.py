import argparse

from datapipeline.config.options import SOURCE_FS_FORMATS, SOURCE_TRANSPORTS


def add_source_command(sub, *, common: argparse.ArgumentParser) -> None:
    parser = sub.add_parser(
        "source",
        help="create or list raw sources",
        parents=[common],
    )
    source_sub = parser.add_subparsers(dest="source_cmd", required=True)
    create = source_sub.add_parser(
        "create",
        help="create a provider+dataset source (yaml only)",
        description=(
            "Create a source YAML using transport + format or a loader entrypoint.\n\n"
            "Usage:\n"
            "  jerry source create <provider>.<dataset> -t fs -f csv\n"
            "  jerry source create <provider>.<dataset> -t http -f json\n"
            "  jerry source create <provider>.<dataset> -t synthetic\n\n"
            "  jerry source create <provider> <dataset> --loader mypkg.loaders.demo:Loader\n"
            "  jerry source create <provider> <dataset> --parser myparser\n\n"
            "Examples:\n"
            "  fs CSV:        -t fs  -f csv\n"
            "  fs NDJSON:     -t fs  -f jsonl\n"
            "  HTTP JSON:     -t http -f json\n"
            "  Synthetic:     -t synthetic\n\n"
            "Note: set 'glob: true' in the generated YAML if your 'path' contains wildcards."
        ),
    )
    create.add_argument("provider", nargs="?", help="provider name")
    create.add_argument("dataset", nargs="?", help="dataset slug")
    create.add_argument(
        "--provider", "-p", dest="provider_opt", metavar="PROVIDER", help="provider name"
    )
    create.add_argument(
        "--dataset", "-d", dest="dataset_opt", metavar="DATASET", help="dataset slug"
    )
    create.add_argument("--alias", "-a", help="provider.dataset alias")
    create.add_argument(
        "--transport",
        "-t",
        choices=SOURCE_TRANSPORTS,
        required=False,
        help="how data is accessed: fs/http/synthetic",
    )
    create.add_argument(
        "--format",
        "-f",
        choices=SOURCE_FS_FORMATS,
        help="data format for fs/http transports (ignored otherwise)",
    )
    create.add_argument(
        "--loader",
        help="loader entrypoint (overrides --transport/--format)",
    )
    create.add_argument(
        "--parser",
        help="parser entrypoint (defaults to identity)",
    )
    create.add_argument(
        "--identity",
        action="store_true",
        help="use the built-in identity parser (alias for --parser identity)",
    )
    source_sub.add_parser("list", help="list known sources")
