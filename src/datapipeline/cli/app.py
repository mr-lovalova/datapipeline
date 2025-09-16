import argparse
from datapipeline.cli.commands.run import handle as handle_run
from datapipeline.cli.commands.analyze import handle as handle_analyze
from datapipeline.cli.commands.plugin import handle as handle_plugin
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.link import handle as handle_link
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.filter import handle as handle_filter


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="datapipeline", description="DataPipeline CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # run
    p_run = sub.add_parser(
        "run", help="run records/features/vectors using config/project.yaml")
    p_run.add_argument(
        "--project", "-p", default="config/project.yaml", help="path to project.yaml")
    p_run.add_argument(
        "--stage", "-s", choices=["records", "features", "vectors"], default="vectors")
    p_run.add_argument("--limit", "-n", type=int, default=20)
    p_run.add_argument("--only", nargs="*", default=None)

    # analyze
    p_an = sub.add_parser(
        "analyze", help="analyze vectors using config/project.yaml")
    p_an.add_argument(
        "--project", "-p", default="config/project.yaml", help="path to project.yaml")
    p_an.add_argument("--limit", "-n", type=int, default=None)

    # plugin
    p_plugin = sub.add_parser("plugin", help="manage plugins")
    plugin_sub = p_plugin.add_subparsers(dest="plugin_cmd", required=True)
    p_init = plugin_sub.add_parser("init", help="scaffold a new plugin")
    p_init.add_argument("--name", "-n", required=True)
    p_init.add_argument("--out", "-o", default=".")

    # source
    p_src = sub.add_parser("source", help="manage sources (see 'source create -h' for examples)")
    src_sub = p_src.add_subparsers(dest="src_cmd", required=True)
    p_src_create = src_sub.add_parser(
        "create",
        help="create a provider+dataset source",
        description=(
            "Scaffold a source using transport + format.\n\n"
            "Examples:\n"
            "  fs CSV:        -t fs  -f csv\n"
            "  fs NDJSON:     -t fs  -f json-lines\n"
            "  URL JSON:      -t url -f json\n"
            "  Synthetic:     -t synthetic\n\n"
            "Note: set 'glob: true' in the generated YAML if your 'path' contains wildcards."
        ),
    )
    p_src_create.add_argument("--provider", "-p", required=True)
    p_src_create.add_argument("--dataset", "-d", required=True)
    p_src_create.add_argument(
        "--transport", "-t",
        choices=["fs", "url", "synthetic"],
        required=True,
        help="how data is accessed: fs/url/synthetic",
    )
    p_src_create.add_argument(
        "--format", "-f",
        choices=["csv", "json", "json-lines"],
        help="data format for fs/url transports (ignored otherwise)",
    )

    # domain
    p_dom = sub.add_parser(
        "domain",
        help="manage domains (try: 'datapipeline domain create -h')",
    )
    dom_sub = p_dom.add_subparsers(dest="dom_cmd", required=True)
    p_dom_create = dom_sub.add_parser(
        "create",
        help="create a domain",
        description=(
            "Create a domain package. Defaults to Record base. "
            "Use --time-aware to base on TimeFeatureRecord (adds 'time' and 'value' fields)."
        ),
    )
    p_dom_create.add_argument("--domain", "-d", required=True)
    p_dom_create.add_argument(
        "--time-aware",
        "-t",
        action="store_true",
        help="use TimeFeatureRecord base (UTC-aware 'time' + 'value' fields) instead of Record",
    )

    # link
    p_link = sub.add_parser(
        "link", help="link a source to a domain (create mapper)")
    p_link.add_argument("--time-aware", "-t", action="store_true")

    # list
    p_list = sub.add_parser("list", help="list registered sources or domains")
    list_sub = p_list.add_subparsers(dest="list_cmd", required=True)
    list_sub.add_parser("sources", help="list sources")
    list_sub.add_parser("domains", help="list domains")

    # filter
    p_filt = sub.add_parser("filter", help="manage filters")
    filt_sub = p_filt.add_subparsers(dest="filter_cmd", required=True)
    p_filt_create = filt_sub.add_parser("create", help="create a filter function")
    p_filt_create.add_argument("--name", "-n", required=True, help="filter entrypoint name and function/module name")

    args = parser.parse_args()
    if args.cmd == "run":
        handle_run(project=args.project, stage=args.stage,
                   limit=args.limit)
        return
    if args.cmd == "analyze":
        handle_analyze(project=args.project, limit=getattr(args, "limit", None))
        return
    if args.cmd == "plugin":
        handle_plugin(subcmd=args.plugin_cmd, name=getattr(
            args, "name", None), out=getattr(args, "out", "."))
        return
    if args.cmd == "source":
        handle_source(
            subcmd=args.src_cmd,
            provider=getattr(args, "provider", None),
            dataset=getattr(args, "dataset", None),
            transport=getattr(args, "transport", None),
            format=getattr(args, "format", None),
        )
        return
    if args.cmd == "domain":
        handle_domain(subcmd=args.dom_cmd, domain=getattr(args, "domain", None),
                      time_aware=getattr(args, "time_aware", False))
        return
    if args.cmd == "link":
        handle_link(time_aware=getattr(args, "time_aware", False))
        return
    if args.cmd == "list":
        handle_list(subcmd=args.list_cmd)
        return
    if args.cmd == "filter":
        handle_filter(subcmd=args.filter_cmd, name=getattr(args, "name", None))
        return
