import argparse

from datapipeline.cli.commands.run import handle_prep, handle_serve
from datapipeline.cli.commands.analyze import analyze as handle_analyze
from datapipeline.cli.commands.plugin import station as handle_station
from datapipeline.cli.commands.source import handle as handle_source
from datapipeline.cli.commands.domain import handle as handle_domain
from datapipeline.cli.commands.link import handle as handle_link
from datapipeline.cli.commands.list_ import handle as handle_list
from datapipeline.cli.commands.filter import handle as handle_filter


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="jerry",
        description="Mixology-themed CLI for building and serving data pipelines.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # prep (debug mode with visuals)
    p_prep = sub.add_parser(
        "prep",
        help="run pipeline stages with visual progress",
    )
    prep_sub = p_prep.add_subparsers(dest="prep_cmd", required=True)
    prep_steps = {
        "pour": "preview record-stage output",
        "build": "inspect feature-stage output",
        "stir": "examine vector-stage output",
    }
    for step, help_text in prep_steps.items():
        sp = prep_sub.add_parser(step, help=help_text)
        sp.add_argument(
            "--project", "-p", default="config/project.yaml", help="path to project.yaml"
        )
        sp.add_argument("--limit", "-n", type=int, default=20)

    # serve (production run, no visuals)
    p_serve = sub.add_parser(
        "serve",
        help="produce vectors without progress visuals",
    )
    p_serve.add_argument(
        "--project", "-p", default="config/project.yaml", help="path to project.yaml"
    )
    p_serve.add_argument(
        "--limit", "-n", type=int, default=None,
        help="optional cap on the number of vectors to emit",
    )
    p_serve.add_argument(
        "--output", "-o", default="print",
        help="output destination: 'print', 'stream', or a file ending in .pt",
    )

    # taste (analysis)
    p_taste = sub.add_parser(
        "taste",
        help="analyze vector completeness and feature stats",
    )
    p_taste.add_argument(
        "--project", "-p", default="config/project.yaml", help="path to project.yaml"
    )
    p_taste.add_argument("--limit", "-n", type=int, default=None)

    # distillery (sources)
    p_dist = sub.add_parser(
        "distillery",
        help="add or list raw sources",
    )
    dist_sub = p_dist.add_subparsers(dest="dist_cmd", required=True)
    p_dist_add = dist_sub.add_parser(
        "add",
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
    p_dist_add.add_argument("--provider", "-p", required=True)
    p_dist_add.add_argument("--dataset", "-d", required=True)
    p_dist_add.add_argument(
        "--transport", "-t",
        choices=["fs", "url", "synthetic"],
        required=True,
        help="how data is accessed: fs/url/synthetic",
    )
    p_dist_add.add_argument(
        "--format", "-f",
        choices=["csv", "json", "json-lines"],
        help="data format for fs/url transports (ignored otherwise)",
    )
    dist_sub.add_parser("list", help="list known sources")

    # spirit (domains)
    p_spirit = sub.add_parser(
        "spirit",
        help="add or list domains",
    )
    spirit_sub = p_spirit.add_subparsers(dest="spirit_cmd", required=True)
    p_spirit_add = spirit_sub.add_parser(
        "add",
        help="create a domain",
        description=(
            "Create a domain package. Defaults to Record base. "
            "Use --time-aware to base on TimeFeatureRecord (adds 'time' and 'value' fields)."
        ),
    )
    p_spirit_add.add_argument("--domain", "-d", required=True)
    p_spirit_add.add_argument(
        "--time-aware",
        "-t",
        action="store_true",
        help="use TimeFeatureRecord base (UTC-aware 'time' + 'value' fields) instead of Record",
    )
    spirit_sub.add_parser("list", help="list known domains")

    # contract (link source ↔ domain)
    p_contract = sub.add_parser(
        "contract",
        help="link a distillery source to a spirit domain",
    )
    p_contract.add_argument("--time-aware", "-t", action="store_true")

    # station (plugin scaffolding)
    p_station = sub.add_parser(
        "station",
        help="scaffold plugin workspaces",
    )
    station_sub = p_station.add_subparsers(dest="station_cmd", required=True)
    p_station_init = station_sub.add_parser(
        "init", help="create a plugin skeleton")
    p_station_init.add_argument("--name", "-n", required=True)
    p_station_init.add_argument("--out", "-o", default=".")

    # filter (unchanged helper)
    p_filt = sub.add_parser("filter", help="manage filters")
    filt_sub = p_filt.add_subparsers(dest="filter_cmd", required=True)
    p_filt_create = filt_sub.add_parser(
        "create", help="create a filter function")
    p_filt_create.add_argument(
        "--name", "-n", required=True,
        help="filter entrypoint name and function/module name",
    )

    args = parser.parse_args()

    if args.cmd == "prep":
        handle_prep(action=args.prep_cmd,
                    project=args.project, limit=args.limit)
        return

    if args.cmd == "serve":
        handle_serve(
            project=args.project,
            limit=getattr(args, "limit", None),
            output=args.output,
        )
        return

    if args.cmd == "taste":
        handle_analyze(project=args.project,
                       limit=getattr(args, "limit", None))
        return

    if args.cmd == "distillery":
        if args.dist_cmd == "list":
            handle_list(subcmd="sources")
        else:
            handle_source(
                subcmd="add",
                provider=getattr(args, "provider", None),
                dataset=getattr(args, "dataset", None),
                transport=getattr(args, "transport", None),
                format=getattr(args, "format", None),
            )
        return

    if args.cmd == "spirit":
        if args.spirit_cmd == "list":
            handle_list(subcmd="domains")
        else:
            handle_domain(
                subcmd="add",
                domain=getattr(args, "domain", None),
                time_aware=getattr(args, "time_aware", False),
            )
        return

    if args.cmd == "contract":
        handle_link(time_aware=getattr(args, "time_aware", False))
        return

    if args.cmd == "station":
        handle_station(
            subcmd=args.station_cmd,
            name=getattr(args, "name", None),
            out=getattr(args, "out", "."),
        )
        return

    if args.cmd == "filter":
        handle_filter(subcmd=args.filter_cmd, name=getattr(args, "name", None))
        return
