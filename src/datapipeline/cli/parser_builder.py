import argparse

from datapipeline.cli.parser.build import add_build_command
from datapipeline.cli.parser.clean import add_clean_command
from datapipeline.cli.parser.common import build_common_parent
from datapipeline.cli.parser.demo import add_demo_command
from datapipeline.cli.parser.domain import add_domain_command
from datapipeline.cli.parser.filter import add_filter_command
from datapipeline.cli.parser.inflow import add_inflow_command
from datapipeline.cli.parser.inspect import add_inspect_command
from datapipeline.cli.parser.list_ import add_list_command
from datapipeline.cli.parser.plugin import add_plugin_command
from datapipeline.cli.parser.scaffold import add_simple_scaffold_command
from datapipeline.cli.parser.serve import add_serve_command
from datapipeline.cli.parser.source import add_source_command
from datapipeline.cli.parser.stream import add_stream_command


def build_parser() -> argparse.ArgumentParser:
    common = build_common_parent()
    parser = argparse.ArgumentParser(
        prog="jerry",
        description="Mixology-themed CLI for building and serving data pipelines.",
        parents=[common],
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    add_serve_command(sub, common=common)
    add_inspect_command(sub, common=common)
    add_build_command(sub, common=common)
    add_clean_command(sub, common=common)
    add_demo_command(sub, common=common)
    add_list_command(sub, common=common)
    add_source_command(sub, common=common)
    add_domain_command(sub, common=common)
    add_simple_scaffold_command(
        sub,
        common=common,
        cmd="dto",
        help_text="create DTOs",
        arg_help="DTO class name",
    )
    add_simple_scaffold_command(
        sub,
        common=common,
        cmd="parser",
        help_text="create parsers",
        arg_help="Parser class name",
    )
    add_simple_scaffold_command(
        sub,
        common=common,
        cmd="mapper",
        help_text="create mappers",
        arg_help="Mapper function name",
    )
    add_simple_scaffold_command(
        sub,
        common=common,
        cmd="loader",
        help_text="create loaders",
        arg_help="Loader name",
    )
    add_inflow_command(sub, common=common)
    add_stream_command(sub, common=common)
    add_plugin_command(sub, common=common)
    add_filter_command(sub, common=common)
    return parser
