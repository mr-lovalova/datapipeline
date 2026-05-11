from datapipeline.config.catalog import EPArgs, SourceConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.mappers.noop import identity
from datapipeline.plugins import LOADERS_EP, MAPPERS_EP, PARSERS_EP
from datapipeline.sources.models.source import Source
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args

from .common import iter_mapped


def build_source_from_spec(spec: SourceConfig) -> Source:
    parser_cls = load_ep(PARSERS_EP, spec.parser.entrypoint)
    loader_cls = load_ep(LOADERS_EP, spec.loader.entrypoint)
    loader_args = normalize_args(spec.loader.args)
    parser_args = normalize_args(spec.parser.args)
    return Source(loader=loader_cls(**loader_args), parser=parser_cls(**parser_args))


def build_mapper_from_spec(
    spec: EPArgs | None,
    *,
    runtime=None,
    row_mapper: bool = False,
):
    """Return a callable(raw_iter) -> iter with args bound if present."""
    if not spec or not spec.entrypoint:
        return identity
    fn = load_ep(MAPPERS_EP, spec.entrypoint)
    args = normalize_args(spec.args)
    if row_mapper:
        context = PipelineContext(runtime) if runtime is not None else None

        def _map_rows(rows):
            for row in rows:
                yield from iter_mapped(fn(row, context=context, **args))

        return _map_rows
    if args:
        return lambda raw: fn(raw, **args)
    return fn
