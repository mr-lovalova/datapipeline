from datapipeline.utils.load import load_ep
from datapipeline.plugins import PARSERS_EP, LOADERS_EP, MAPPERS_EP
from datapipeline.sources.models.source import Source
from datapipeline.config.catalog import SourceConfig, EPArgs
from datapipeline.mappers.noop import identity
from datapipeline.utils.placeholders import normalize_args


def build_source_from_spec(spec: SourceConfig) -> Source:
    P = load_ep(PARSERS_EP, spec.parser.entrypoint)
    L = load_ep(LOADERS_EP, spec.loader.entrypoint)
    loader_args = normalize_args(spec.loader.args)
    parser_args = normalize_args(spec.parser.args)
    return Source(loader=L(**loader_args), parser=P(**parser_args))


def build_mapper_from_spec(spec: EPArgs | None):
    """Return a callable(raw_iter) -> iter with args bound if present."""
    if not spec or not spec.entrypoint:
        return identity
    fn = load_ep(MAPPERS_EP, spec.entrypoint)
    args = normalize_args(spec.args)
    if args:
        return lambda raw: fn(raw, **args)
    return fn
