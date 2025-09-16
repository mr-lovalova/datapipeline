from datapipeline.utils.load import load_ep
from datapipeline.plugins import PARSERS_EP, LOADERS_EP, MAPPERS_EP
from datapipeline.sources.models.source import Source
from datapipeline.config.catalog import RawSourceSpec, EPArgs


def build_source_from_spec(spec: RawSourceSpec) -> Source:
    P = load_ep(PARSERS_EP, spec.parser.entrypoint)
    L = load_ep(LOADERS_EP, spec.loader.entrypoint)
    return Source(loader=L(**spec.loader.args), parser=P(**spec.parser.args))


def build_mapper_from_spec(spec: EPArgs | None):
    if not spec or not spec.entrypoint:
        from datapipeline.mappers.noop import identity
        return identity, {}
    fn = load_ep(MAPPERS_EP, spec.entrypoint)
    return fn, dict(spec.args or {})
