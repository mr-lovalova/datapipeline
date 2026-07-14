from pathlib import Path

from datapipeline.config.catalog import (
    CoreIoLoaderConfig,
    EntryPointConfig,
    FsSourceArgs,
    SourceConfig,
)
from datapipeline.mappers.noop import identity
from datapipeline.plugins import LOADERS_EP, MAPPERS_EP, PARSERS_EP
from datapipeline.services.path_policy import resolve_relative_fs_loader_path
from datapipeline.sources.models.source import Source
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


def build_source_from_spec(
    spec: SourceConfig,
    project_yaml: Path | None = None,
) -> Source:
    parser_cls = load_ep(PARSERS_EP, spec.parser.entrypoint)
    loader_cls = load_ep(LOADERS_EP, spec.loader.entrypoint)
    if isinstance(spec.loader, CoreIoLoaderConfig):
        loader_args = normalize_args(spec.loader.args.model_dump(exclude_unset=True))
        if (
            project_yaml is not None
            and isinstance(spec.loader.args, FsSourceArgs)
            and not Path(spec.loader.args.path).is_absolute()
        ):
            loader_args["path"] = resolve_relative_fs_loader_path(
                spec.loader.args.path,
                project_yaml.parent.resolve(),
            )
    else:
        loader_args = normalize_args(spec.loader.args)
    parser_args = normalize_args(spec.parser.args)
    return Source(loader=loader_cls(**loader_args), parser=parser_cls(**parser_args))


def build_mapper_from_spec(spec: EntryPointConfig | None):
    """Return a callable(raw_iter) -> iter with args bound if present."""
    if spec is None:
        return identity
    fn = load_ep(MAPPERS_EP, spec.entrypoint)
    args = normalize_args(spec.args)
    if args:
        return lambda raw: fn(raw, **args)
    return fn
