from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

from datapipeline.config.sources import (
    CoreIoLoaderConfig,
    EntryPointConfig,
    FsParquetSourceArgs,
    FsSourceArgs,
    SourceConfig,
)
from datapipeline.plugins import LOADERS_EP, MAPPERS_EP, PARSERS_EP
from datapipeline.services.path_policy import resolve_relative_fs_loader_path
from datapipeline.sources.models.source import Source
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


def build_source(config: SourceConfig, project_yaml: Path) -> Source:
    parser_factory = load_ep(PARSERS_EP, config.parser.entrypoint)
    loader_factory = load_ep(LOADERS_EP, config.loader.entrypoint)
    if isinstance(config.loader, CoreIoLoaderConfig):
        loader_args = normalize_args(config.loader.args.model_dump(exclude_unset=True))
        if (
            isinstance(config.loader.args, (FsSourceArgs, FsParquetSourceArgs))
            and not Path(config.loader.args.path).is_absolute()
        ):
            loader_args["path"] = resolve_relative_fs_loader_path(
                config.loader.args.path,
                project_yaml.parent.resolve(),
            )
    else:
        loader_args = normalize_args(config.loader.args)
    parser_args = normalize_args(config.parser.args)
    return Source(
        loader=loader_factory(**loader_args),
        parser=parser_factory(**parser_args),
    )


def build_mapper(
    config: EntryPointConfig,
) -> Callable[[Iterator[Any]], Iterable[Any]]:
    mapper = load_ep(MAPPERS_EP, config.entrypoint)
    args = normalize_args(config.args)
    if args:
        return lambda records: mapper(records, **args)
    return mapper
