from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

from datapipeline.config.sources import (
    EntryPointConfig,
    FsLoaderConfig,
    HttpLoaderConfig,
    SourceConfig,
)
from datapipeline.plugins import LOADERS_EP, MAPPERS_EP, PARSERS_EP
from datapipeline.services.path_policy import resolve_relative_fs_loader_path
from datapipeline.sources.factory import build_builtin_loader
from datapipeline.sources.source import Source
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


def build_source(config: SourceConfig, project_yaml: Path) -> Source:
    parser_factory = load_ep(PARSERS_EP, config.parser.entrypoint)
    if isinstance(config.loader, (FsLoaderConfig, HttpLoaderConfig)):
        loader_config = config.loader
        if isinstance(loader_config, FsLoaderConfig):
            loader_config = loader_config.model_copy(
                update={
                    "path": resolve_relative_fs_loader_path(
                        loader_config.path,
                        project_yaml.parent.resolve(),
                    )
                }
            )
        loader = build_builtin_loader(loader_config)
    else:
        loader_factory = load_ep(LOADERS_EP, config.loader.entrypoint)
        loader_args = normalize_args(config.loader.args)
        loader = loader_factory(**loader_args)
    parser_args = normalize_args(config.parser.args)
    return Source(
        loader=loader,
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
