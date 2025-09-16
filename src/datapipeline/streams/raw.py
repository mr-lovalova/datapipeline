from typing import Dict, Iterator
from datapipeline.sources.models.source import Source

_SOURCES: Dict[str, Source] = {}


def register_source(alias: str, source: Source) -> None:
    _SOURCES[alias] = source


def get_source(alias: str) -> Source:
    return _SOURCES[alias]


def open_raw_stream(alias: str) -> Iterator[object]:
    return get_source(alias).stream()
