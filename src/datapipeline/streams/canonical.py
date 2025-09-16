from typing import NamedTuple, Callable, Iterator, Any, Dict, Tuple
from datapipeline.streams.raw import open_raw_stream
_CANON: Dict[str, Tuple[str, Callable[..., Iterator[Any]], dict]] = {}


def register_stream(alias: str, source_alias: str, mapper: Callable[..., Iterator[Any]], mapper_args: dict | None = None) -> None:
    _CANON[alias] = (source_alias, mapper, mapper_args or {})


class CanonicalEntry(NamedTuple):
    source_alias: str
    mapper: Callable[..., Iterator[Any]]
    mapper_args: dict


def canonical_entry(alias: str) -> CanonicalEntry:
    src_alias, mapper, args = _CANON[alias]
    return CanonicalEntry(src_alias, mapper, dict(args))


def open_canonical_stream(alias: str) -> Iterator[Any]:
    try:
        source_alias, mapper, kwargs = _CANON[alias]
    except KeyError:
        raise ValueError(
            f"Unknown stream alias '{alias}'. Available: {', '.join(sorted(_CANON))}"
        )
    return mapper(open_raw_stream(source_alias), **kwargs)
