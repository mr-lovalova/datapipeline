from collections.abc import Iterator, Mapping
from typing import Any

from datapipeline.cache import cached_record_stream
from datapipeline.dag.context import PipelineContext


def close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def open_input_records(
    context: PipelineContext,
    refs: Mapping[str, str],
    owner: str,
) -> tuple[dict[str, str], dict[str, Iterator[Any]], list[Iterator[Any]]]:
    known_streams = set(context.runtime.registries.stream_sources.keys())
    upstreams: list[Iterator[Any]] = []
    records: dict[str, Iterator[Any]] = {}
    for alias, ref in refs.items():
        if ref not in known_streams:
            raise ValueError(
                f"Stream '{owner}' references unknown stream '{ref}'. "
                f"Known streams: {sorted(known_streams)}"
            )
        upstream = iter(cached_record_stream(context, ref))
        upstreams.append(upstream)
        records[alias] = _unwrap_records(upstream)
    return refs, records, upstreams


def _unwrap_records(iterator: Iterator[Any]) -> Iterator[Any]:
    for item in iterator:
        yield getattr(item, "record", item)
