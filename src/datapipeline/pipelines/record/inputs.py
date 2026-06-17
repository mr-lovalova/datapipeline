from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.record.streams import open_record_stream


def close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


@contextmanager
def open_input_records(
    context: PipelineContext,
    refs: Mapping[str, str],
    owner: str,
) -> Iterator[dict[str, Iterator[Any]]]:
    known_streams = set(context.runtime.registries.stream_sources.keys())
    upstreams: list[Iterator[Any]] = []
    records: dict[str, Iterator[Any]] = {}
    try:
        for alias, ref in refs.items():
            if ref not in known_streams:
                raise ValueError(
                    f"Stream '{owner}' references unknown stream '{ref}'. "
                    f"Known streams: {sorted(known_streams)}"
                )
            upstream = iter(open_record_stream(context, ref))
            upstreams.append(upstream)
            records[alias] = _unwrap_records(upstream)
        yield records
    finally:
        for upstream in upstreams:
            close_iterator(upstream)


def _unwrap_records(iterator: Iterator[Any]) -> Iterator[Any]:
    for item in iterator:
        yield getattr(item, "record", item)
