from collections.abc import Iterator, Sequence
from typing import Any

from datapipeline.cache import cached_record_stream
from datapipeline.config.catalog import ContractConfig
from datapipeline.dag.context import PipelineContext


def close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def open_input_records(
    context: PipelineContext,
    inputs: Sequence[str] | None,
    *,
    owner: str,
) -> tuple[dict[str, str], dict[str, Iterator[Any]], list[Iterator[Any]]]:
    refs = _input_refs(inputs)
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


def _input_refs(inputs: Sequence[str] | None) -> dict[str, str]:
    return {
        alias: ref
        for alias, ref in (
            ContractConfig.parse_input_spec(item) for item in (inputs or [])
        )
    }


def _unwrap_records(iterator: Iterator[Any]) -> Iterator[Any]:
    for item in iterator:
        yield getattr(item, "record", item)
