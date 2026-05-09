from collections.abc import Iterator
from typing import Any

from datapipeline.cache import cached_record_stream
from datapipeline.config.catalog import ContractConfig
from datapipeline.dag.context import PipelineContext


def close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def resolve_input_streams(
    context: PipelineContext,
    spec: ContractConfig,
) -> dict[str, Iterator[Any]]:
    runtime = context.runtime
    known_streams = set(runtime.registries.stream_sources.keys())

    out: dict[str, Iterator[Any]] = {}
    for item in spec.inputs or []:
        alias, ref = ContractConfig.parse_input_spec(item)
        if ref not in known_streams:
            raise ValueError(
                f"{spec.kind.capitalize()} stream '{spec.id}' references unknown "
                f"stream '{ref}'. Known streams: {sorted(known_streams)}"
            )
        out[alias] = cached_record_stream(context, ref)
    return out


def unwrap_records(iterator: Iterator[Any]) -> Iterator[Any]:
    for item in iterator:
        yield getattr(item, "record", item)
