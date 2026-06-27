from collections.abc import Iterable
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.plugins import (
    DEBUG_TRANSFORMS_EP,
    RECORD_TRANSFORMS_EP,
    STREAM_TRANFORMS_EP,
)
from datapipeline.transforms.engine import apply_transforms
from datapipeline.transforms.utils import partition_key


TIME_ORDER_FIELD = "time"


def open_records(stream: RecordStream):
    return stream.stream()


def require_stream_source(context: PipelineContext, stream_id: str) -> Any:
    source_registry = context.runtime.registries.stream_sources
    try:
        return source_registry.get(stream_id)
    except KeyError as exc:
        available = sorted(source_registry.keys())
        available_text = ", ".join(available) if available else "(none)"
        raise KeyError(
            f"Unknown stream '{stream_id}'. Check dataset.yaml and stream ids. "
            f"Available streams: {available_text}"
        ) from exc


def map_records(mapper, records):
    return mapper(records)


def apply_record_operations(
    context: PipelineContext,
    operations: Any,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return apply_transforms(records, RECORD_TRANSFORMS_EP, operations, context)


def order_records(
    context: PipelineContext,
    batch_size: int,
    partition_by: str | list[str] | None,
    ordered_by: list[str] | None,
    progress_stage: str,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    if ordered_by == required_record_order(partition_by):
        return records or ()
    return batch_sort(
        records,
        batch_size=batch_size,
        key=lambda rec: (partition_key(rec, partition_by), rec.time),
        progress_stage=progress_stage,
    )


def required_record_order(partition_by: str | list[str] | None) -> list[str]:
    if partition_by is None:
        return [TIME_ORDER_FIELD]
    if isinstance(partition_by, str):
        fields = [partition_by]
    else:
        fields = list(partition_by)
    return [*fields, TIME_ORDER_FIELD]


def apply_stream_operations(
    context: PipelineContext,
    operations: Any,
    state_partition_by: str | list[str] | None,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return apply_transforms(
        records,
        STREAM_TRANFORMS_EP,
        operations,
        context,
        extra_kwargs={"partition_by": state_partition_by},
    )


def apply_debug_operations(
    context: PipelineContext,
    operations: Any,
    state_partition_by: str | list[str] | None,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return apply_transforms(
        records,
        DEBUG_TRANSFORMS_EP,
        operations,
        context,
        extra_kwargs={"partition_by": state_partition_by},
    )
