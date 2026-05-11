from collections.abc import Iterable
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.transforms.engine import apply_transforms
from datapipeline.plugins import (
    DEBUG_TRANSFORMS_EP,
    RECORD_TRANSFORMS_EP,
    STREAM_TRANFORMS_EP,
)
from datapipeline.transforms.utils import partition_key

RECORD_NODE_COUNT = 6


def open_stream(stream):
    return stream.stream()


def map_records(mapper: Any, records: Iterable[Any] | None) -> Iterable[Any]:
    return mapper(records)


def apply_record_operations(
    context: PipelineContext,
    operations: Any,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return apply_transforms(records, RECORD_TRANSFORMS_EP, operations, context)


def order_records(
    batch_size: int,
    partition_by: str | list[str] | None,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return batch_sort(
        records,
        batch_size=batch_size,
        key=lambda rec: (partition_key(rec, partition_by), rec.time),
    )


def apply_stream_operations(
    context: PipelineContext,
    operations: Any,
    partition_by: str | list[str] | None,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return apply_transforms(
        records,
        STREAM_TRANFORMS_EP,
        operations,
        context,
        extra_kwargs={"partition_by": partition_by},
    )


def apply_debug_operations(
    context: PipelineContext,
    operations: Any,
    partition_by: str | list[str] | None,
    records: Iterable[Any] | None,
) -> Iterable[Any]:
    return apply_transforms(
        records,
        DEBUG_TRANSFORMS_EP,
        operations,
        context,
        extra_kwargs={"partition_by": partition_by},
    )
