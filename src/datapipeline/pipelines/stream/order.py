from collections.abc import Iterator
from functools import partial
from typing import Any

from datapipeline.domain.stream import canonical_record_order
from datapipeline.execution.node import PipelineNode
from datapipeline.pipelines.sort import SortProgress, batch_sort
from datapipeline.transforms.utils import partition_key


def build_record_order_node(
    partition_by: tuple[str, ...],
    presorted: bool,
    buffer_bytes: int,
) -> PipelineNode:
    if presorted:
        return PipelineNode(
            name="order_records",
            apply=partial(validate_record_order, partition_by),
        )

    progress = SortProgress()
    return PipelineNode(
        name="order_records",
        apply=partial(sort_records, partition_by, buffer_bytes, progress),
        progress=progress.snapshot,
    )


def validate_record_order(
    partition_by: tuple[str, ...],
    records: Iterator[Any],
) -> Iterator[Any]:
    def record_order_key(record: Any) -> tuple[Any, Any]:
        return partition_key(record, partition_by), record.time

    ordered_by = list(canonical_record_order(partition_by))
    previous_key = None
    for position, record in enumerate(records, start=1):
        current_key = record_order_key(record)
        if previous_key is not None and not previous_key <= current_key:
            raise ValueError(
                f"Record {position} violates declared ordered_by {ordered_by!r}: "
                f"key {current_key!r} follows {previous_key!r}."
            )
        previous_key = current_key
        yield record


def sort_records(
    partition_by: tuple[str, ...],
    buffer_bytes: int,
    progress: SortProgress,
    records: Iterator[Any],
) -> Iterator[Any]:
    def record_order_key(record: Any) -> tuple[Any, Any]:
        return partition_key(record, partition_by), record.time

    yield from batch_sort(
        records,
        buffer_bytes=buffer_bytes,
        key=record_order_key,
        progress=progress,
    )
