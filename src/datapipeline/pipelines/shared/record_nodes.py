from collections.abc import Iterator
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.dag.events import ProgressResource, ProgressSnapshot
from datapipeline.dag.runner import report_node_progress
from datapipeline.domain.stream import RecordStream
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.plugins import (
    DEBUG_TRANSFORMS_EP,
    RECORD_TRANSFORMS_EP,
    STREAM_TRANFORMS_EP,
)
from datapipeline.sources.observability import (
    describe_loader,
    loader_current_label,
    loader_current_resource_id,
    loader_progress_sequence,
    unit_for_loader,
)
from datapipeline.transforms.engine import apply_transforms
from datapipeline.transforms.utils import partition_key


TIME_ORDER_FIELD = "time"


def open_records(stream: RecordStream[Any]) -> Iterator[Any]:
    loader = getattr(stream, "loader", None)
    if loader is None:
        yield from stream.stream()
        return

    observability = describe_loader(loader)
    sequence = loader_progress_sequence(loader, observability)
    resources_by_id: dict[int | str, ProgressResource] = {}
    resource: ProgressResource | None
    if sequence:
        total_resources = len(sequence)
        resources_by_id = {
            entry.source_resource_id: ProgressResource(
                index=index,
                total=total_resources,
                label=entry.label,
            )
            for index, entry in enumerate(sequence, start=1)
        }
        resource = resources_by_id[sequence[0].source_resource_id]
    else:
        label = loader_current_label(loader, observability)
        resource = (
            ProgressResource(index=1, total=1, label=label)
            if label is not None
            else None
        )
    unit = unit_for_loader(loader)
    emitted = 0

    def report_source_progress() -> None:
        report_node_progress(
            ProgressSnapshot(
                completed=emitted,
                unit=unit,
                resource=resource,
            )
        )

    report_source_progress()
    source_resource_id = loader_current_resource_id(loader)
    for record in stream.stream():
        current_resource_id = loader_current_resource_id(loader)
        if current_resource_id != source_resource_id:
            source_resource_id = current_resource_id
            current_resource: ProgressResource | None
            if sequence:
                if current_resource_id is None:
                    raise RuntimeError(
                        "Source yielded a record without identifying its resource"
                    )
                try:
                    current_resource = resources_by_id[current_resource_id]
                except KeyError as exc:
                    raise RuntimeError(
                        f"Unknown source resource {current_resource_id!r}"
                    ) from exc
            else:
                label = loader_current_label(loader, observability)
                current_resource = (
                    ProgressResource(index=1, total=1, label=label)
                    if label is not None
                    else None
                )
            if current_resource != resource:
                resource = current_resource
                report_source_progress()
        emitted += 1
        yield record


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
    records: Iterator[Any],
) -> Iterator[Any]:
    return apply_transforms(records, RECORD_TRANSFORMS_EP, operations, context)


def order_records(
    context: PipelineContext,
    partition_by: str | list[str] | None,
    ordered_by: list[str] | None,
    records: Iterator[Any],
) -> Iterator[Any]:

    def record_order_key(record: Any) -> tuple[Any, Any]:
        return partition_key(record, partition_by), record.time

    if ordered_by == required_record_order(partition_by):
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
        return
    yield from batch_sort(
        records,
        buffer_bytes=context.runtime.execution.sort_buffer_bytes,
        key=record_order_key,
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
    records: Iterator[Any],
) -> Iterator[Any]:
    return apply_transforms(
        records,
        STREAM_TRANFORMS_EP,
        operations,
        context,
        partition_by=state_partition_by,
    )


def apply_debug_operations(
    context: PipelineContext,
    operations: Any,
    state_partition_by: str | list[str] | None,
    records: Iterator[Any],
) -> Iterator[Any]:
    return apply_transforms(
        records,
        DEBUG_TRANSFORMS_EP,
        operations,
        context,
        partition_by=state_partition_by,
    )
