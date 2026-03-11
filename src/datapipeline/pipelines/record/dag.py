from collections.abc import Iterator
from typing import Any

from datapipeline.dag.dag import StageDag
from datapipeline.dag.runner import run_stage_dag
from datapipeline.dag.node import PipelineStep
from datapipeline.pipelines.record.nodes import (
    apply_debug_operations,
    apply_record_operations,
    apply_stream_operations,
    map_records,
    open_source,
    order_records,
)
from datapipeline.dag.context import PipelineContext


def build_record_pipeline(
    context: PipelineContext,
    record_stream_id: str,
    stage: int | None = None,
) -> Iterator[Any]:
    dag = StageDag(
        name=f"record:{record_stream_id}",
        nodes=build_record_nodes(context, record_stream_id),
    ).upto_stage(stage)
    return run_stage_dag(context, dag)


def build_record_nodes(
    context: PipelineContext,
    record_stream_id: str,
) -> tuple[PipelineStep, ...]:
    registries = context.runtime.registries
    source = _require_source(context, record_stream_id)
    mapper = registries.mappers.get(record_stream_id)
    record_operations = registries.record_operations.get(record_stream_id)
    stream_operations = registries.stream_operations.get(record_stream_id)
    debug_operations = registries.debug_operations.get(record_stream_id)
    partition_by = registries.partition_by.get(record_stream_id)
    batch_size = registries.sort_batch_size.get(record_stream_id)
    return (
        PipelineStep(
            name="open_source",
            op=open_source,
            args=(source,),
            output="dtos",
        ),
        PipelineStep(
            input="dtos",
            name="map_records",
            op=map_records,
            args=(mapper,),
            output="mapped",
        ),
        PipelineStep(
            input="mapped",
            name="record_transforms",
            op=apply_record_operations,
            args=(context, record_operations),
            output="transformed",
        ),
        PipelineStep(
            input="transformed",
            name="order_records",
            op=order_records,
            args=(batch_size, partition_by),
            output="ordered",
        ),
        PipelineStep(
            input="ordered",
            name="stream_transforms",
            op=apply_stream_operations,
            args=(context, stream_operations, partition_by),
            output="stream_transforms",
        ),
        PipelineStep(
            input="stream_transforms",
            name="debug_transforms",
            op=apply_debug_operations,
            args=(context, debug_operations, partition_by),
            output="stream_transforms",
        ),
    )


def _require_source(context: PipelineContext, record_stream_id: str) -> Any:
    source_registry = context.runtime.registries.stream_sources
    try:
        return source_registry.get(record_stream_id)
    except KeyError as exc:
        available = sorted(source_registry.keys())
        available_text = ", ".join(available) if available else "(none)"
        raise KeyError(
            "Unknown record_stream "
            f"'{record_stream_id}'. Check dataset.yaml and contracts/ ids. "
            f"Available streams: {available_text}"
        ) from exc
