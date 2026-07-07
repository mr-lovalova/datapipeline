from collections.abc import Iterator
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.dag.dag import Dag
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.runner import run_dag
from datapipeline.pipelines.shared.record_nodes import (
    apply_debug_operations,
    apply_stream_operations,
    map_records,
    open_records,
    order_records,
    require_stream_source,
)
from datapipeline.sources.observability import source_metadata


def build_stream_pipeline(
    context: PipelineContext,
    stream_id: str,
    node: int | None = None,
) -> Iterator[Any]:
    dag = build_stream_dag(context, stream_id).upto_node(node)
    return run_dag(context, dag)


def build_stream_dag(
    context: PipelineContext,
    stream_id: str,
) -> Dag:
    source = require_stream_source(context, stream_id)
    metadata = source_metadata(source)
    return Dag(
        name=f"stream:{stream_id}",
        nodes=build_stream_nodes(context, stream_id),
        metadata={"source": metadata} if metadata else None,
    )


def build_stream_nodes(
    context: PipelineContext,
    stream_id: str,
) -> tuple[PipelineNode, ...]:
    registries = context.runtime.registries
    source = require_stream_source(context, stream_id)
    mapper = registries.mappers.get(stream_id)
    stream_operations = registries.stream_operations.get(stream_id)
    debug_operations = registries.debug_operations.get(stream_id)
    state_by = registries.partition_by.get(stream_id)
    try:
        ordered_by = registries.ordered_by.get(stream_id)
    except KeyError:
        ordered_by = None
    batch_size = registries.sort_batch_size.get(stream_id)
    return (
        PipelineNode(
            name="open_records",
            op=open_records,
            args=(source,),
            output="records",
        ),
        PipelineNode(
            input="records",
            name="map_records",
            op=map_records,
            args=(mapper,),
            output="mapped",
        ),
        PipelineNode(
            input="mapped",
            name="order_records",
            op=order_records,
            args=(context, batch_size, state_by, ordered_by),
            output="ordered",
        ),
        PipelineNode(
            input="ordered",
            name="stream_transforms",
            op=apply_stream_operations,
            args=(context, stream_operations, state_by),
            output="stream_transforms",
        ),
        PipelineNode(
            input="stream_transforms",
            name="debug_transforms",
            op=apply_debug_operations,
            args=(context, debug_operations, state_by),
            output="stream_transforms",
        ),
    )
