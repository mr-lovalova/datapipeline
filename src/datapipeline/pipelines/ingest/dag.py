from collections.abc import Iterator
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.dag.dag import Dag
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.runner import run_dag
from datapipeline.pipelines.shared.record_nodes import (
    apply_record_operations,
    map_records,
    open_records,
    order_records,
    require_stream_source,
)

def build_ingest_pipeline(
    context: PipelineContext,
    stream_id: str,
    node: int | None = None,
) -> Iterator[Any]:
    dag = build_ingest_dag(context, stream_id).upto_node(node)
    return run_dag(context, dag)


def build_ingest_dag(
    context: PipelineContext,
    stream_id: str,
) -> Dag:
    return Dag(
        name=f"ingest:{stream_id}",
        nodes=build_ingest_nodes(context, stream_id),
    )


def build_ingest_nodes(
    context: PipelineContext,
    stream_id: str,
) -> tuple[PipelineNode, ...]:
    registries = context.runtime.registries
    source = require_stream_source(context, stream_id)
    mapper = registries.mappers.get(stream_id)
    record_operations = registries.record_operations.get(stream_id)
    partition_by = registries.partition_by.get(stream_id)
    batch_size = registries.sort_batch_size.get(stream_id)
    return (
        PipelineNode(
            name="open_source",
            op=open_records,
            args=(source,),
            output="dtos",
        ),
        PipelineNode(
            input="dtos",
            name="map_records",
            op=map_records,
            args=(mapper,),
            output="mapped",
        ),
        PipelineNode(
            input="mapped",
            name="record_transforms",
            op=apply_record_operations,
            args=(context, record_operations),
            output="transformed",
        ),
        PipelineNode(
            input="transformed",
            name="order_records",
            op=order_records,
            args=(context, batch_size, partition_by),
            output="ordered",
        ),
    )
