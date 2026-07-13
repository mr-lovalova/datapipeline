from functools import partial

from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode
from datapipeline.pipelines.shared.record_nodes import order_records
from datapipeline.pipelines.shared.transform_nodes import (
    build_stream_transform_nodes,
)
from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    require_runtime_stream,
)


def build_stream_nodes(
    context: PipelineContext,
    stream_id: str,
) -> tuple[PipelineNode, ...]:
    stream = require_runtime_stream(context.runtime, stream_id)
    if isinstance(stream, DerivedRuntimeStream):
        callable_node = PipelineNode(name="map_records", apply=stream.mapper)
    elif isinstance(stream, AlignedRuntimeStream):
        callable_node = PipelineNode(name="combine_records", apply=stream.combine)
    else:
        raise TypeError(f"Stream '{stream_id}' is not derived or aligned")
    transform_nodes = build_stream_transform_nodes(
        context,
        stream.transforms,
        stream.partition_by,
    )
    return (
        callable_node,
        PipelineNode(
            name="order_records",
            apply=partial(
                order_records,
                context,
                stream.partition_by,
                stream.presorted,
            ),
        ),
        *transform_nodes,
    )
