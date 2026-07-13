from functools import partial

from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode
from datapipeline.pipelines.shared.record_nodes import (
    map_records,
    order_records,
)
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
    if not isinstance(stream, (DerivedRuntimeStream, AlignedRuntimeStream)):
        raise TypeError(f"Stream '{stream_id}' is not a derived stream")
    transform_nodes = build_stream_transform_nodes(
        context,
        stream.transforms,
        stream.partition_by,
    )
    return (
        PipelineNode(
            name="map_records",
            apply=partial(map_records, stream.mapper),
        ),
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
