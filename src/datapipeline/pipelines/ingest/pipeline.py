from functools import partial

from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import Node, PipelineNode, SourceNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.pipelines.shared.record_nodes import (
    open_records,
    order_records,
)
from datapipeline.pipelines.shared.transform_nodes import (
    build_record_transform_nodes,
)
from datapipeline.sources.observability import source_summary
from datapipeline.runtime import (
    IngestRuntimeStream,
    require_runtime_stream,
)


def build_ingest_pipeline(
    context: PipelineContext,
    stream_id: str,
) -> Pipeline:
    stream = _ingest_stream(context, stream_id)
    return Pipeline(
        name=f"ingest:{stream_id}",
        nodes=build_ingest_nodes(context, stream_id),
        summary=source_summary(stream.source),
    )


def build_ingest_nodes(
    context: PipelineContext,
    stream_id: str,
) -> tuple[Node, ...]:
    stream = _ingest_stream(context, stream_id)
    return (
        SourceNode(
            name="open_source",
            open=partial(open_records, stream.source),
        ),
        PipelineNode(
            name="map_records",
            apply=stream.mapper,
        ),
        *build_record_transform_nodes(stream.transforms),
        PipelineNode(
            name="order_records",
            apply=partial(
                order_records,
                context,
                stream.partition_by,
                stream.presorted,
            ),
        ),
    )


def _ingest_stream(
    context: PipelineContext,
    stream_id: str,
) -> IngestRuntimeStream:
    stream = require_runtime_stream(context.runtime, stream_id)
    if not isinstance(stream, IngestRuntimeStream):
        raise TypeError(f"Stream '{stream_id}' is not an ingest")
    return stream
