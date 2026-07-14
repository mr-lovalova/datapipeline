from collections.abc import Iterator
from dataclasses import replace
from functools import partial
from typing import Any

from datapipeline.alignment.engine import align_streams
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import Node, PipelineNode, SourceNode
from datapipeline.execution.observer import NoopPipelineObserver
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.stream.order import build_record_order_node
from datapipeline.pipelines.stream.transform_nodes import (
    build_record_transform_nodes,
    build_stream_transform_nodes,
)
from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    IngestRuntimeStream,
    require_runtime_stream,
)
from datapipeline.sources.observability import source_progress, source_summary


_INTERNAL_INPUT_OBSERVER = NoopPipelineObserver()


def run_stream_pipeline(
    context: PipelineContext,
    stream_id: str,
) -> Iterator[Any]:
    return run_pipeline(context, build_stream_pipeline(context, stream_id))


def build_stream_pipeline(
    context: PipelineContext,
    stream_id: str,
) -> Pipeline:
    stream = require_runtime_stream(context.runtime, stream_id)
    if isinstance(stream, IngestRuntimeStream):
        return Pipeline(
            name=f"ingest:{stream_id}",
            nodes=_ingest_nodes(context, stream),
            summary=source_summary(stream.source),
        )
    if isinstance(stream, DerivedRuntimeStream):
        upstream = build_stream_pipeline(context, stream.input_stream)
        input_stream = require_runtime_stream(context.runtime, stream.input_stream)
        upstream_nodes = tuple(
            replace(node, name=f"{upstream.name}/{node.name}")
            for node in upstream.nodes
        )
        return Pipeline(
            name=f"stream:{stream_id}",
            nodes=(
                *upstream_nodes,
                *_derived_nodes(
                    context,
                    stream,
                    input_partition_by=input_stream.partition_by,
                ),
            ),
            summary=upstream.summary,
        )
    if isinstance(stream, AlignedRuntimeStream):
        return Pipeline(
            name=f"stream:{stream_id}",
            nodes=(
                SourceNode(
                    name="align_inputs",
                    open=partial(
                        _align_inputs,
                        context,
                        stream.input_streams,
                        stream.partition_by,
                    ),
                ),
                *_aligned_nodes(context, stream),
            ),
            summary="inputs=" + ",".join(stream.input_streams),
        )
    raise TypeError(f"Unsupported runtime stream: {type(stream).__name__}")


def _ingest_nodes(
    context: PipelineContext,
    stream: IngestRuntimeStream,
) -> tuple[Node, ...]:
    return (
        SourceNode(
            name="open_source",
            open=stream.source.stream,
            progress=source_progress(stream.source),
        ),
        PipelineNode(name="map_records", apply=stream.mapper),
        *build_record_transform_nodes(stream.transforms),
        build_record_order_node(
            stream.partition_by,
            stream.presorted,
            context.runtime.execution.sort_buffer_bytes,
        ),
    )


def _derived_nodes(
    context: PipelineContext,
    stream: DerivedRuntimeStream,
    input_partition_by: tuple[str, ...],
) -> tuple[PipelineNode, ...]:
    nodes: list[PipelineNode] = []
    if stream.mapper is not None:
        nodes.append(PipelineNode(name="map_records", apply=stream.mapper))
    if stream.mapper is not None or stream.partition_by != input_partition_by:
        nodes.append(
            build_record_order_node(
                stream.partition_by,
                stream.presorted,
                context.runtime.execution.sort_buffer_bytes,
            )
        )
    nodes.extend(
        build_stream_transform_nodes(
            context,
            stream.transforms,
            stream.partition_by,
        )
    )
    return tuple(nodes)


def _aligned_nodes(
    context: PipelineContext,
    stream: AlignedRuntimeStream,
) -> tuple[PipelineNode, ...]:
    return (
        PipelineNode(name="combine_records", apply=stream.combine),
        build_record_order_node(
            stream.partition_by,
            stream.presorted,
            context.runtime.execution.sort_buffer_bytes,
        ),
        *build_stream_transform_nodes(
            context,
            stream.transforms,
            stream.partition_by,
        ),
    )


def _align_inputs(
    context: PipelineContext,
    input_streams: tuple[str, ...],
    partition_by: tuple[str, ...],
) -> Iterator[tuple[Any, ...]]:
    inputs = [
        (
            stream_id,
            run_pipeline(
                context,
                build_stream_pipeline(context, stream_id),
                observer=_INTERNAL_INPUT_OBSERVER,
            ),
        )
        for stream_id in input_streams
    ]
    yield from align_streams(inputs, partition_by=partition_by)
