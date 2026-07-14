from collections.abc import Iterator
from contextlib import ExitStack
from dataclasses import replace
from functools import partial
from typing import Any

from datapipeline.alignment.engine import align_streams
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import SourceNode
from datapipeline.execution.observer import NoopPipelineObserver
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.ingest.pipeline import build_ingest_pipeline
from datapipeline.pipelines.stream.nodes import build_stream_nodes
from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    IngestRuntimeStream,
    require_runtime_stream,
)


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
        return build_ingest_pipeline(context, stream_id)
    if isinstance(stream, DerivedRuntimeStream):
        upstream = build_stream_pipeline(context, stream.input_stream)
        upstream_nodes = tuple(
            replace(node, name=f"{upstream.name}/{node.name}")
            for node in upstream.nodes
        )
        return Pipeline(
            name=f"stream:{stream_id}",
            nodes=(*upstream_nodes, *build_stream_nodes(context, stream_id)),
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
                *build_stream_nodes(context, stream_id),
            ),
            summary="inputs=" + ",".join(stream.input_streams),
        )
    raise TypeError(f"Unsupported runtime stream: {type(stream).__name__}")


def _align_inputs(
    context: PipelineContext,
    input_streams: tuple[str, ...],
    partition_by: tuple[str, ...],
) -> Iterator[tuple[Any, ...]]:
    with ExitStack() as opened:
        inputs = []
        for stream_id in input_streams:
            records = run_pipeline(
                context,
                build_stream_pipeline(context, stream_id),
                observer=_INTERNAL_INPUT_OBSERVER,
            )
            opened.callback(records.close)
            inputs.append((stream_id, records))
        aligned = align_streams(
            inputs,
            partition_by=partition_by,
        )
        opened.callback(aligned.close)
        yield from aligned
