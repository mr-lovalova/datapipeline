from collections.abc import Generator, Iterable, Iterator
from dataclasses import replace
from datetime import datetime
from functools import partial
from typing import Any

from datapipeline.alignment.broadcast import broadcast_stream
from datapipeline.alignment.engine import align_streams
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.observer import ignore_pipeline_event
from datapipeline.execution.pipeline import Input, Pipeline, Stage
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.stream.order import build_record_order_stage
from datapipeline.pipelines.stream.stages import (
    build_preprocess_stages,
    build_transform_stages,
)
from datapipeline.runtime import (
    AlignedRuntimeStream,
    BroadcastRuntimeStream,
    DerivedRuntimeStream,
    RecordStage,
    SourceRuntimeStream,
    require_runtime_stream,
)
from datapipeline.sources.observability import source_progress, source_summary


def run_stream_pipeline(
    context: PipelineContext,
    stream_id: str,
) -> Generator[Any, None, None]:
    return run_pipeline(context, build_stream_pipeline(context, stream_id))


def build_stream_pipeline(
    context: PipelineContext,
    stream_id: str,
) -> Pipeline:
    stream = require_runtime_stream(context.runtime, stream_id)
    if isinstance(stream, SourceRuntimeStream):
        return Pipeline(
            name=f"stream:{stream_id}",
            input=Input(
                name="open_source",
                open=stream.source.stream,
                progress=source_progress(stream.source),
            ),
            stages=_source_stages(context, stream),
            summary=source_summary(stream.source),
        )
    if isinstance(stream, DerivedRuntimeStream):
        upstream = build_stream_pipeline(context, stream.input_stream)
        return Pipeline(
            name=f"stream:{stream_id}",
            input=replace(
                upstream.input,
                name=f"{upstream.name}/{upstream.input.name}",
            ),
            stages=(
                *(
                    replace(stage, name=f"{upstream.name}/{stage.name}")
                    for stage in upstream.stages
                ),
                *build_transform_stages(
                    context,
                    stream.transforms,
                    stream.partition_by,
                ),
            ),
            summary=upstream.summary,
        )
    if isinstance(stream, BroadcastRuntimeStream):
        return Pipeline(
            name=f"stream:{stream_id}",
            input=Input(
                name="broadcast_inputs",
                open=partial(
                    _broadcast_inputs,
                    context,
                    stream.input_stream,
                    stream.broadcast_stream,
                    stream.partition_by,
                ),
            ),
            stages=(
                Stage(name="combine_records", apply=stream.combine),
                *build_transform_stages(
                    context,
                    stream.transforms,
                    stream.partition_by,
                ),
            ),
            summary=(
                f"primary={stream.input_stream},broadcast={stream.broadcast_stream}"
            ),
        )
    if isinstance(stream, AlignedRuntimeStream):
        return Pipeline(
            name=f"stream:{stream_id}",
            input=Input(
                name="align_inputs",
                open=partial(
                    _align_inputs,
                    context,
                    stream.inputs,
                    stream.partition_by,
                ),
            ),
            stages=(
                Stage(name="combine_records", apply=stream.combine),
                *build_transform_stages(
                    context,
                    stream.transforms,
                    stream.partition_by,
                ),
            ),
            summary="inputs=" + ",".join(stream.inputs),
        )
    raise TypeError(f"Unsupported runtime stream: {type(stream).__name__}")


def _source_stages(
    context: PipelineContext,
    stream: SourceRuntimeStream,
) -> tuple[Stage, ...]:
    return (
        Stage(
            name="map_records",
            apply=partial(_map_records, stream.mapper),
        ),
        *build_preprocess_stages(stream.preprocess),
        build_record_order_stage(
            stream.partition_by,
            stream.presorted,
            context.runtime.execution.sort_buffer_bytes,
        ),
        *build_transform_stages(
            context,
            stream.transforms,
            stream.partition_by,
        ),
    )


def _map_records(mapper: RecordStage, records: Iterator[Any]) -> Iterator[Any]:
    mapped: Iterable[Any] = ()
    iterator: Iterator[Any] = iter(())
    processing_failed = False
    try:
        mapped = mapper(records)
        if mapped is None:
            raise TypeError("Mapper returned None; return an iterable.")
        iterator = iter(mapped)
        for position, record in enumerate(iterator, start=1):
            record_time = getattr(record, "time", None)
            if not isinstance(record_time, datetime):
                raise TypeError(
                    f"Mapped record {position} time must be a datetime; "
                    f"got {type(record_time).__name__}."
                )
            if record_time.tzinfo is None or record_time.utcoffset() is None:
                raise ValueError(
                    f"Mapped record {position} time must be timezone-aware."
                )
            yield record
    except GeneratorExit:
        raise
    except BaseException:
        processing_failed = True
        raise
    finally:
        try:
            if iterator is not records:
                closer = getattr(iterator, "close", None)
                if callable(closer):
                    closer()
            if iterator is not mapped and mapped is not records:
                closer = getattr(mapped, "close", None)
                if callable(closer):
                    closer()
        except BaseException:
            if not processing_failed:
                raise


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
                observer=ignore_pipeline_event,
            ),
        )
        for stream_id in input_streams
    ]
    yield from align_streams(inputs, partition_by=partition_by)


def _broadcast_inputs(
    context: PipelineContext,
    input_stream: str,
    broadcast_input: str,
    partition_by: tuple[str, ...],
) -> Iterator[tuple[Any, Any]]:
    input_pipeline = build_stream_pipeline(context, input_stream)
    broadcast_pipeline = build_stream_pipeline(context, broadcast_input)
    primary = run_pipeline(
        context,
        input_pipeline,
        observer=ignore_pipeline_event,
    )
    broadcast = run_pipeline(
        context,
        broadcast_pipeline,
        observer=ignore_pipeline_event,
    )
    yield from broadcast_stream(primary, broadcast, partition_by)
