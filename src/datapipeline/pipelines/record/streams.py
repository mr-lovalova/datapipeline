from collections.abc import Generator
from typing import Any

from datapipeline.dag.context import PipelineContext


def open_record_stream(
    context: PipelineContext,
    stream_id: str,
) -> Generator[Any, None, None]:
    pipeline = context.runtime.registries.stream_specs.get(stream_id).pipeline
    if pipeline == "ingest":
        from datapipeline.pipelines.ingest import build_ingest_pipeline

        return build_ingest_pipeline(context, stream_id)

    from datapipeline.pipelines.stream import build_stream_pipeline

    return build_stream_pipeline(context, stream_id)
