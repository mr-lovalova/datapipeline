from datapipeline.config.catalog import (
    AlignedStreamConfig,
    IngestConfig,
    StreamConfig,
)
from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    IngestRuntimeStream,
    Runtime,
    RuntimeStream,
)
from datapipeline.services.definitions import PipelineDefinition
from datapipeline.services.streams.aligned import build_combine_stage
from datapipeline.services.streams.ingest import (
    build_mapper_from_spec,
    build_source_from_spec,
)
from datapipeline.services.streams.validation import stream_partition_by
from datapipeline.sources.models.source import Source


def _compile_ingest(
    spec: IngestConfig,
    sources: dict[str, Source],
) -> IngestRuntimeStream:
    return IngestRuntimeStream(
        source=sources[spec.from_.source],
        mapper=build_mapper_from_spec(spec.map),
        transforms=tuple(spec.record),
        partition_by=spec.partition_by,
        presorted=spec.ordered_by is not None,
    )


def _compile_stream(
    spec: StreamConfig,
    ingests: dict[str, IngestConfig],
    streams: dict[str, StreamConfig],
) -> DerivedRuntimeStream | AlignedRuntimeStream:
    partition_by = stream_partition_by(ingests, streams, spec.id)
    if isinstance(spec, AlignedStreamConfig):
        return AlignedRuntimeStream(
            input_streams=spec.input_streams(),
            combine=build_combine_stage(spec),
            transforms=tuple(spec.stream),
            partition_by=partition_by,
            presorted=spec.ordered_by is not None,
        )
    return DerivedRuntimeStream(
        input_stream=spec.from_.stream,
        mapper=build_mapper_from_spec(spec.map),
        transforms=tuple(spec.stream),
        partition_by=partition_by,
        presorted=spec.ordered_by is not None,
    )


def compile_runtime(definition: PipelineDefinition) -> Runtime:
    streams = definition.streams
    sources = {
        source_id: build_source_from_spec(
            source.model_copy(deep=True),
            project_yaml=definition.project.path,
        )
        for source_id, source in streams.sources.items()
    }
    runtime_streams: dict[str, RuntimeStream] = {
        stream_id: _compile_ingest(ingest.model_copy(deep=True), sources)
        for stream_id, ingest in streams.ingests.items()
    }
    runtime_streams.update(
        {
            stream_id: _compile_stream(
                stream.model_copy(deep=True),
                streams.ingests,
                streams.streams,
            )
            for stream_id, stream in streams.streams.items()
        }
    )
    return Runtime(
        project_yaml=definition.project.path,
        artifacts_root=definition.project.artifacts_root,
        dataset=definition.dataset.model_copy(deep=True),
        streams=runtime_streams,
    )
