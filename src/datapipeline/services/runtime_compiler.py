from pathlib import Path

from datapipeline.config.catalog import (
    AlignedStreamConfig,
    IngestConfig,
    SourceConfig,
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


def _compile_ingest(
    spec: IngestConfig,
    source: SourceConfig,
    project_yaml: Path,
) -> IngestRuntimeStream:
    return IngestRuntimeStream(
        source=build_source_from_spec(
            source,
            project_yaml=project_yaml,
        ),
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
        mapper=(build_mapper_from_spec(spec.map) if spec.map is not None else None),
        transforms=tuple(spec.stream),
        partition_by=partition_by,
        presorted=spec.ordered_by is not None,
    )


def compile_runtime(definition: PipelineDefinition) -> Runtime:
    streams = definition.streams
    runtime_streams: dict[str, RuntimeStream] = {
        stream_id: _compile_ingest(
            ingest.model_copy(deep=True),
            streams.sources[ingest.from_.source],
            definition.project.path,
        )
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
