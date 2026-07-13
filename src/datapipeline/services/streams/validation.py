from datapipeline.config.catalog import (
    AlignedStreamConfig,
    IngestConfig,
    SourceConfig,
    StreamConfig,
)
from datapipeline.domain.stream import canonical_record_order


def validate_unique_stream_ids(
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
) -> None:
    duplicates = sorted(set(ingests) & set(stream_configs))
    if duplicates:
        raise ValueError(
            f"Duplicate stream id(s) across ingests and streams: {duplicates}"
        )


def validate_stream_configs(
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
) -> None:
    known_streams = set(ingests) | set(stream_configs)
    for stream_id, spec in stream_configs.items():
        missing = [ref for ref in spec.input_streams() if ref not in known_streams]
        if missing:
            raise ValueError(
                f"Stream '{stream_id}' references unknown stream(s): {missing}"
            )
    _validate_stream_cycles(stream_configs)

    for ingest_id, ingest in ingests.items():
        if ingest.ordered_by is None:
            continue
        canonical_order = canonical_record_order(ingest.partition_by)
        if ingest.ordered_by != canonical_order:
            raise ValueError(
                f"Ingest '{ingest_id}' ordered_by must be "
                f"{list(canonical_order)!r}; got {list(ingest.ordered_by)!r}"
            )

    for stream_id, stream in stream_configs.items():
        partition_by = stream_partition_by(ingests, stream_configs, stream_id)
        if stream.ordered_by is None:
            continue
        canonical_order = canonical_record_order(partition_by)
        if stream.ordered_by != canonical_order:
            raise ValueError(
                f"Stream '{stream_id}' ordered_by must be "
                f"{list(canonical_order)!r}; got {list(stream.ordered_by)!r}"
            )


def validate_ingest_sources(
    sources: dict[str, SourceConfig],
    ingests: dict[str, IngestConfig],
) -> None:
    for ingest_id, spec in ingests.items():
        if spec.from_.source not in sources:
            raise ValueError(
                f"Ingest '{ingest_id}' references unknown source '{spec.from_.source}'"
            )


def stream_partition_by(
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
    stream_id: str,
) -> tuple[str, ...]:
    if stream_id in ingests:
        return ingests[stream_id].partition_by

    spec = stream_configs[stream_id]
    if not isinstance(spec, AlignedStreamConfig):
        if spec.partition_by is not None:
            return spec.partition_by
        return stream_partition_by(ingests, stream_configs, spec.from_.stream)

    input_partitions = [
        stream_partition_by(ingests, stream_configs, input_stream)
        for input_stream in spec.input_streams()
    ]
    expected = input_partitions[0]
    for input_stream, partition_by in zip(
        spec.input_streams()[1:],
        input_partitions[1:],
        strict=True,
    ):
        if partition_by != expected:
            raise ValueError(
                f"Aligned stream '{stream_id}' input '{input_stream}' has "
                f"partition_by {list(partition_by)!r}; expected {list(expected)!r}"
            )
    return input_partitions[0]


def stream_feature_id_by(
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
    stream_id: str,
) -> tuple[str, ...] | None:
    if stream_id in ingests:
        return ingests[stream_id].feature_id_by

    spec = stream_configs[stream_id]
    if isinstance(spec, AlignedStreamConfig) or spec.feature_id_by is not None:
        return spec.feature_id_by
    return stream_feature_id_by(ingests, stream_configs, spec.from_.stream)


def _validate_stream_cycles(stream_configs: dict[str, StreamConfig]) -> None:
    visited: set[str] = set()
    visiting: list[str] = []

    def visit(stream_id: str) -> None:
        if stream_id in visited:
            return
        if stream_id in visiting:
            start = visiting.index(stream_id)
            cycle = [*visiting[start:], stream_id]
            raise ValueError("Stream dependency cycle: " + " -> ".join(cycle))

        visiting.append(stream_id)
        for input_stream in stream_configs[stream_id].input_streams():
            if input_stream in stream_configs:
                visit(input_stream)
        visiting.pop()
        visited.add(stream_id)

    for stream_id in stream_configs:
        visit(stream_id)
