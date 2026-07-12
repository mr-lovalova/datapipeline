from datapipeline.config.catalog import IngestConfig, StreamConfig


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
    for stream_id, spec in stream_configs.items():
        if spec.aligns_streams:
            stream_partition_by(ingests, stream_configs, stream_id)


def stream_partition_by(
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
    stream_id: str,
) -> str | list[str] | None:
    if stream_id in ingests:
        return ingests[stream_id].partition_by

    spec = stream_configs[stream_id]
    if not spec.aligns_streams:
        return spec.partition_by

    input_partitions = [
        stream_partition_by(ingests, stream_configs, input_stream)
        for input_stream in spec.input_streams()
    ]
    expected = _partition_fields(input_partitions[0])
    for input_stream, partition_by in zip(
        spec.input_streams()[1:],
        input_partitions[1:],
        strict=True,
    ):
        if _partition_fields(partition_by) != expected:
            raise ValueError(
                f"Aligned stream '{stream_id}' input '{input_stream}' has "
                f"partition_by {partition_by!r}; expected {input_partitions[0]!r}"
            )
    return input_partitions[0]


def _partition_fields(partition_by: str | list[str] | None) -> tuple[str, ...]:
    if partition_by is None:
        return ()
    if isinstance(partition_by, str):
        return (partition_by,)
    return tuple(partition_by)


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
