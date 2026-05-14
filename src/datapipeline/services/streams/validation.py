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
        refs = spec.input_refs()
        missing = [ref for ref in refs.values() if ref not in known_streams]
        if missing:
            raise ValueError(
                f"Stream '{stream_id}' references unknown stream(s): {missing}"
            )


def stream_partition_by(
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
    spec: StreamConfig,
):
    if not spec.joins_streams:
        return spec.partition_by
    primary = spec.from_.primary
    if primary is None:
        raise ValueError(f"Joined stream '{spec.id}' requires from.primary")
    primary_ref = spec.input_refs()[primary]
    if primary_ref in stream_configs:
        return stream_configs[primary_ref].partition_by
    return ingests[primary_ref].partition_by
