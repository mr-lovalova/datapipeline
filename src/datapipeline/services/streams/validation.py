from datapipeline.config.catalog import StreamConfig
from datapipeline.services.streams.join_plan import build_join_input_plans


def validate_stream_configs(stream_configs: dict[str, StreamConfig]) -> None:
    for stream_id, spec in stream_configs.items():
        if spec.reads_source:
            continue

        refs = spec.input_refs()
        missing = [ref for ref in refs.values() if ref not in stream_configs]
        if missing:
            raise ValueError(
                f"Stream '{stream_id}' references unknown stream(s): {missing}"
            )
        if spec.maps_streams:
            continue

        _validate_joined_stream(stream_id, spec, refs, stream_configs)


def stream_partition_by(
    stream_configs: dict[str, StreamConfig],
    spec: StreamConfig,
):
    if not spec.joins_streams:
        return spec.partition_by
    primary = spec.from_.primary
    if primary is None:
        raise ValueError(f"Joined stream '{spec.id}' requires from.primary")
    primary_ref = spec.input_refs()[primary]
    return stream_configs[primary_ref].partition_by


def _validate_joined_stream(
    stream_id: str,
    spec: StreamConfig,
    refs: dict[str, str],
    stream_configs: dict[str, StreamConfig],
) -> None:
    join = spec.from_
    primary = join.primary
    if primary is None:
        raise ValueError(f"Joined stream '{stream_id}' requires from.primary")
    build_join_input_plans(
        stream_id=stream_id,
        input_refs=refs,
        primary=primary,
        broadcast=set(join.broadcast),
        partition_by={ref: stream_configs[ref].partition_by for ref in refs.values()},
    )
