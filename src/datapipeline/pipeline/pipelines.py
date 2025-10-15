import heapq
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from datapipeline.pipeline.utils.keygen import group_key_for

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.pipeline.stages import (
    open_source_stream,
    build_record_stream,
    apply_record_operations,
    build_feature_stream,
    regularize_feature_stream,
    apply_feature_transforms,
    vector_assemble_stage,
    post_process)
from datapipeline.registries.registries import (
    partition_by as partition_by_reg,
    sort_batch_size as sort_batch_size_reg,
)


def build_feature_pipeline(
    cfg: FeatureRecordConfig,
    stage: int | None = None,
) -> Iterator[Any]:
    record_stream_id = cfg.record_stream

    dtos = open_source_stream(record_stream_id)
    if stage == 0:
        return dtos

    records = build_record_stream(dtos, record_stream_id)
    if stage == 1:
        return records

    records = apply_record_operations(records, record_stream_id)
    if stage == 2:
        return records

    partition_by = partition_by_reg.get(record_stream_id)
    features = build_feature_stream(records, cfg.id, partition_by)
    if stage == 3:
        return features

    batch_size = sort_batch_size_reg.get(record_stream_id)
    regularized = regularize_feature_stream(
        features, record_stream_id, batch_size)
    if stage == 4:
        return regularized

    transformed = apply_feature_transforms(
        regularized, cfg.scale, cfg.sequence)
    if stage == 5:
        return transformed
    return transformed


def build_pipeline(
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    vector_transforms: Sequence[Mapping[str, Any]] | None = None,
    stage: int | None = None,
) -> Iterator[Any]:
    if stage is not None and stage <= 5:
        first = next(iter(configs))
        return build_feature_pipeline(first, stage=stage)

    streams = [build_feature_pipeline(cfg, stage=None) for cfg in configs]

    merged = heapq.merge(
        *streams, key=lambda fr: group_key_for(fr, group_by_cadence)
    )
    vectors = vector_assemble_stage(merged, group_by_cadence)
    if stage == 6:
        return vectors
    cleaned = post_process(vectors, vector_transforms)
    if stage == 7 or stage is None:
        return cleaned
    raise ValueError("unknown stage")
