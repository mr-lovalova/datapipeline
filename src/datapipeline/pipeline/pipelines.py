import heapq
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Tuple

from datapipeline.pipeline.utils.memory_sort import memory_sorted
from datapipeline.pipeline.utils.ordering import canonical_key

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.vector import Vector
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
    group_by: str,
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
    features = build_feature_stream(records, cfg.id, group_by, partition_by)
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

    sorted = memory_sorted(
        transformed, batch_size=batch_size, key=canonical_key)
    if stage == 6:  # final sort needed for downstream consumers:
        return sorted
    return sorted


def build_pipeline(
    configs: Sequence[FeatureRecordConfig],
    group_by: str,
    vector_transforms: Sequence[Mapping[str, Any]] | None = None,
    stage: int | None = None,
) -> Iterator[Any]:
    if stage is not None and stage <= 6:
        first = next(iter(configs))
        return build_feature_pipeline(first, group_by, stage=stage)

    streams = [build_feature_pipeline(
        cfg, group_by, stage=None) for cfg in configs]
    merged = heapq.merge(*streams, key=lambda fr: fr.group_key)
    vectors = vector_assemble_stage(merged)
    if stage == 7:
        return vectors
    cleaned = post_process(vectors, vector_transforms)
    if stage == 8 or stage is None:
        return cleaned
    raise ValueError("unknown stage")
