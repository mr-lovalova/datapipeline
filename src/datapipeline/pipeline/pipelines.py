import heapq
from collections.abc import Iterator, Sequence
from typing import Any
from itertools import tee

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.utils.keygen import group_key_for
from datapipeline.pipeline.utils.memory_sort import batch_sort
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.pipeline.stages import (
    open_source_stream,
    build_record_stream,
    apply_record_operations,
    build_feature_stream,
    regularize_feature_stream,
    apply_feature_transforms,
    vector_assemble_stage,
    sample_assemble_stage,
    align_stream,
    window_keys,
)
from datapipeline.pipeline.context import PipelineContext


def build_feature_pipeline(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    stage: int | None = None,
) -> Iterator[Any]:
    runtime = context.runtime
    record_stream_id = cfg.record_stream

    dtos = open_source_stream(context, record_stream_id)
    if stage == 0:
        return dtos

    records = build_record_stream(context, dtos, record_stream_id)
    if stage == 1:
        return records

    records = apply_record_operations(context, records, record_stream_id)
    if stage == 2:
        return records

    partition_by = runtime.registries.partition_by.get(record_stream_id)
    features = build_feature_stream(records, cfg.id, partition_by)
    if stage == 3:
        return features

    batch_size = runtime.registries.sort_batch_size.get(record_stream_id)
    regularized = regularize_feature_stream(
        context, features, record_stream_id, batch_size)
    if stage == 4:
        return regularized

    transformed = apply_feature_transforms(
        context, regularized, cfg.scale, cfg.sequence)
    if stage == 5:
        return transformed

    def _time_then_id(item: Any):
        rec = getattr(item, "record", None)
        if rec is not None:
            t = getattr(rec, "time", None)
        else:
            recs = getattr(item, "records", None)
            t = getattr(recs[0], "time", None) if recs else None
        return (t, getattr(item, "id", None))

    sorted_for_grouping = batch_sort(
        transformed, batch_size=batch_size, key=_time_then_id
    )
    return sorted_for_grouping


def build_vector_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    *,
    rectangular: bool = True,
) -> Iterator[Any]:
    """Build the vector assembly pipeline for features and optionally attach targets."""
    feature_cfgs = list(configs)
    target_cfgs = list(target_configs or [])
    if not feature_cfgs and not target_cfgs:
        return iter(())

    if rectangular:
        start, end = context.window_bounds(rectangular_required=True)
        keys = window_keys(start, end, group_by_cadence)
    else:
        keys = None

    feature_vectors = _assemble_vectors(
        context,
        feature_cfgs,
        group_by_cadence,
    )
    if keys is not None:
        # share keys across feature/target alignment
        if target_cfgs:
            keys_feature, keys_target = tee(keys, 2)
        else:
            keys_feature = keys
            keys_target = None
        feature_vectors = align_stream(feature_vectors, keys=keys_feature)
    else:
        keys_target = None

    if not target_cfgs:
        return sample_assemble_stage(feature_vectors)

    target_vectors = _assemble_vectors(
        context,
        target_cfgs,
        group_by_cadence,
    )
    if keys is not None:
        target_vectors = align_stream(target_vectors, keys=keys_target)
    return sample_assemble_stage(feature_vectors, target_vectors)


def _assemble_vectors(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
) -> Iterator[tuple[tuple, Vector]]:
    if not configs:
        return iter(())
    streams = [
        build_feature_pipeline(
            context,
            cfg,
        )
        for cfg in configs
    ]
    merged = heapq.merge(
        *streams, key=lambda fr: group_key_for(fr, group_by_cadence)
    )
    return vector_assemble_stage(merged, group_by_cadence)
