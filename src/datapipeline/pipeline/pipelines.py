import heapq
from collections.abc import Iterator, Sequence
from typing import Any

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
    stage: int | None = None,
) -> Iterator[Any]:
    """Build the vector assembly pipeline.
    Stages:
      - 0..5: delegates to feature pipeline for the first configured feature
      - 6: assembled vectors
    """
    if stage is not None and stage <= 5:
        first = next(iter(configs))
        return build_feature_pipeline(context, first, stage=stage)

    streams = [build_feature_pipeline(context, cfg, stage=None) for cfg in configs]
    merged = heapq.merge(
        *streams, key=lambda fr: group_key_for(fr, group_by_cadence)
    )
    vectors = vector_assemble_stage(merged, group_by_cadence)
    return vectors
