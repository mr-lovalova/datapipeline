from __future__ import annotations

from collections import defaultdict
from itertools import groupby
from typing import Any, Iterable, Iterator, Optional, Sequence, Tuple, Mapping

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.vector import Vector, vectorize_record_group
from datapipeline.pipeline.utils.memory_sort import memory_sorted
from datapipeline.pipeline.utils.ordering import canonical_key
from datapipeline.pipeline.utils.transform_utils import (
    filter_record_stream,
    record_to_feature,
    transform_record_stream,
)
from datapipeline.pipeline.utils.transform_utils import instantiate_transforms
from datapipeline.plugins import TRANSFORMS_EP, VECTOR_TRANSFORMS_EP


def record_stage(
    raw_stream: Iterable[Any],
    filters: Optional[Sequence[dict[str, Any]]] = None,
    transforms: Optional[Sequence[dict[str, Any]]] = None,
) -> Iterator[Any]:
    """Apply configured filters and transforms to the raw record stream."""

    stream = filter_record_stream(iter(raw_stream), filters)
    return transform_record_stream(stream, transforms)


def feature_stage(
    record_stream: Iterable[Any],
    cfg: FeatureRecordConfig,
    group_by: GroupBy,
) -> Iterator[FeatureRecord | FeatureSequence]:
    """Wrap filtered records as FeatureRecord objects.
    Assign partition-aware feature_ids and normalized group_keys before transforms.
    Sort feature streams, apply feature/sequence transforms, and emit canonical order."""

    stream = record_to_feature(record_stream, cfg, group_by)
    batch_size = getattr(cfg, "sort_batch_size", None) or 100_000
    # Initial sort by (feature_id, time) within batches to prepare for sequence transforms
    stream = memory_sorted(
        stream,
        batch_size=batch_size,
        key=lambda fr: (fr.feature_id, fr.record.time),
    )

    # Apply feature transforms first, then sequence (windowing) transforms
    transform_specs = (
        ("fill", "fill", lambda v: dict(v)),
        ("scale", "scale", lambda v: {} if isinstance(v, bool) else dict(v)),
    )
    feature_tf = [
        {ep: coerce(getattr(cfg, field))}
        for field, ep, coerce in transform_specs
        if getattr(cfg, field, None)
    ]
    # Append any user-declared feature-stage custom transforms
    if getattr(cfg, "transforms", None):
        feature_tf.extend(cfg.transforms or [])
    seq_tf = (
        [{"sequence": dict(cfg.sequence)}]
        if isinstance(getattr(cfg, "sequence", None), dict)
        else []
    )
    combined_tf = feature_tf + seq_tf
    for transform in instantiate_transforms(TRANSFORMS_EP, combined_tf):
        stream = transform.apply(stream)
    return memory_sorted(stream, batch_size=batch_size, key=canonical_key)


def vector_stage(merged: Iterator[FeatureRecord | FeatureSequence]) -> Iterator[Tuple[Any, Vector]]:
    """Group the merged feature stream by group_key.
    Coalesce each partitioned feature_id into record buckets.
    Yield (group_key, Vector) pairs ready for downstream consumption."""

    for group_key, group in groupby(merged, key=lambda fr: fr.group_key):
        feature_map = defaultdict(list)
        for fr in group:
            if isinstance(fr, FeatureSequence):
                records = fr.records
            else:
                records = [fr.record]
            feature_map[fr.feature_id].extend(records)
        yield group_key, vectorize_record_group(feature_map)


def vector_cleaning_stage(
    stream: Iterator[Tuple[Any, Vector]],
    clauses: Optional[Sequence[Mapping[str, Any]]],
) -> Iterator[Tuple[Any, Vector]]:
    """Apply configured vector transforms to the merged feature stream."""

    transforms = instantiate_transforms(VECTOR_TRANSFORMS_EP, clauses)

    for transform in transforms:
        stream = transform.apply(stream)
    return stream
