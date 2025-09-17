
"""Composable pipeline stages used by the runtime."""

from __future__ import annotations

from collections import defaultdict
from itertools import groupby
from typing import Any, Iterable, Iterator, Optional, Sequence, Tuple

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.vector import Vector, vectorize_record_group
from datapipeline.pipeline.utils.memory_sort import memory_sorted
from datapipeline.pipeline.utils.ordering import canonical_key
from datapipeline.pipeline.utils.transform_utils import (
    filter_record_stream,
    record_to_feature,
    transform_feature_stream,
    transform_record_stream,
)


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
) -> Iterator[FeatureRecord]:
    """Wrap filtered records as FeatureRecord objects.
    Assign partition-aware feature_ids and normalized group_keys before transforms.
    Sort feature streams, apply feature/sequence transforms, and emit canonical order."""

    stream = record_to_feature(record_stream, cfg, group_by)
    stream = memory_sorted(
        stream,
        batch_size=100000,
        key=lambda fr: (fr.feature_id, getattr(fr.record, "time", 0)),
    )
    stream = transform_feature_stream(stream, cfg)
    return memory_sorted(stream, batch_size=100000, key=canonical_key)


def vector_stage(merged: Iterator[FeatureRecord]) -> Iterator[Tuple[Any, Vector]]:
    """Group the merged feature stream by group_key.
    Coalesce each partitioned feature_id into record buckets.
    Yield (group_key, Vector) pairs ready for downstream consumption."""

    for group_key, group in groupby(merged, key=lambda fr: fr.group_key):
        feature_map = defaultdict(list)
        for fr in group:
            records = fr.record if isinstance(fr.record, list) else [fr.record]
            feature_map[fr.feature_id].extend(records)
        yield group_key, vectorize_record_group(feature_map)
