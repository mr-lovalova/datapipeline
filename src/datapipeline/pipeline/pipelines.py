"""Helpers for assembling the different pipeline stages."""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any, Tuple

from datapipeline.config.dataset.feature import BaseRecordConfig, FeatureRecordConfig
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.stages import feature_stage, record_stage, vector_stage


def build_record_pipeline(
    cfg: BaseRecordConfig,
    open_stream: Callable[[str], Iterable[Any]],
) -> Iterator[Any]:
    """Open a configured stream and apply record-level filters/transforms."""

    raw = open_stream(cfg.stream)
    return record_stage(raw, cfg.filters, cfg.transforms)


def build_feature_pipeline(
    cfg: FeatureRecordConfig,
    group_by: GroupBy,
    open_stream: Callable[[str], Iterable[Any]],
) -> Iterator[FeatureRecord]:
    """Build the feature-level stream for a single feature configuration."""

    rec = build_record_pipeline(cfg, open_stream)
    return feature_stage(rec, cfg, group_by)


def build_vector_pipeline(
    configs: Sequence[FeatureRecordConfig],
    group_by: GroupBy,
    open_stream: Callable[[str], Iterable[Any]],
) -> Iterator[Tuple[Any, Vector]]:
    """Merge feature streams and yield grouped vectors ready for export."""

    streams = [build_feature_pipeline(c, group_by, open_stream) for c in configs]
    merged = heapq.merge(*streams, key=lambda fr: fr.group_key)
    return vector_stage(merged)
