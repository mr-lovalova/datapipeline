from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any, Tuple

from datapipeline.config.dataset.feature import BaseRecordConfig, FeatureRecordConfig
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.stages import feature_stage, record_stage, vector_stage, vector_cleaning_stage


def build_record_pipeline(
    cfg: BaseRecordConfig,
    open_stream: Callable[[str], Iterable[Any]],
) -> Iterator[Any]:
    """Open a configured stream and apply record-level filters/transforms."""

    raw = open_stream(cfg.stream)
    # Derive record-stage clauses from generic list
    # Build record-stage filters/transforms directly from simple fields
    filters: list[dict[str, Any]] = []
    flt = getattr(cfg, "filter", None)
    if isinstance(flt, dict):
        op = flt.get("operator") or flt.get("op")
        field = flt.get("field")
        if op and field is not None:
            val = flt.get("value") if "value" in flt else flt.get("values")
            filters.append({str(op): {str(field): val}})
    # Optional multiple filters
    flts = getattr(cfg, "filters", None)
    if isinstance(flts, list):
        for f in flts:
            if not isinstance(f, dict):
                continue
            op = f.get("operator") or f.get("op")
            field = f.get("field")
            if op and field is not None:
                val = f.get("value") if "value" in f else f.get("values")
                filters.append({str(op): {str(field): val}})

    transforms: list[dict[str, Any]] = []
    lag = getattr(cfg, "lag", None)
    if lag is not None:
        transforms.append({"lag": lag})
    # Append any user-declared record-stage custom transforms
    if getattr(cfg, "record_transforms", None):
        transforms.extend(cfg.record_transforms or [])
    return record_stage(raw, filters, transforms)


def build_feature_pipeline(
    cfg: FeatureRecordConfig,
    group_by: GroupBy,
    open_stream: Callable[[str], Iterable[Any]],
) -> Iterator[FeatureRecord | FeatureSequence]:
    """Build the feature-level stream for a single feature configuration."""

    rec = build_record_pipeline(cfg, open_stream)
    return feature_stage(rec, cfg, group_by)


def build_vector_pipeline(
    configs: Sequence[FeatureRecordConfig],
    group_by: GroupBy,
    open_stream: Callable[[str], Iterable[Any]],
    vector_transforms: Sequence[Mapping[str, Any]] | None = None,
) -> Iterator[Tuple[Any, Vector]]:
    """Merge feature streams and yield grouped vectors ready for export."""

    streams = [build_feature_pipeline(
        c, group_by, open_stream) for c in configs]
    merged = heapq.merge(*streams, key=lambda fr: fr.group_key)
    stream = vector_stage(merged)
    return vector_cleaning_stage(stream, vector_transforms)
