from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.metadata import (
    VectorMetadata,
    Window,
    FEATURE_VECTORS_COUNT_KEY,
    TARGET_VECTORS_COUNT_KEY,
)
from datapipeline.config.tasks import MetadataTask
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent
from datapipeline.config.dataset.normalize import floor_time_to_bucket
from datapipeline.utils.time import parse_timecode

from .utils import collect_schema_entries, metadata_entries_from_stats


def _entry_window(entry: dict) -> tuple[datetime | None, datetime | None]:
    start = entry.get("first_ts")
    end = entry.get("last_ts")
    return (start if isinstance(start, datetime) else None, end if isinstance(end, datetime) else None)


def _group_ranges(entries: list[dict], key_name: str) -> list[tuple[datetime, datetime]]:
    grouped: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    for entry in entries:
        start, end = _entry_window(entry)
        if start is None or end is None:
            continue
        group_key = entry.get(key_name) or entry.get("id")
        if not isinstance(group_key, str):
            continue
        grouped[group_key].append((start, end))
    ranges: list[tuple[datetime, datetime]] = []
    for values in grouped.values():
        group_start = min(start for start, _ in values)
        group_end = max(end for _, end in values)
        ranges.append((group_start, group_end))
    return ranges


def _range_union(ranges):
    if not ranges:
        return None, None
    start = min(r[0] for r in ranges)
    end = max(r[1] for r in ranges)
    if start >= end:
        return None, None
    return start, end


def _range_intersection(ranges):
    if not ranges:
        return None, None
    start = max(r[0] for r in ranges)
    end = min(r[1] for r in ranges)
    if start >= end:
        return None, None
    return start, end


def _window_bounds_from_stats(
    feature_stats: list[dict],
    target_stats: list[dict],
    *,
    mode: str,
) -> tuple[datetime | None, datetime | None]:
    base_ranges = _group_ranges(
        feature_stats, "base_id") + _group_ranges(target_stats, "base_id")
    partition_ranges = _group_ranges(
        feature_stats, "id") + _group_ranges(target_stats, "id")

    if mode == "intersection":
        return _range_intersection(base_ranges)
    if mode == "strict":
        return _range_intersection(partition_ranges)
    if mode == "relaxed":
        return _range_union(partition_ranges)
    # default to union
    return _range_union(base_ranges if base_ranges else partition_ranges)


def _window_size(start: datetime | None, end: datetime | None, cadence: str | None) -> int | None:
    if start is None or end is None or cadence is None:
        return None
    try:
        anchored_start = floor_time_to_bucket(start, cadence)
        anchored_end = floor_time_to_bucket(end, cadence)
        step = parse_timecode(cadence)
        if anchored_end < anchored_start:
            return None
        return int(((anchored_end - anchored_start) / step)) + 1
    except Exception:
        return None


def materialize_metadata(runtime: Runtime, task_cfg: MetadataTask) -> Tuple[str, Dict[str, object]] | None:
    if not task_cfg.enabled:
        return None
    dataset = load_dataset(runtime.project_yaml, "vectors")
    features_cfgs = list(dataset.features or [])
    feature_stats, feature_vectors, feature_min, feature_max = collect_schema_entries(
        runtime,
        features_cfgs,
        dataset.group_by,
        cadence_strategy=task_cfg.cadence_strategy,
        collect_metadata=True,
    )
    target_meta: list[dict] = []
    target_vectors = 0
    target_cfgs = list(dataset.targets or [])
    target_stats: list[dict] = []
    target_min = target_max = None
    if target_cfgs:
        target_stats, target_vectors, target_min, target_max = collect_schema_entries(
            runtime,
            target_cfgs,
            dataset.group_by,
            cadence_strategy=task_cfg.cadence_strategy,
            collect_metadata=True,
        )
        target_meta = metadata_entries_from_stats(
            target_stats, task_cfg.cadence_strategy)
    feature_meta = metadata_entries_from_stats(
        feature_stats, task_cfg.cadence_strategy)

    generated_at = datetime.now(timezone.utc)
    window_obj: Window | None = None
    computed_start, computed_end = _window_bounds_from_stats(
        feature_stats,
        target_stats if target_cfgs else [],
        mode=task_cfg.window_mode,
    )
    start = computed_start
    end = computed_end
    if start is not None and end is not None and start < end:
        size = _window_size(start, end, dataset.group_by)
        window_obj = Window(start=start, end=end,
                            mode=task_cfg.window_mode, size=size)

    doc = VectorMetadata(
        schema_version=1,
        generated_at=generated_at,
        features=feature_meta,
        targets=target_meta,
        counts={
            FEATURE_VECTORS_COUNT_KEY: feature_vectors,
            TARGET_VECTORS_COUNT_KEY: target_vectors,
        },
        window=window_obj,
    )

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)
    with destination.open("w", encoding="utf-8") as fh:
        json.dump(doc.model_dump(mode="json"), fh, indent=2)

    meta: Dict[str, object] = {
        "features": len(feature_meta),
        "targets": len(target_meta),
    }
    return str(relative_path), meta
