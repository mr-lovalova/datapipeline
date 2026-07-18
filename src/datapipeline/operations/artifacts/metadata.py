from collections import defaultdict
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from datapipeline.artifacts.models import (
    SampleDomainEntry,
    SampleMetadata,
    VectorMetadata,
    VectorMetadataCounts,
    VectorMetadataEntry,
    VECTOR_METADATA_VERSION,
    Window,
    WindowMode,
)
from datapipeline.config.tasks import MetadataTask
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.runtime import Runtime
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.utils.time import count_cadence_buckets, parse_cadence

from .utils import (
    collect_vector_metadata,
    metadata_entries_from_stats,
    VectorMetadataStats,
)


ObservedRange = tuple[datetime, datetime]


def _collapsed_ranges(
    grouped: dict[str, list[ObservedRange]],
) -> list[ObservedRange]:
    return [
        (
            min(start for start, _ in values),
            max(end for _, end in values),
        )
        for values in grouped.values()
    ]


def _base_ranges(entries: Sequence[VectorMetadataStats]) -> list[ObservedRange]:
    grouped: dict[str, list[ObservedRange]] = defaultdict(list)
    for entry in entries:
        start = entry.first_observed
        end = entry.last_observed
        if start is None or end is None:
            continue
        grouped[entry.base_id].append((start, end))
    return _collapsed_ranges(grouped)


def _partition_ranges(
    entries: Sequence[VectorMetadataStats],
) -> list[ObservedRange]:
    grouped: dict[str, list[ObservedRange]] = defaultdict(list)
    for entry in entries:
        start = entry.first_observed
        end = entry.last_observed
        if start is None or end is None:
            continue
        grouped[entry.id].append((start, end))
    return _collapsed_ranges(grouped)


def _range_union(
    ranges: Sequence[ObservedRange],
) -> tuple[datetime | None, datetime | None]:
    if not ranges:
        return None, None
    start = min(r[0] for r in ranges)
    end = max(r[1] for r in ranges)
    return start, end


def _range_intersection(
    ranges: Sequence[ObservedRange],
) -> tuple[datetime | None, datetime | None]:
    if not ranges:
        return None, None
    start = max(r[0] for r in ranges)
    end = min(r[1] for r in ranges)
    if start > end:
        return None, None
    return start, end


def _window_bounds_from_stats(
    feature_stats: Sequence[VectorMetadataStats],
    target_stats: Sequence[VectorMetadataStats],
    mode: WindowMode,
) -> tuple[datetime | None, datetime | None]:
    base_ranges = _base_ranges(feature_stats) + _base_ranges(target_stats)
    partition_ranges = _partition_ranges(feature_stats) + _partition_ranges(
        target_stats
    )

    if mode == "union":
        return _range_union(base_ranges if base_ranges else partition_ranges)
    if mode == "intersection":
        return _range_intersection(base_ranges)
    if mode == "strict":
        return _range_intersection(partition_ranges)
    if mode == "relaxed":
        return _range_union(partition_ranges)
    raise ValueError(f"Unsupported metadata window mode {mode!r}.")


def _merge_sample_domains(
    feature_domain: dict[tuple, tuple[datetime, datetime]],
    target_domain: dict[tuple, tuple[datetime, datetime]],
    mode: WindowMode,
) -> dict[tuple, tuple[datetime, datetime]]:
    if mode not in {"union", "intersection", "strict", "relaxed"}:
        raise ValueError(f"Unsupported metadata window mode {mode!r}.")
    if not target_domain:
        return dict(feature_domain)
    if mode in {"intersection", "strict"}:
        merged: dict[tuple, tuple[datetime, datetime]] = {}
        for key, feature_range in feature_domain.items():
            target_range = target_domain.get(key)
            if target_range is None:
                continue
            start = max(feature_range[0], target_range[0])
            end = min(feature_range[1], target_range[1])
            if start <= end:
                merged[key] = (start, end)
        return merged

    merged = dict(feature_domain)
    for key, target_range in target_domain.items():
        existing_range = merged.get(key)
        if existing_range is None:
            merged[key] = target_range
            continue
        merged[key] = (
            min(existing_range[0], target_range[0]),
            max(existing_range[1], target_range[1]),
        )
    return merged


def _sample_metadata(
    cadence: str,
    sample_keys: list[str],
    domain: dict[tuple, tuple[datetime, datetime]],
) -> SampleMetadata:
    return SampleMetadata(
        cadence=cadence,
        keys=sample_keys,
        domain=[
            SampleDomainEntry(key=list(key), start=start, end=end)
            for key, (start, end) in sorted(domain.items())
        ],
    )


def materialize_metadata(
    runtime: Runtime,
    task_cfg: MetadataTask,
) -> ArtifactOutput:
    dataset = runtime.dataset
    features_cfgs = list(dataset.features)
    (
        feature_stats,
        feature_vectors,
        feature_domain,
    ) = collect_vector_metadata(
        runtime,
        features_cfgs,
        dataset.sample.cadence,
        dataset.sample.keys,
        "scan_features",
    )
    target_meta: tuple[VectorMetadataEntry, ...] = ()
    target_vectors = 0
    target_cfgs = list(dataset.targets)
    target_stats: list[VectorMetadataStats] = []
    target_domain: dict[tuple, tuple[datetime, datetime]] = {}
    if target_cfgs:
        (
            target_stats,
            target_vectors,
            target_domain,
        ) = collect_vector_metadata(
            runtime,
            target_cfgs,
            dataset.sample.cadence,
            dataset.sample.keys,
            "scan_targets",
        )
        target_meta = metadata_entries_from_stats(target_stats)
    feature_meta = metadata_entries_from_stats(feature_stats)

    window_obj: Window | None = None
    computed_start, computed_end = _window_bounds_from_stats(
        feature_stats,
        target_stats,
        mode=task_cfg.window_mode,
    )
    start = computed_start
    end = computed_end
    if start is not None and end is not None:
        size = count_cadence_buckets(start, end, parse_cadence(dataset.sample.cadence))
        window_obj = Window(start=start, end=end, mode=task_cfg.window_mode, size=size)
    sample_domain = _merge_sample_domains(
        feature_domain,
        target_domain,
        task_cfg.window_mode,
    )
    sample_meta = None
    if dataset.sample.keys:
        sample_meta = _sample_metadata(
            dataset.sample.cadence,
            dataset.sample.keys,
            sample_domain,
        )

    doc = VectorMetadata(
        schema_version=VECTOR_METADATA_VERSION,
        features=feature_meta,
        targets=target_meta,
        counts=VectorMetadataCounts(
            feature_vectors=feature_vectors,
            target_vectors=target_vectors,
        ),
        window=window_obj,
        sample=sample_meta,
    )

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    write_json_artifact(
        destination,
        doc.model_dump(mode="json", exclude_none=True),
    )

    meta: dict[str, object] = {
        "features": len(feature_meta),
        "targets": len(target_meta),
    }
    return ArtifactOutput(relative_path=str(relative_path), meta=meta)
