from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    ScalarVectorMetadataEntry,
    VectorMetadataEntry,
    VectorSchemaCadence,
)
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.runner import resolve_heartbeat_interval_seconds
from datapipeline.execution.observability import OperationProgressTracker
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.domain.feature_id import base_id as _base_feature_id
from datapipeline.transforms.utils import is_missing


def _type_name(value: object) -> str:
    if value is None:
        return "null"
    return type(value).__name__


@dataclass
class VectorMetadataStats:
    id: str
    base_id: str
    kind: Literal["scalar", "list"] | None = None
    list_length: int | None = None
    present_count: int = 0
    null_count: int = 0
    scalar_types: set[str] = field(default_factory=set)
    element_types: set[str] = field(default_factory=set)
    lengths: Counter[int] = field(default_factory=Counter)
    first_observed: datetime | None = None
    last_observed: datetime | None = None
    observed_elements: int = 0

    def observe(self, value: object, observed_at: datetime | None) -> None:
        if observed_at is not None:
            self.first_observed = (
                observed_at
                if self.first_observed is None
                else min(self.first_observed, observed_at)
            )
            self.last_observed = (
                observed_at
                if self.last_observed is None
                else max(self.last_observed, observed_at)
            )

        self.present_count += 1
        if is_missing(value):
            self.null_count += 1
            return

        if isinstance(value, list):
            if self.kind == "scalar":
                raise ValueError(
                    f"Vector {self.id!r} contains both scalar and list values."
                )
            if not value:
                raise ValueError(
                    f"Vector {self.id!r} contains an empty list; "
                    "list vectors require a positive fixed length."
                )
            length = len(value)
            if self.list_length is not None and self.list_length != length:
                raise ValueError(
                    f"Vector {self.id!r} contains list values with different "
                    f"lengths: {self.list_length} and {length}."
                )
            self.kind = "list"
            self.list_length = length
            self.lengths[length] += 1
            self.observed_elements += sum(
                1 for element in value if not is_missing(element)
            )
            self.element_types.update(_type_name(element) for element in value)
            return

        if self.kind == "list":
            raise ValueError(
                f"Vector {self.id!r} contains both list and scalar values."
            )
        self.kind = "scalar"
        self.scalar_types.add(_type_name(value))


def collect_vector_metadata(
    runtime: Runtime,
    configs: Sequence[FeatureRecordConfig],
    cadence: str,
    sample_keys: Sequence[str],
    progress_step: str,
) -> tuple[
    list[VectorMetadataStats],
    int,
    dict[tuple, tuple[datetime, datetime]],
]:
    if not configs:
        return [], 0, {}
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        configs,
        cadence,
        rectangular=False,
        sample_keys=sample_keys,
    )

    stats: dict[str, VectorMetadataStats] = {}
    sample_domain: dict[tuple, tuple[datetime, datetime]] = {}
    vector_count = 0
    progress = OperationProgressTracker(
        progress_step,
        "vectors",
        resolve_heartbeat_interval_seconds(runtime.heartbeat_interval_seconds),
    )
    try:
        for sample in vectors:
            vector_count += 1
            if (
                not isinstance(sample.key, tuple)
                or len(sample.key) != len(sample_keys) + 1
                or not isinstance(sample.key[0], datetime)
            ):
                raise RuntimeError(
                    "Vector sample key does not match the configured sample keys."
                )
            ts = sample.key[0]
            if sample_keys:
                _update_sample_domain(sample_domain, sample.key, ts)
            for fid, value in sample.features.values.items():
                entry = stats.get(fid)
                if entry is None:
                    entry = stats[fid] = VectorMetadataStats(
                        id=fid,
                        base_id=_base_feature_id(fid),
                    )
                entry.observe(value, ts)
            progress.advance()
    finally:
        closer = getattr(vectors, "close", None)
        if callable(closer):
            closer()

    config_order = {config.id: index for index, config in enumerate(configs)}
    observed_ids = {entry.base_id for entry in stats.values()}
    unexpected_ids = sorted(observed_ids - set(config_order))
    if unexpected_ids:
        raise RuntimeError(
            "Vector metadata contains IDs outside the configured vectors: "
            f"{unexpected_ids!r}."
        )
    missing_ids = sorted(set(config_order) - observed_ids)
    if missing_ids:
        raise RuntimeError(
            "Configured vectors produced no metadata: "
            f"{missing_ids!r}. Check upstream source data and credentials."
        )

    ordered_stats = sorted(
        stats.values(),
        key=lambda entry: (config_order[entry.base_id], entry.id),
    )
    return ordered_stats, vector_count, sample_domain


def _update_sample_domain(
    sample_domain: dict[tuple, tuple[datetime, datetime]],
    group_key: tuple,
    ts: datetime,
) -> None:
    key_values = tuple(group_key[1:])
    current = sample_domain.get(key_values)
    if current is None:
        sample_domain[key_values] = (ts, ts)
        return
    start, end = current
    sample_domain[key_values] = min(start, ts), max(end, ts)


def metadata_entries_from_stats(
    entries: Sequence[VectorMetadataStats],
) -> tuple[VectorMetadataEntry, ...]:
    metadata_entries: list[VectorMetadataEntry] = []
    for entry in entries:
        kind = entry.kind or "scalar"
        if kind == "list":
            target = entry.list_length
            if target is None:
                raise ValueError(
                    f"List metadata entry {entry.id!r} has no fixed sequence length."
                )
            metadata_entries.append(
                ListVectorMetadataEntry(
                    id=entry.id,
                    base_id=entry.base_id,
                    kind="list",
                    present_count=entry.present_count,
                    null_count=entry.null_count,
                    first_observed=entry.first_observed,
                    last_observed=entry.last_observed,
                    element_types=tuple(sorted(entry.element_types)),
                    lengths={
                        str(length): count
                        for length, count in sorted(entry.lengths.items())
                    },
                    cadence=VectorSchemaCadence(target=target),
                    observed_elements=entry.observed_elements,
                )
            )
            continue
        if kind != "scalar":
            raise ValueError(f"Unsupported vector metadata kind {kind!r}.")
        metadata_entries.append(
            ScalarVectorMetadataEntry(
                id=entry.id,
                base_id=entry.base_id,
                kind="scalar",
                present_count=entry.present_count,
                null_count=entry.null_count,
                first_observed=entry.first_observed,
                last_observed=entry.last_observed,
                value_types=tuple(sorted(entry.scalar_types)),
            )
        )
    return tuple(metadata_entries)
