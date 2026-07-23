from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    ScalarVectorMetadataEntry,
    VectorMetadataEntry,
)
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.domain.series_id import base_id as _base_series_id
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


class VectorMetadataCollector:
    def __init__(
        self,
        configs: Sequence[SeriesConfig],
        sample_keys: Sequence[str],
    ) -> None:
        self._config_order = {config.id: index for index, config in enumerate(configs)}
        self._sample_keys = tuple(sample_keys)
        self._stats: dict[str, VectorMetadataStats] = {}
        self._sample_domain: dict[tuple, tuple[datetime, datetime]] = {}
        self._vector_count = 0

    def observe(self, key: tuple, values: Mapping[str, object]) -> None:
        if not values:
            return
        if (
            not isinstance(key, tuple)
            or len(key) != len(self._sample_keys) + 1
            or not isinstance(key[0], datetime)
        ):
            raise RuntimeError(
                "Vector sample key does not match the configured sample keys."
            )

        self._vector_count += 1
        observed_at = key[0]
        if self._sample_keys:
            _update_sample_domain(self._sample_domain, key, observed_at)
        for series_id, value in values.items():
            entry = self._stats.get(series_id)
            if entry is None:
                entry = self._stats[series_id] = VectorMetadataStats(
                    id=series_id,
                    base_id=_base_series_id(series_id),
                )
            entry.observe(value, observed_at)

    def result(
        self,
    ) -> tuple[
        list[VectorMetadataStats],
        int,
        dict[tuple, tuple[datetime, datetime]],
    ]:
        observed_ids = {entry.base_id for entry in self._stats.values()}
        configured_ids = set(self._config_order)
        unexpected_ids = sorted(observed_ids - configured_ids)
        if unexpected_ids:
            raise RuntimeError(
                "Vector metadata contains IDs outside the configured vectors: "
                f"{unexpected_ids!r}."
            )
        missing_ids = sorted(configured_ids - observed_ids)
        if missing_ids:
            raise RuntimeError(
                "Configured vectors produced no metadata: "
                f"{missing_ids!r}. Check upstream source data and credentials."
            )
        ordered = sorted(
            self._stats.values(),
            key=lambda entry: (
                self._config_order[entry.base_id],
                entry.id,
            ),
        )
        return ordered, self._vector_count, self._sample_domain


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
            length = entry.list_length
            if length is None:
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
                    length=length,
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
