from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from datapipeline.artifacts.models import (
    ListVectorColumnStats,
    ListVectorMetadataEntry,
    ScalarVectorColumnStats,
    VectorBaseStats,
    VectorColumnStats,
    VectorMetadataEntry,
    VectorStatsSection,
)
from datapipeline.transforms.utils import is_missing


@dataclass(slots=True)
class _Counts:
    present_samples: int = 0
    non_null_samples: int = 0
    observed_elements: int = 0


class VectorStatsAccumulator:
    """Collect bounded availability counters for one vector section."""

    def __init__(self, entries: Sequence[VectorMetadataEntry]) -> None:
        self._entries = {entry.id: entry for entry in entries}
        self._bases = {entry.base_id: _Counts() for entry in entries}
        self._columns = {entry.id: _Counts() for entry in entries}

    def update(self, values: Mapping[str, Any]) -> None:
        present_bases: set[str] = set()
        non_null_bases: set[str] = set()

        for identifier, value in values.items():
            entry = self._entries.get(identifier)
            if entry is None:
                raise ValueError(
                    f"Vector contains ID {identifier!r} missing from metadata. "
                    "Rebuild vector metadata."
                )

            present_bases.add(entry.base_id)
            counts = self._columns[identifier]
            counts.present_samples += 1

            if isinstance(entry, ListVectorMetadataEntry):
                observed = _observed_list_elements(entry, value)
                counts.observed_elements += observed
                if observed:
                    counts.non_null_samples += 1
                    non_null_bases.add(entry.base_id)
            else:
                if isinstance(value, list):
                    raise ValueError(
                        f"Scalar vector {identifier!r} contains a list value."
                    )
                if not is_missing(value):
                    counts.non_null_samples += 1
                    non_null_bases.add(entry.base_id)

        for base_id in present_bases:
            self._bases[base_id].present_samples += 1
        for base_id in non_null_bases:
            self._bases[base_id].non_null_samples += 1

    def finish(self) -> VectorStatsSection:
        bases = tuple(
            VectorBaseStats(
                id=identifier,
                present_samples=counts.present_samples,
                non_null_samples=counts.non_null_samples,
            )
            for identifier, counts in sorted(self._bases.items())
        )
        columns: list[VectorColumnStats] = []
        for identifier, entry in sorted(self._entries.items()):
            counts = self._columns[identifier]
            if isinstance(entry, ListVectorMetadataEntry):
                columns.append(
                    ListVectorColumnStats(
                        id=identifier,
                        base_id=entry.base_id,
                        present_samples=counts.present_samples,
                        non_null_samples=counts.non_null_samples,
                        kind="list",
                        length=entry.cadence.target,
                        observed_elements=counts.observed_elements,
                    )
                )
            else:
                columns.append(
                    ScalarVectorColumnStats(
                        id=identifier,
                        base_id=entry.base_id,
                        present_samples=counts.present_samples,
                        non_null_samples=counts.non_null_samples,
                        kind="scalar",
                    )
                )
        return VectorStatsSection(bases=bases, columns=tuple(columns))


def _observed_list_elements(
    entry: ListVectorMetadataEntry,
    value: object,
) -> int:
    if is_missing(value):
        return 0
    if not isinstance(value, list):
        raise ValueError(f"List vector {entry.id!r} contains a scalar value.")
    expected = entry.cadence.target
    if len(value) != expected:
        raise ValueError(
            f"List vector {entry.id!r} has length {len(value)}; expected {expected}."
        )
    return sum(not is_missing(element) for element in value)
