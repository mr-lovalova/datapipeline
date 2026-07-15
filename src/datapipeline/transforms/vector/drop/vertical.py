from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector

from ..common import reject_unknown_values, select_expected_ids


@dataclass(frozen=True)
class ColumnCoverage:
    """Observed coverage for one scalar or list-valued vector column."""

    id: str
    kind: Literal["scalar", "list"]
    present_count: int
    null_count: int = 0
    sequence_length: int | None = None
    observed_elements: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id:
            raise ValueError("Coverage id must be a non-empty string")
        if type(self.present_count) is not int or self.present_count < 0:
            raise ValueError("present_count must be a nonnegative integer")
        if (
            type(self.null_count) is not int
            or not 0 <= self.null_count <= self.present_count
        ):
            raise ValueError("null_count must be between zero and present_count")
        if self.kind == "scalar":
            if self.sequence_length is not None or self.observed_elements is not None:
                raise ValueError("Scalar coverage cannot define sequence fields")
            return
        if self.kind != "list":
            raise ValueError("Coverage kind must be 'scalar' or 'list'")
        if type(self.sequence_length) is not int or self.sequence_length <= 0:
            raise ValueError("List coverage requires a positive sequence_length")
        if type(self.observed_elements) is not int or self.observed_elements < 0:
            raise ValueError("List coverage requires nonnegative observed_elements")
        maximum_observed = (self.present_count - self.null_count) * self.sequence_length
        if self.observed_elements > maximum_observed:
            raise ValueError(
                f"Observed elements for {self.id!r} exceed its present sequences"
            )

    def ratio(self, total_opportunities: int) -> float:
        if self.present_count > total_opportunities:
            raise ValueError(f"Coverage for {self.id!r} exceeds total opportunities")
        if self.kind == "scalar":
            return (self.present_count - self.null_count) / total_opportunities

        assert self.sequence_length is not None
        assert self.observed_elements is not None
        expected_elements = total_opportunities * self.sequence_length
        if self.observed_elements > expected_elements:
            raise ValueError(
                f"Observed elements for {self.id!r} exceed total opportunities"
            )
        return self.observed_elements / expected_elements


def _selection(
    expected_ids: Sequence[str],
    coverage: Sequence[ColumnCoverage],
    total_opportunities: int,
    threshold: float,
    ids: Sequence[str] | None,
) -> tuple[tuple[str, ...], frozenset[str]]:
    expected, selected = select_expected_ids(expected_ids, ids)
    if type(total_opportunities) is not int or total_opportunities <= 0:
        raise ValueError("total_opportunities must be a positive integer")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise TypeError("threshold must be a number between 0 and 1")
    minimum = float(threshold)
    if not 0.0 <= minimum <= 1.0:
        raise ValueError("threshold must be between 0 and 1")

    by_id: dict[str, ColumnCoverage] = {}
    expected_set = frozenset(expected)
    for entry in coverage:
        if entry.id not in expected_set:
            raise ValueError(f"Coverage contains unexpected id: {entry.id!r}")
        if entry.id in by_id:
            raise ValueError(f"Duplicate coverage entry for {entry.id!r}")
        by_id[entry.id] = entry
    missing = [identifier for identifier in selected if identifier not in by_id]
    if missing:
        raise ValueError(f"Missing coverage metadata for ids: {missing!r}")

    dropped = frozenset(
        identifier
        for identifier in selected
        if by_id[identifier].ratio(total_opportunities) < minimum
    )
    retained = tuple(identifier for identifier in expected if identifier not in dropped)
    return retained, dropped


class SelectFeaturesTransform:
    """Remove feature columns selected by an explicit coverage plan."""

    def __init__(
        self,
        expected_ids: Sequence[str],
        coverage: Sequence[ColumnCoverage],
        total_opportunities: int,
        threshold: float,
        ids: Sequence[str] | None = None,
    ) -> None:
        self.expected_ids = tuple(expected_ids)
        self.expected_id_set = frozenset(self.expected_ids)
        self.retained_ids, self.dropped_ids = _selection(
            expected_ids,
            coverage,
            total_opportunities,
            threshold,
            ids,
        )

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            values = sample.features.values
            reject_unknown_values(values, self.expected_id_set)
            if not self.dropped_ids:
                yield sample
                continue
            retained = {
                identifier: value
                for identifier, value in values.items()
                if identifier not in self.dropped_ids
            }
            yield sample.with_features(Vector(values=retained))


class SelectTargetsTransform:
    """Remove target columns selected by an explicit coverage plan."""

    def __init__(
        self,
        expected_ids: Sequence[str],
        coverage: Sequence[ColumnCoverage],
        total_opportunities: int,
        threshold: float,
        ids: Sequence[str] | None = None,
    ) -> None:
        self.expected_ids = tuple(expected_ids)
        self.expected_id_set = frozenset(self.expected_ids)
        self.retained_ids, self.dropped_ids = _selection(
            expected_ids,
            coverage,
            total_opportunities,
            threshold,
            ids,
        )

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            if sample.targets is None:
                yield sample
                continue
            values = sample.targets.values
            reject_unknown_values(values, self.expected_id_set)
            if not self.dropped_ids:
                yield sample
                continue
            retained = {
                identifier: value
                for identifier, value in values.items()
                if identifier not in self.dropped_ids
            }
            yield sample.with_targets(Vector(values=retained))
