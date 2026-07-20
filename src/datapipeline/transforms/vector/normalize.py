from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from datapipeline.artifacts.models import VectorMetadataEntry
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector


class _VectorNormalizer:
    def __init__(self, entries: Sequence[VectorMetadataEntry]) -> None:
        self.entries = tuple(entries)
        if not self.entries:
            raise ValueError("metadata entries must not be empty")
        identifiers = [entry.id for entry in self.entries]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("metadata entry ids must be unique")
        self.expected_id_set = frozenset(identifiers)

    def apply(self, values: Mapping[str, Any]) -> dict[str, Any]:
        extras = [
            identifier
            for identifier in values
            if identifier not in self.expected_id_set
        ]
        if extras:
            raise ValueError(f"Vector contains unexpected ids: {extras!r}")

        normalized: dict[str, Any] = {}
        for entry in self.entries:
            if entry.id not in values:
                normalized[entry.id] = self._missing_value(entry)
                continue
            value = values[entry.id]
            if entry.kind == "scalar":
                normalized[entry.id] = value
                continue
            if not isinstance(value, list):
                raise TypeError(f"Vector id {entry.id!r} must contain a list.")
            if len(value) != entry.length:
                raise ValueError(
                    f"Vector id {entry.id!r} requires {entry.length} values; "
                    f"got {len(value)}."
                )
            normalized[entry.id] = value
        return normalized

    @staticmethod
    def _missing_value(entry: VectorMetadataEntry) -> Any:
        if entry.kind == "scalar":
            return None
        return [None] * entry.length


class NormalizeFeaturesTransform:
    """Normalize feature vectors against their metadata contract."""

    def __init__(self, entries: Sequence[VectorMetadataEntry]) -> None:
        self._normalizer = _VectorNormalizer(entries)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            values = self._normalizer.apply(sample.features.values)
            yield sample.with_features(Vector(values=values))


class NormalizeTargetsTransform:
    """Normalize target vectors against their metadata contract."""

    def __init__(self, entries: Sequence[VectorMetadataEntry]) -> None:
        self._normalizer = _VectorNormalizer(entries)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            current = sample.targets.values if sample.targets is not None else {}
            values = self._normalizer.apply(current)
            yield sample.with_targets(Vector(values=values))
