from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from datapipeline.artifacts.models import VectorSchemaEntry
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector

from .common import require_scalar


class _SchemaNormalizer:
    def __init__(self, entries: Sequence[VectorSchemaEntry]) -> None:
        self.entries = tuple(entries)
        if not self.entries:
            raise ValueError("schema entries must not be empty")
        identifiers = [entry.id for entry in self.entries]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("schema entry ids must be unique")
        self.expected_ids = tuple(identifiers)
        self.expected_id_set = frozenset(self.expected_ids)

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
                require_scalar(value, entry.id)
                normalized[entry.id] = value
                continue
            if not isinstance(value, list):
                raise TypeError(f"Vector id {entry.id!r} must contain a list.")
            assert entry.cadence is not None
            if len(value) != entry.cadence.target:
                raise ValueError(
                    f"Vector id {entry.id!r} requires {entry.cadence.target} values; "
                    f"got {len(value)}."
                )
            for item in value:
                require_scalar(item, entry.id)
            normalized[entry.id] = list(value)
        return normalized

    @staticmethod
    def _missing_value(entry: VectorSchemaEntry) -> Any:
        if entry.kind == "scalar":
            return None
        assert entry.cadence is not None
        return [None] * entry.cadence.target


class NormalizeFeaturesTransform:
    """Normalize feature vectors against explicit typed schema entries."""

    def __init__(self, entries: Sequence[VectorSchemaEntry]) -> None:
        self._schema = _SchemaNormalizer(entries)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            values = self._schema.apply(sample.features.values)
            yield sample.with_features(Vector(values=values))


class NormalizeTargetsTransform:
    """Normalize target vectors against explicit typed schema entries."""

    def __init__(self, entries: Sequence[VectorSchemaEntry]) -> None:
        self._schema = _SchemaNormalizer(entries)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            current = sample.targets.values if sample.targets is not None else {}
            values = self._schema.apply(current)
            yield sample.with_targets(Vector(values=values))
