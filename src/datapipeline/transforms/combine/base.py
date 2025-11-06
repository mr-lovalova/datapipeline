from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from datapipeline.domain.feature import FeatureRecord
from datapipeline.pipeline.context import PipelineContext
from datapipeline.transforms.utils import clone_record_with_value
from datapipeline.transforms.vector_utils import (
    make_partition_id,
    partition_suffix,
)


class FeatureCombineTransform(ABC):
    """Base class for feature-level combine transforms."""

    def __init__(self) -> None:
        self._context: PipelineContext | None = None

    @property
    def context(self) -> PipelineContext | None:
        return self._context

    def bind_context(self, context: PipelineContext) -> None:  # pragma: no cover - optional hook
        """Optional context binding hook mirrored from other transform types."""
        self._context = context

    @abstractmethod
    def apply(
        self,
        stream: Iterator[FeatureRecord],
        *,
        inputs: Mapping[str, Iterator[FeatureRecord]],
        target_id: str,
    ) -> Iterator[FeatureRecord]:
        """Return an iterator of derived FeatureRecord objects."""

    def __call__(
        self,
        stream: Iterator[FeatureRecord],
        *,
        inputs: Mapping[str, Iterator[FeatureRecord]],
        target_id: str,
    ) -> Iterator[FeatureRecord]:
        return self.apply(stream, inputs=inputs, target_id=target_id)


@dataclass(frozen=True)
class AlignedFeatureRow:
    """Represents a single timestamp/partition slice across aligned streams."""

    record: FeatureRecord
    partition: str
    values: Mapping[str, Any]

    @property
    def time(self) -> datetime:
        return self.record.record.time

    def get(self, feature_id: str, default: Any = None) -> Any:
        return self.values.get(feature_id, default)


class FeatureStreamAligner:
    """Align dependency feature streams by (partition, timestamp)."""

    def __init__(
        self,
        *,
        dependencies: Mapping[str, Iterable[FeatureRecord]],
        required: set[str] | None = None,
    ) -> None:
        self._required = set(required or ())
        self._tables = {
            feature_id: self._index_stream(stream)
            for feature_id, stream in dependencies.items()
        }

    @staticmethod
    def _index_stream(stream: Iterable[FeatureRecord]) -> dict[str, dict[datetime, FeatureRecord]]:
        buckets: dict[str, dict[datetime, FeatureRecord]] = defaultdict(dict)
        for fr in stream:
            suffix = partition_suffix(fr.id)
            buckets[suffix][fr.record.time] = fr
        return buckets

    def lookup(
        self, feature_id: str, partition: str, when: datetime
    ) -> FeatureRecord | None:
        table = self._tables.get(feature_id, {})
        return table.get(partition, {}).get(when)

    def aligned_values(
        self, partition: str, when: datetime
    ) -> tuple[dict[str, Any], bool]:
        values: dict[str, Any] = {}
        missing_required = False
        for feature_id, table in self._tables.items():
            fr = table.get(partition, {}).get(when)
            value = fr.record.value if fr else None
            values[feature_id] = value
            if feature_id in self._required and value is None:
                missing_required = True
                break
        return values, missing_required


class AlignedFeatureCombineTransform(FeatureCombineTransform):
    """Convenience base that aligns dependency streams for you."""

    def __init__(self, *, required: set[str] | Iterable[str] | None = None) -> None:
        super().__init__()
        self._required = set(required or ())

    @abstractmethod
    def compute(self, row: AlignedFeatureRow) -> Any:
        """Return the derived feature value for the aligned row."""

    def apply(
        self,
        stream: Iterator[FeatureRecord],
        *,
        inputs: Mapping[str, Iterator[FeatureRecord]],
        target_id: str,
    ) -> Iterator[FeatureRecord]:
        aligner = FeatureStreamAligner(
            dependencies=inputs,
            required=self._required,
        )

        for fr in stream:
            partition = partition_suffix(fr.id)
            when = fr.record.time
            values, missing_required = aligner.aligned_values(partition, when)
            if missing_required:
                continue

            row = AlignedFeatureRow(
                record=fr,
                partition=partition,
                values=values,
            )
            derived = self.compute(row)
            if derived is None:
                continue
            yield FeatureRecord(
                record=clone_record_with_value(fr.record, derived),
                id=make_partition_id(target_id, partition),
            )
