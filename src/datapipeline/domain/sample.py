from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator, Optional

from .vector import Vector


@dataclass
class Sample:
    """
    Represents a single grouped vector sample emitted by the pipeline.

    Attributes:
        key: Group identifier (tuple when group_by cadence > 1).
        features: Feature vector payload.
        targets: Optional target vector when requested.
    """

    key: Any
    features: Vector
    targets: Optional[Vector] = None

    def __iter__(self) -> Iterator[Any]:
        """Retain tuple-like unpacking compatibility."""
        yield self.key
        yield self.features

    def __len__(self) -> int:
        return 2

    def __getitem__(self, idx: int) -> Any:
        if idx == 0:
            return self.key
        if idx == 1:
            return self.features
        raise IndexError(idx)

    @property
    def vector(self) -> Vector:
        return self.features

    def with_targets(self, targets: Optional[Vector]) -> "Sample":
        return Sample(key=self.key, features=self.features, targets=targets)

    def with_features(self, features: Vector) -> "Sample":
        return Sample(key=self.key, features=features, targets=self.targets)
