from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Iterator, Optional, Literal

from .vector import Vector

PayloadMode = Literal["sample", "vector"]


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

    def with_targets(self, targets: Optional[Vector]) -> "Sample":
        return Sample(key=self.key, features=self.features, targets=targets)

    def with_features(self, features: Vector) -> "Sample":
        return Sample(key=self.key, features=features, targets=self.targets)

    def as_full_payload(self) -> dict[str, Any]:
        return asdict(self)

    def as_vector_payload(self) -> dict[str, Any]:
        data: dict[str, Any] = {"features": list(self.features.values.values())}
        if self.targets is not None:
            data["targets"] = list(self.targets.values.values())
        return data
