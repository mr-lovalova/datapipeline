from dataclasses import dataclass
from typing import Any, Optional

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

    def with_targets(self, targets: Optional[Vector]) -> "Sample":
        return Sample(key=self.key, features=self.features, targets=targets)

    def with_features(self, features: Vector) -> "Sample":
        return Sample(key=self.key, features=features, targets=self.targets)
