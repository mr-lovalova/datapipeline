from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
)


@dataclass(frozen=True)
class StageDemand:
    stage: int | None
    include_targets: bool


def _needs_scaler(configs: Iterable[FeatureRecordConfig]) -> bool:
    for cfg in configs:
        scale = getattr(cfg, "scale", False)
        if isinstance(scale, dict):
            return True
        if bool(scale):
            return True
    return False


def _requires_scaler(dataset: FeatureDatasetConfig, include_targets: bool) -> bool:
    if _needs_scaler(dataset.features or []):
        return True
    if include_targets and dataset.targets:
        return _needs_scaler(dataset.targets)
    return False


def required_artifacts_for(
    dataset: FeatureDatasetConfig,
    demands: Iterable[StageDemand],
) -> set[str]:
    required: set[str] = set()
    for demand in demands:
        stage = demand.stage
        include_targets = demand.include_targets
        effective_stage = 7 if stage is None else stage

        if effective_stage >= 5 and _requires_scaler(dataset, include_targets):
            required.add(SCALER_STATISTICS)

        if effective_stage >= 6:
            required.add(VECTOR_SCHEMA)

    return required
