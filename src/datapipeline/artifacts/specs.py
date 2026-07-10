from dataclasses import dataclass
from typing import Callable, Iterable

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
    VECTOR_STATS,
)


@dataclass(frozen=True)
class ArtifactDefinition:
    key: str
    task_id: str
    dependencies: tuple[str, ...] = ()
    required_if: Callable[[FeatureDatasetConfig], bool] | None = None

    def requires_dataset(self) -> bool:
        return self.required_if is not None

    def is_required_for(self, dataset: FeatureDatasetConfig) -> bool:
        if self.required_if is None:
            return True
        return self.required_if(dataset)


def _needs_scaler(configs: Iterable[FeatureRecordConfig]) -> bool:
    for cfg in configs:
        scale = getattr(cfg, "scale", False)
        if isinstance(scale, dict):
            return True
        if bool(scale):
            return True
    return False


def dataset_requires_scaler(dataset: FeatureDatasetConfig) -> bool:
    if _needs_scaler(dataset.features or []):
        return True
    if dataset.targets:
        return _needs_scaler(dataset.targets)
    return False


ARTIFACT_DEFINITIONS: tuple[ArtifactDefinition, ...] = (
    ArtifactDefinition(
        key=SCALER_STATISTICS,
        task_id="scaler",
        required_if=dataset_requires_scaler,
    ),
    ArtifactDefinition(
        key=VECTOR_INPUTS,
        task_id="vector_inputs",
        dependencies=(SCALER_STATISTICS,),
    ),
    ArtifactDefinition(
        key=VECTOR_SCHEMA,
        task_id="schema",
        dependencies=(VECTOR_INPUTS,),
    ),
    ArtifactDefinition(
        key=VECTOR_SCHEMA_METADATA,
        task_id="metadata",
        dependencies=(VECTOR_INPUTS,),
    ),
    ArtifactDefinition(
        key=VECTOR_STATS,
        task_id="stats",
        dependencies=(VECTOR_SCHEMA_METADATA, VECTOR_SCHEMA),
    ),
)
