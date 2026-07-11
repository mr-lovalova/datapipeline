from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
    VECTOR_STATS,
)


@dataclass(frozen=True)
class ArtifactDefinition:
    key: str
    dependencies: tuple[str, ...] = ()
    required_if: Callable[[FeatureDatasetConfig], bool] | None = None

    def requires_dataset(self) -> bool:
        return self.required_if is not None

    def is_required_for(self, dataset: FeatureDatasetConfig) -> bool:
        if self.required_if is None:
            return True
        return self.required_if(dataset)


def feature_uses_managed_scaler(config: FeatureRecordConfig) -> bool:
    scale = config.scale
    if scale is True:
        return True
    return isinstance(scale, Mapping) and bool(scale) and "model_path" not in scale


def _needs_scaler(configs: Iterable[FeatureRecordConfig]) -> bool:
    return any(feature_uses_managed_scaler(config) for config in configs)


def dataset_requires_scaler(dataset: FeatureDatasetConfig) -> bool:
    if _needs_scaler(dataset.features or []):
        return True
    if dataset.targets:
        return _needs_scaler(dataset.targets)
    return False


ARTIFACT_DEFINITIONS: tuple[ArtifactDefinition, ...] = (
    ArtifactDefinition(
        key=SCALER_STATISTICS,
        required_if=dataset_requires_scaler,
    ),
    ArtifactDefinition(
        key=VECTOR_INPUTS,
        dependencies=(SCALER_STATISTICS,),
    ),
    ArtifactDefinition(
        key=VECTOR_SCHEMA,
        dependencies=(VECTOR_INPUTS,),
    ),
    ArtifactDefinition(
        key=VECTOR_METADATA,
        dependencies=(VECTOR_INPUTS,),
    ),
    ArtifactDefinition(
        key=VECTOR_STATS,
        dependencies=(VECTOR_METADATA, VECTOR_SCHEMA),
    ),
)
