from collections.abc import Callable
from dataclasses import dataclass

from datapipeline.config.dataset.dataset import FeatureDatasetConfig

SCALER_STATISTICS = "scaler"
VECTOR_SCHEMA = "schema"
VECTOR_METADATA = "metadata"
VECTOR_STATS = "stats"
VECTOR_INPUTS = "vector_inputs"


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


def dataset_requires_scaler(dataset: FeatureDatasetConfig) -> bool:
    return any(config.scale for config in (*dataset.features, *dataset.targets))


ARTIFACT_DEFINITIONS: tuple[ArtifactDefinition, ...] = (
    ArtifactDefinition(
        key=SCALER_STATISTICS,
        required_if=dataset_requires_scaler,
    ),
    ArtifactDefinition(
        key=VECTOR_INPUTS,
    ),
    ArtifactDefinition(
        key=VECTOR_METADATA,
        dependencies=(VECTOR_INPUTS,),
    ),
    ArtifactDefinition(
        key=VECTOR_SCHEMA,
        dependencies=(VECTOR_METADATA,),
    ),
    ArtifactDefinition(
        key=VECTOR_STATS,
        dependencies=(VECTOR_METADATA, VECTOR_SCHEMA),
    ),
)
