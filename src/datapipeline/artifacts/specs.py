from collections.abc import Callable
from dataclasses import dataclass

from datapipeline.config.dataset.dataset import DatasetConfig

SCALER_STATISTICS = "scaler"
VECTOR_METADATA = "metadata"
VECTOR_STATS = "stats"
VARIABLE_RECORDS = "variable_records"


@dataclass(frozen=True)
class ArtifactDefinition:
    key: str
    dependencies: tuple[str, ...] = ()
    required_if: Callable[[DatasetConfig], bool] | None = None

    def requires_dataset(self) -> bool:
        return self.required_if is not None

    def is_required_for(self, dataset: DatasetConfig) -> bool:
        if self.required_if is None:
            return True
        return self.required_if(dataset)


def dataset_requires_scaler(dataset: DatasetConfig) -> bool:
    return any(config.scale for config in dataset.variables)


ARTIFACT_DEFINITIONS: tuple[ArtifactDefinition, ...] = (
    ArtifactDefinition(
        key=SCALER_STATISTICS,
        required_if=dataset_requires_scaler,
    ),
    ArtifactDefinition(
        key=VARIABLE_RECORDS,
    ),
    ArtifactDefinition(
        key=VECTOR_METADATA,
        dependencies=(VARIABLE_RECORDS,),
    ),
    ArtifactDefinition(
        key=VECTOR_STATS,
        dependencies=(VECTOR_METADATA,),
    ),
)
