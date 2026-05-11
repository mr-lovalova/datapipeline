from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
    VECTOR_STATS,
)


@dataclass(frozen=True)
class ArtifactDefinition:
    key: str
    task_id: str
    min_stage: int | None
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


ARTIFACT_DEFINITIONS: tuple[
    ArtifactDefinition,
    ...
] = (
    ArtifactDefinition(
        key=VECTOR_SCHEMA,
        task_id="schema",
        min_stage=7,
    ),
    ArtifactDefinition(
        key=VECTOR_SCHEMA_METADATA,
        task_id="metadata",
        min_stage=7,
    ),
    ArtifactDefinition(
        key=SCALER_STATISTICS,
        task_id="scaler",
        min_stage=6,
        required_if=dataset_requires_scaler,
    ),
    ArtifactDefinition(
        key=VECTOR_STATS,
        task_id="stats",
        min_stage=None,
    ),
)


def artifact_definition_for_key(
    key: str,
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> ArtifactDefinition | None:
    items = definitions if definitions is not None else ARTIFACT_DEFINITIONS
    for item in items:
        if item.key == key:
            return item
    return None


def artifact_key_for_task_id(
    task_id: str,
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> str | None:
    items = definitions if definitions is not None else ARTIFACT_DEFINITIONS
    for item in items:
        if item.task_id == task_id:
            return item.key
    return None


def task_id_for_artifact_key(
    key: str,
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> str | None:
    item = artifact_definition_for_key(key, definitions)
    return item.task_id if item is not None else None


def artifact_keys_for_task_ids(
    task_ids: Iterable[str],
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> set[str]:
    selected = set(task_ids)
    items = definitions if definitions is not None else ARTIFACT_DEFINITIONS
    return {item.key for item in items if item.task_id in selected}


def artifact_build_order(
    required_keys: Iterable[str],
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> list[str]:
    items = list(definitions if definitions is not None else ARTIFACT_DEFINITIONS)
    selected = set(required_keys)
    precedence = {item.key: index for index, item in enumerate(items)}
    return sorted(selected, key=lambda item: precedence.get(item, len(precedence)))
