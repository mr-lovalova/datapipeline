from dataclasses import dataclass, replace
from typing import Callable, Iterable, Sequence

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import ArtifactTask
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
    VECTOR_STATS,
)

@dataclass(frozen=True)
class StageDemand:
    stage: int | None


@dataclass(frozen=True)
class ArtifactDefinition:
    key: str
    task_id: str
    min_stage: int | None
    dependencies: tuple[str, ...] = ()
    required_if: Callable[[FeatureDatasetConfig], bool] = lambda _dataset: True


def _needs_scaler(configs: Iterable[FeatureRecordConfig]) -> bool:
    for cfg in configs:
        scale = getattr(cfg, "scale", False)
        if isinstance(scale, dict):
            return True
        if bool(scale):
            return True
    return False


def _requires_scaler(dataset: FeatureDatasetConfig) -> bool:
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
        required_if=_requires_scaler,
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


def artifact_definitions_with_task_dependencies(
    tasks: Iterable[ArtifactTask],
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> tuple[ArtifactDefinition, ...]:
    items = tuple(definitions if definitions is not None else ARTIFACT_DEFINITIONS)
    key_by_id = {item.task_id: item.key for item in items}
    dependencies_by_key: dict[str, tuple[str, ...]] = {}

    for task in tasks:
        key = key_by_id.get(task.id)
        if key is None:
            continue
        dependency_keys: list[str] = []
        seen: set[str] = set()
        for dependency_id in task.dependencies:
            dependency_key = key_by_id.get(dependency_id)
            if dependency_key is None:
                raise ValueError(
                    f"Unknown dependency {dependency_id!r} for task id {task.id!r}"
                )
            if dependency_key in seen:
                continue
            dependency_keys.append(dependency_key)
            seen.add(dependency_key)
        dependencies_by_key[key] = tuple(dependency_keys)

    updated: list[ArtifactDefinition] = []
    for item in items:
        if item.key in dependencies_by_key:
            updated.append(replace(item, dependencies=dependencies_by_key[item.key]))
        else:
            updated.append(item)
    return tuple(updated)


def _requirement_closure(
    keys: set[str],
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> set[str]:
    items = definitions if definitions is not None else ARTIFACT_DEFINITIONS
    mapping = {item.key: item for item in items}
    closure = set(keys)
    stack = list(keys)
    while stack:
        current = stack.pop()
        requirement = mapping.get(current)
        if requirement is None:
            continue
        for dep in requirement.dependencies:
            if dep not in closure:
                closure.add(dep)
                stack.append(dep)
    return closure


def artifact_build_order(
    required_keys: Iterable[str],
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> list[str]:
    items = list(definitions if definitions is not None else ARTIFACT_DEFINITIONS)
    mapping = {item.key: item for item in items}
    selected = _requirement_closure(set(required_keys), definitions=items)

    precedence = {item.key: index for index, item in enumerate(items)}
    visiting: set[str] = set()
    visited: set[str] = set()
    ordered: list[str] = []

    def visit(key: str) -> None:
        if key in visited:
            return
        if key in visiting:
            raise ValueError(f"Artifact dependency cycle detected at '{key}'")
        visiting.add(key)
        requirement = mapping.get(key)
        if requirement is not None:
            for dep in sorted(
                requirement.dependencies,
                key=lambda dep_key: precedence.get(dep_key, len(precedence)),
            ):
                visit(dep)
        visiting.remove(key)
        visited.add(key)
        if key in selected:
            ordered.append(key)

    for key in sorted(selected, key=lambda item: precedence.get(item, len(precedence))):
        visit(key)
    return ordered


def required_artifacts_for(
    dataset: FeatureDatasetConfig,
    demands: Iterable[StageDemand],
    definitions: Sequence[ArtifactDefinition] | None = None,
) -> set[str]:
    required: set[str] = set()
    items = tuple(definitions if definitions is not None else ARTIFACT_DEFINITIONS)
    for demand in demands:
        if demand.stage is None:
            effective_stage = 8
            for requirement in items:
                if requirement.min_stage is None:
                    continue
                if effective_stage < requirement.min_stage:
                    continue
                if not requirement.required_if(dataset):
                    continue
                required.add(requirement.key)
            continue

        # Stage previews are feature-pipeline node-index cutoffs.
        # Only scaler artifacts can be required there (feature transforms node).
        if demand.stage >= 6:
            scaler_def = artifact_definition_for_key(SCALER_STATISTICS, items)
            if scaler_def is not None and scaler_def.required_if(dataset):
                required.add(SCALER_STATISTICS)
    return _requirement_closure(required, definitions=items)
