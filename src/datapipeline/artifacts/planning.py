from dataclasses import dataclass
from typing import Iterable

from datapipeline.artifacts.specs import (
    ARTIFACT_DEFINITIONS,
    ArtifactDefinition,
    artifact_definition_for_key,
    artifact_key_for_task_id,
    artifact_keys_for_task_ids,
)
from datapipeline.build.state import BuildState
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import ArtifactTask, TicksTask


@dataclass(frozen=True)
class ArtifactPlanningContext:
    task_configs: tuple[ArtifactTask, ...]
    definitions: tuple[ArtifactDefinition, ...]
    tasks_by_id: dict[str, ArtifactTask]


def build_planning_context(
    task_configs: Iterable[ArtifactTask],
) -> ArtifactPlanningContext:
    configs = tuple(task_configs)
    definitions = (
        *ARTIFACT_DEFINITIONS,
        *(
            ArtifactDefinition(key=task.id, task_id=task.id, min_stage=None)
            for task in configs
            if isinstance(task, TicksTask)
        ),
    )
    return ArtifactPlanningContext(
        task_configs=configs,
        definitions=definitions,
        tasks_by_id={task.id: task for task in configs},
    )


def selected_artifact_keys_for_build(
    *,
    context: ArtifactPlanningContext,
    required_artifacts: set[str] | None = None,
    profile_target: str | None = None,
    profile_name: str | None = None,
) -> set[str]:
    profile_keys: set[str] | None = None
    if profile_target:
        if profile_target not in context.tasks_by_id:
            raise ValueError(
                f"Build profile '{profile_name or '<unnamed>'}' references unknown target '{profile_target}'."
            )
        key = artifact_key_for_task_id(profile_target, context.definitions)
        if key is None:
            raise ValueError(
                f"Build profile '{profile_name or '<unnamed>'}' target '{profile_target}' "
                "is not bound to an artifact definition."
            )
        profile_keys = {key}

    if required_artifacts is None:
        if profile_keys is not None:
            selected = set(profile_keys)
        else:
            selected = artifact_keys_for_task_ids(
                set(context.tasks_by_id.keys()),
                context.definitions,
            )
    else:
        selected = set(required_artifacts)
        if profile_keys is not None:
            selected &= profile_keys

    by_key = {definition.key: definition for definition in context.definitions}
    pending = list(selected)
    while pending:
        key = pending.pop()
        definition = by_key.get(key)
        if definition is None:
            continue
        for dependency in definition.dependencies:
            if dependency in selected:
                continue
            selected.add(dependency)
            pending.append(dependency)
    return selected


def required_artifact_keys_for_dataset(
    *,
    context: ArtifactPlanningContext,
    selected_keys: set[str],
    dataset: FeatureDatasetConfig,
) -> set[str]:
    required: set[str] = set()
    for key in selected_keys:
        definition = artifact_definition_for_key(key, context.definitions)
        if definition is None or definition.is_required_for(dataset):
            required.add(key)
    return required


def has_dataset_requirements(
    *,
    context: ArtifactPlanningContext,
    selected_keys: set[str],
) -> bool:
    for key in selected_keys:
        definition = artifact_definition_for_key(key, context.definitions)
        if definition is not None and definition.requires_dataset():
            return True
    return False


def stale_artifact_keys(
    *,
    selected_keys: Iterable[str],
    state: BuildState | None,
    config_hash: str,
    hash_meta_key: str = "_config_hash",
) -> set[str]:
    stale: set[str] = set()
    for key in selected_keys:
        if state is None:
            stale.add(key)
            continue
        info = state.artifacts.get(key)
        if info is None:
            stale.add(key)
            continue
        value = (info.meta or {}).get(hash_meta_key)
        artifact_hash = value if isinstance(value, str) and value.strip() else state.config_hash
        if artifact_hash != config_hash:
            stale.add(key)
    return stale
