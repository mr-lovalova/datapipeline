from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType

from datapipeline.artifacts.specs import ARTIFACT_DEFINITIONS, ArtifactDefinition
from datapipeline.build.state import BuildState
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import ArtifactTask, OperationTask, StatsTask, TicksTask
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
    VECTOR_STATS,
)


@dataclass(frozen=True)
class ArtifactFreshness:
    missing: frozenset[str]
    stale: frozenset[str]
    outdated: frozenset[str]


@dataclass(frozen=True)
class ArtifactGraph:
    definitions: tuple[ArtifactDefinition, ...]
    tasks_by_id: Mapping[str, ArtifactTask]
    _definitions_by_key: Mapping[str, ArtifactDefinition] = field(
        init=False,
        repr=False,
    )
    _keys_by_task_id: Mapping[str, str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        definitions_by_key: dict[str, ArtifactDefinition] = {}
        keys_by_task_id: dict[str, str] = {}
        for definition in self.definitions:
            if definition.key in definitions_by_key:
                raise ValueError(f"Duplicate artifact key '{definition.key}'.")
            if definition.task_id in keys_by_task_id:
                raise ValueError(
                    f"Artifact task '{definition.task_id}' produces more than one artifact."
                )
            definitions_by_key[definition.key] = definition
            keys_by_task_id[definition.task_id] = definition.key

        for definition in self.definitions:
            for dependency in definition.dependencies:
                if dependency not in definitions_by_key:
                    raise ValueError(
                        f"Artifact '{definition.key}' has unknown dependency '{dependency}'."
                    )

        object.__setattr__(
            self,
            "tasks_by_id",
            MappingProxyType(dict(self.tasks_by_id)),
        )
        object.__setattr__(
            self,
            "_definitions_by_key",
            MappingProxyType(definitions_by_key),
        )
        object.__setattr__(
            self,
            "_keys_by_task_id",
            MappingProxyType(keys_by_task_id),
        )
        self._validate_acyclic()

    @classmethod
    def from_tasks(cls, task_configs: Iterable[ArtifactTask]) -> "ArtifactGraph":
        tasks = tuple(task_configs)
        tasks_by_id: dict[str, ArtifactTask] = {}
        for task in tasks:
            if task.id in tasks_by_id:
                raise ValueError(f"Duplicate artifact task id '{task.id}'.")
            tasks_by_id[task.id] = task

        tasks_by_output: dict[Path, str] = {}
        for task in tasks:
            output = Path(task.output)
            previous_task_id = tasks_by_output.get(output)
            if previous_task_id is not None:
                raise ValueError(
                    f"Artifact tasks '{previous_task_id}' and '{task.id}' write "
                    f"the same output '{task.output}'."
                )
            tasks_by_output[output] = task.id

        definitions = list(ARTIFACT_DEFINITIONS)
        stats_task = tasks_by_id.get("stats")
        if isinstance(stats_task, StatsTask) and stats_task.mode == "raw":
            definitions = [
                replace(
                    definition,
                    dependencies=(VECTOR_SCHEMA_METADATA,),
                )
                if definition.task_id == "stats"
                else definition
                for definition in definitions
            ]

        built_in_task_ids = {definition.task_id for definition in definitions}
        built_in_keys = {definition.key for definition in definitions}
        tick_artifact_keys = tuple(
            task.id
            for task in tasks
            if isinstance(task, TicksTask) and task.id not in built_in_task_ids
        )
        if tick_artifact_keys:
            definitions = [
                replace(
                    definition,
                    dependencies=(*definition.dependencies, *tick_artifact_keys),
                )
                if definition.key in {SCALER_STATISTICS, VECTOR_INPUTS}
                else definition
                for definition in definitions
            ]

        for task in tasks:
            if task.id in built_in_task_ids:
                continue
            if task.id in built_in_keys:
                raise ValueError(
                    f"Artifact task id '{task.id}' collides with a built-in artifact key."
                )
            definitions.append(ArtifactDefinition(key=task.id, task_id=task.id))

        return cls(tuple(definitions), tasks_by_id)

    def _validate_acyclic(self) -> None:
        visited: set[str] = set()
        path: list[str] = []

        def visit(key: str) -> None:
            if key in path:
                start = path.index(key)
                cycle = [*path[start:], key]
                raise ValueError("Artifact dependency cycle: " + " -> ".join(cycle))
            if key in visited:
                return
            path.append(key)
            for dependency in self.definition(key).dependencies:
                visit(dependency)
            path.pop()
            visited.add(key)

        for definition in self.definitions:
            visit(definition.key)

    def definition(self, key: str) -> ArtifactDefinition:
        try:
            return self._definitions_by_key[key]
        except KeyError as exc:
            raise ValueError(f"Unknown artifact '{key}'.") from exc

    def key_for_task(self, task_id: str) -> str | None:
        return self._keys_by_task_id.get(task_id)

    def keys_for_tasks(self, task_ids: Iterable[str]) -> set[str]:
        return {
            key
            for task_id in task_ids
            if (key := self.key_for_task(task_id)) is not None
        }

    def declared_artifact_keys(self) -> set[str]:
        return self.keys_for_tasks(self.tasks_by_id)

    def runtime_requirements(
        self,
        task: OperationTask,
        *,
        preview_index: int | None,
    ) -> set[str]:
        tick_artifacts = self.keys_for_tasks(
            artifact_task.id
            for artifact_task in self.tasks_by_id.values()
            if isinstance(artifact_task, TicksTask)
        )
        if task.entrypoint == "core.runtime.pipeline":
            if preview_index is None:
                return {VECTOR_SCHEMA_METADATA, VECTOR_SCHEMA}
            if preview_index < 0 or preview_index > 14:
                raise ValueError("preview_index must be between 0 and 14")
            if preview_index <= 6:
                return set()
            if preview_index <= 9:
                return tick_artifacts
            if preview_index <= 11:
                return {SCALER_STATISTICS, *tick_artifacts}
            if preview_index == 12:
                return {VECTOR_SCHEMA_METADATA}
            return {VECTOR_SCHEMA_METADATA, VECTOR_SCHEMA}
        if task.entrypoint == "core.runtime.materialize_stream":
            return tick_artifacts
        if task.entrypoint in {
            "core.runtime.coverage",
            "core.runtime.matrix",
            "core.runtime.thresholds",
        }:
            return {VECTOR_STATS}
        return set()

    def runtime_dependency_closure(
        self,
        task: OperationTask,
        *,
        preview_index: int | None,
        dataset: FeatureDatasetConfig | None,
    ) -> tuple[str, ...]:
        roots = self.runtime_requirements(task, preview_index=preview_index)
        if (
            task.entrypoint == "core.runtime.pipeline"
            and dataset is not None
            and not dataset.features
            and not dataset.targets
        ):
            return ()
        keys = set(self.dependency_closure(roots))
        if self.requires_dataset(keys):
            if dataset is None:
                raise ValueError(
                    f"Runtime task '{task.id}' requires a feature dataset to "
                    "resolve artifact dependencies."
                )
            keys = set(self.active_dependency_closure(roots, dataset))
        return self.topological_order(keys)

    def select_roots(
        self,
        *,
        required_artifacts: set[str] | None = None,
        profile_target: str | None = None,
        profile_name: str | None = None,
    ) -> set[str]:
        profile_keys: set[str] | None = None
        if profile_target:
            if profile_target not in self.tasks_by_id:
                raise ValueError(
                    f"Build profile '{profile_name or '<unnamed>'}' references "
                    f"unknown target '{profile_target}'."
                )
            key = self.key_for_task(profile_target)
            if key is None:
                raise ValueError(
                    f"Build profile '{profile_name or '<unnamed>'}' target "
                    f"'{profile_target}' is not bound to an artifact definition."
                )
            profile_keys = {key}

        if required_artifacts is None:
            return (
                set(profile_keys)
                if profile_keys is not None
                else self.declared_artifact_keys()
            )

        selected = set(required_artifacts)
        if profile_keys is not None:
            selected &= profile_keys
        return selected

    def select_keys(
        self,
        *,
        required_artifacts: set[str] | None = None,
        profile_target: str | None = None,
        profile_name: str | None = None,
    ) -> set[str]:
        roots = self.select_roots(
            required_artifacts=required_artifacts,
            profile_target=profile_target,
            profile_name=profile_name,
        )
        return set(self.dependency_closure(roots))

    def dependency_closure(self, roots: Iterable[str]) -> tuple[str, ...]:
        root_keys = set(roots)
        for key in root_keys:
            self.definition(key)

        selected: set[str] = set()

        def include(key: str) -> None:
            if key in selected:
                return
            selected.add(key)
            for dependency in self.definition(key).dependencies:
                include(dependency)

        for definition in self.definitions:
            if definition.key in root_keys:
                include(definition.key)
        return self.topological_order(selected)

    def active_dependency_closure(
        self,
        roots: Iterable[str],
        dataset: FeatureDatasetConfig,
    ) -> tuple[str, ...]:
        root_keys = set(roots)
        for key in root_keys:
            self.definition(key)

        selected: set[str] = set()

        def include(key: str) -> None:
            if key in selected:
                return
            definition = self.definition(key)
            if not definition.is_required_for(dataset):
                return
            selected.add(key)
            for dependency in definition.dependencies:
                include(dependency)

        for definition in self.definitions:
            if definition.key in root_keys:
                include(definition.key)
        return self.topological_order(selected)

    def topological_order(self, keys: Iterable[str]) -> tuple[str, ...]:
        selected = set(keys)
        for key in selected:
            self.definition(key)

        ordered: list[str] = []
        visited: set[str] = set()

        def visit(key: str) -> None:
            if key in visited:
                return
            for dependency in self.definition(key).dependencies:
                if dependency in selected:
                    visit(dependency)
            visited.add(key)
            ordered.append(key)

        for definition in self.definitions:
            if definition.key in selected:
                visit(definition.key)
        return tuple(ordered)

    def requires_dataset(self, keys: Iterable[str]) -> bool:
        return any(self.definition(key).requires_dataset() for key in keys)

    def validate_producers(self, keys: Iterable[str]) -> None:
        for key in self.topological_order(keys):
            definition = self.definition(key)
            if definition.task_id not in self.tasks_by_id:
                raise ValueError(
                    f"Required artifact '{key}' is produced by task "
                    f"'{definition.task_id}', but that task is not declared."
                )

    def dependents_of(
        self,
        keys: Iterable[str],
        *,
        active_keys: Iterable[str] | None = None,
    ) -> set[str]:
        roots = set(keys)
        active = (
            set(active_keys)
            if active_keys is not None
            else set(self._definitions_by_key)
        )
        dependents: set[str] = set()
        pending = list(roots)
        while pending:
            dependency = pending.pop()
            for definition in self.definitions:
                if definition.key not in active or definition.key in roots:
                    continue
                if dependency not in definition.dependencies:
                    continue
                if definition.key in dependents:
                    continue
                dependents.add(definition.key)
                pending.append(definition.key)
        return dependents

    def freshness(
        self,
        *,
        keys: Iterable[str],
        state: BuildState | None,
        config_hash: str,
        artifacts_root: Path,
        hash_meta_key: str = "_config_hash",
    ) -> ArtifactFreshness:
        selected = set(keys)
        missing: set[str] = set()
        stale: set[str] = set()
        root = artifacts_root.resolve()

        for key in selected:
            if state is None:
                missing.add(key)
                continue
            info = state.artifacts.get(key)
            if info is None:
                missing.add(key)
                continue
            definition = self.definition(key)
            producer = self.tasks_by_id.get(definition.task_id)
            if producer is not None and Path(info.relative_path) != Path(
                producer.output
            ):
                stale.add(key)
                continue
            value = (info.meta or {}).get(hash_meta_key)
            artifact_hash = (
                value if isinstance(value, str) and value.strip() else state.config_hash
            )
            if artifact_hash != config_hash:
                stale.add(key)
                continue
            artifact_path = (root / info.relative_path).resolve()
            try:
                artifact_path.relative_to(root)
            except ValueError:
                stale.add(key)
                continue
            if not artifact_path.is_file():
                missing.add(key)

        outdated = missing | stale
        for key in self.topological_order(selected):
            dependencies = self.definition(key).dependencies
            if any(
                dependency in selected and dependency in outdated
                for dependency in dependencies
            ):
                outdated.add(key)

        return ArtifactFreshness(
            missing=frozenset(missing),
            stale=frozenset(stale),
            outdated=frozenset(outdated),
        )


def build_artifact_graph(task_configs: Iterable[ArtifactTask]) -> ArtifactGraph:
    return ArtifactGraph.from_tasks(task_configs)
