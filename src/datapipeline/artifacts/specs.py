from dataclasses import dataclass
from typing import Callable, Generic, Iterable, Sequence, TypeVar

from datapipeline.build.tasks.metadata import materialize_metadata
from datapipeline.build.tasks.scaler import materialize_scaler_statistics
from datapipeline.build.tasks.schema import materialize_vector_schema
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    ScalerTask,
    SchemaTask,
)
from datapipeline.runtime import Runtime
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
)

MaterializeResult = tuple[str, dict[str, object]] | None
TTask = TypeVar("TTask", bound=ArtifactTask)
ArtifactMaterializer = Callable[[Runtime, TTask], MaterializeResult]


@dataclass(frozen=True)
class StageDemand:
    stage: int | None


@dataclass(frozen=True)
class ArtifactDefinition(Generic[TTask]):
    key: str
    task_kind: str
    min_stage: int
    dependencies: tuple[str, ...] = ()
    required_if: Callable[[FeatureDatasetConfig], bool] = lambda _dataset: True
    task_type: type[TTask] | None = None
    materialize: ArtifactMaterializer[TTask] | None = None

    def supports(self, task: ArtifactTask) -> bool:
        return self.task_type is not None and isinstance(task, self.task_type)

    def run(self, runtime: Runtime, task: ArtifactTask) -> MaterializeResult:
        if self.task_type is None or self.materialize is None:
            raise RuntimeError(f"Artifact '{self.key}' has no materializer binding")
        if not isinstance(task, self.task_type):
            raise TypeError(
                f"Artifact '{self.key}' expects task type "
                f"{self.task_type.__name__}, got {type(task).__name__}"
            )
        return self.materialize(runtime, task)


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
    ArtifactDefinition[SchemaTask]
    | ArtifactDefinition[MetadataTask]
    | ArtifactDefinition[ScalerTask],
    ...
] = (
    ArtifactDefinition(
        key=VECTOR_SCHEMA,
        task_kind="schema",
        min_stage=7,
        task_type=SchemaTask,
        materialize=materialize_vector_schema,
    ),
    ArtifactDefinition(
        key=VECTOR_SCHEMA_METADATA,
        task_kind="metadata",
        min_stage=7,
        dependencies=(VECTOR_SCHEMA,),
        task_type=MetadataTask,
        materialize=materialize_metadata,
    ),
    ArtifactDefinition(
        key=SCALER_STATISTICS,
        task_kind="scaler",
        min_stage=6,
        required_if=_requires_scaler,
        task_type=ScalerTask,
        materialize=materialize_scaler_statistics,
    ),
)


def artifact_definition_for_key(
    key: str,
    definitions: Sequence[ArtifactDefinition[ArtifactTask]] | None = None,
) -> ArtifactDefinition[ArtifactTask] | None:
    items = definitions if definitions is not None else ARTIFACT_DEFINITIONS
    for item in items:
        if item.key == key:
            return item
    return None


def artifact_keys_for_task_kinds(
    task_kinds: Iterable[str],
    definitions: Sequence[ArtifactDefinition[ArtifactTask]] | None = None,
) -> set[str]:
    selected = set(task_kinds)
    items = definitions if definitions is not None else ARTIFACT_DEFINITIONS
    return {item.key for item in items if item.task_kind in selected}


def _requirement_closure(
    keys: set[str],
    definitions: Sequence[ArtifactDefinition[ArtifactTask]] | None = None,
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
    definitions: Sequence[ArtifactDefinition[ArtifactTask]] | None = None,
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
) -> set[str]:
    required: set[str] = set()
    definitions = ARTIFACT_DEFINITIONS
    for demand in demands:
        effective_stage = 8 if demand.stage is None else demand.stage
        for requirement in definitions:
            if effective_stage < requirement.min_stage:
                continue
            if not requirement.required_if(dataset):
                continue
            required.add(requirement.key)
    return _requirement_closure(required, definitions=definitions)
