from collections.abc import Iterable
from pathlib import Path

from datapipeline.artifacts.planning import ArtifactGraph, build_artifact_graph
from datapipeline.artifacts.validation import nested_tick_dependencies
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.build.state import BuildState, load_build_state
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap.config import build_state_path
from datapipeline.services.project_paths import tasks_dir


def hydrate_runtime_artifacts(
    *,
    runtime: Runtime,
    graph: ArtifactGraph,
    state: BuildState | None,
    config_hash: str,
    artifact_keys: Iterable[str],
) -> tuple[str, ...]:
    keys = set(artifact_keys)
    runtime.artifacts.clear()
    if state is None or not keys:
        return ()

    freshness = graph.freshness(
        keys=keys,
        state=state,
        config_hash=config_hash,
        artifacts_root=runtime.artifacts.root,
    )
    keys_without_producers = {key for key in keys if key not in graph.tasks_by_id}
    unavailable_keys = keys_without_producers | graph.dependents_of(
        keys_without_producers,
        active_keys=keys,
    )
    current_keys = keys - set(freshness.outdated) - unavailable_keys
    ordered_current = graph.topological_order(current_keys)
    for key in ordered_current:
        info = state.artifacts[key]
        runtime.artifacts.register(
            key,
            relative_path=info.relative_path,
            meta=info.meta,
        )
    return ordered_current


def hydrate_runtime_artifacts_for_project(
    runtime: Runtime,
    project_path: Path,
    *,
    graph: ArtifactGraph | None = None,
    dataset: FeatureDatasetConfig | None = None,
) -> tuple[str, ...]:
    state = load_build_state(build_state_path(project_path))
    if state is None:
        runtime.artifacts.clear()
        return ()

    if graph is None:
        artifact_tasks, _runtime_tasks = operation_specs(project_path)
        graph = build_artifact_graph(artifact_tasks)

    artifact_roots = graph.declared_artifact_keys()
    artifact_keys = set(graph.dependency_closure(artifact_roots))
    if not artifact_keys:
        runtime.artifacts.clear()
        return ()
    if graph.requires_dataset(artifact_keys):
        if dataset is None:
            dataset = load_dataset(project_path)
        artifact_keys = set(graph.active_dependency_closure(artifact_roots, dataset))
    nested_ticks = {
        dependency.task.id
        for dependency in nested_tick_dependencies(project_path, graph, artifact_keys)
    }
    artifact_keys -= nested_ticks | graph.dependents_of(
        nested_ticks,
        active_keys=artifact_keys,
    )
    config_hash = compute_config_hash(project_path, tasks_dir(project_path))
    return hydrate_runtime_artifacts(
        runtime=runtime,
        graph=graph,
        state=state,
        config_hash=config_hash,
        artifact_keys=artifact_keys,
    )
