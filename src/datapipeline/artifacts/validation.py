from dataclasses import dataclass
from pathlib import Path

from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.tasks import TicksTask
from datapipeline.config.transforms import EnsureTicksConfig
from datapipeline.services.bootstrap.core import load_streams


@dataclass(frozen=True)
class NestedTickDependency:
    task: TicksTask
    tick_artifacts: frozenset[str]


def validate_artifact_plan(
    project_path: Path,
    graph: ArtifactGraph,
    artifact_keys: set[str],
) -> None:
    graph.validate_producers(artifact_keys)
    nested_dependencies = nested_tick_dependencies(
        project_path,
        graph,
        artifact_keys,
    )
    if nested_dependencies:
        dependency = nested_dependencies[0]
        artifacts = ", ".join(sorted(dependency.tick_artifacts))
        raise ValueError(
            f"Tick artifact '{dependency.task.id}' source stream "
            f"'{dependency.task.stream}' uses tick artifact(s): {artifacts}. "
            "Nested tick artifact "
            "dependencies are not supported."
        )


def nested_tick_dependencies(
    project_path: Path,
    graph: ArtifactGraph,
    artifact_keys: set[str],
) -> tuple[NestedTickDependency, ...]:
    tick_tasks: list[TicksTask] = []
    for key in graph.topological_order(artifact_keys):
        task = graph.tasks_by_id.get(key)
        if isinstance(task, TicksTask):
            tick_tasks.append(task)
    if not tick_tasks:
        return ()

    streams = load_streams(project_path)
    nested: list[NestedTickDependency] = []
    for task in tick_tasks:
        tick_artifacts = stream_tick_artifacts(task.stream, streams)
        if tick_artifacts:
            nested.append(
                NestedTickDependency(
                    task=task,
                    tick_artifacts=frozenset(tick_artifacts),
                )
            )
    return tuple(nested)


def stream_tick_artifacts(
    stream_id: str,
    streams: StreamsConfig,
) -> set[str]:
    artifacts: set[str] = set()
    visited: set[str] = set()

    def visit(current_stream_id: str) -> None:
        if current_stream_id in visited:
            return
        visited.add(current_stream_id)
        stream = streams.streams.get(current_stream_id)
        if stream is None:
            return
        for operation in stream.stream:
            if isinstance(operation, EnsureTicksConfig):
                artifacts.add(operation.artifact)
        for input_stream_id in stream.input_streams():
            visit(input_stream_id)

    visit(stream_id)
    return artifacts
