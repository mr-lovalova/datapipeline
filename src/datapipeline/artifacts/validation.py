from dataclasses import dataclass
from pathlib import Path

from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.tasks import TicksTask
from datapipeline.services.bootstrap.core import load_streams
from datapipeline.transforms.spec import TransformSpec
from datapipeline.utils.time import parse_timecode


@dataclass(frozen=True)
class NestedTickDependency:
    task: TicksTask
    cadence_artifacts: frozenset[str]


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
        artifacts = ", ".join(sorted(dependency.cadence_artifacts))
        raise ValueError(
            f"Tick artifact '{dependency.task.id}' source stream "
            f"'{dependency.task.stream}' uses "
            f"artifact-backed cadence(s): {artifacts}. Nested tick artifact "
            "dependencies are not supported."
        )


def nested_tick_dependencies(
    project_path: Path,
    graph: ArtifactGraph,
    artifact_keys: set[str],
) -> tuple[NestedTickDependency, ...]:
    tick_tasks: list[TicksTask] = []
    for key in graph.topological_order(artifact_keys):
        task = graph.tasks_by_id.get(graph.definition(key).task_id)
        if isinstance(task, TicksTask):
            tick_tasks.append(task)
    if not tick_tasks:
        return ()

    streams = load_streams(project_path)
    nested: list[NestedTickDependency] = []
    for task in tick_tasks:
        cadence_artifacts = _stream_cadence_artifacts(task.stream, streams)
        if cadence_artifacts:
            nested.append(
                NestedTickDependency(
                    task=task,
                    cadence_artifacts=frozenset(cadence_artifacts),
                )
            )
    return tuple(nested)


def _stream_cadence_artifacts(
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
        for operation in stream.stream or ():
            cadence = _artifact_cadence(operation)
            if cadence is not None:
                artifacts.add(cadence)
        for input_stream_id in stream.input_refs().values():
            visit(input_stream_id)

    visit(stream_id)
    return artifacts


def _artifact_cadence(operation: TransformSpec) -> str | None:
    if operation.name != "ensure_cadence":
        return None
    cadence = operation.params.get("cadence")
    if not isinstance(cadence, str) or not cadence.strip():
        return None
    try:
        parse_timecode(cadence)
    except ValueError:
        return cadence
    return None


__all__ = [
    "NestedTickDependency",
    "nested_tick_dependencies",
    "validate_artifact_plan",
]
