import tomllib
from pathlib import Path

from datapipeline.artifacts.planning import build_planning_context
from datapipeline.artifacts.specs import (
    ARTIFACT_DEFINITIONS,
    artifact_build_order,
    artifact_keys_for_task_ids,
)
from datapipeline.config.tasks import (
    MetadataTask,
    ScalerTask,
    SchemaTask,
    StatsTask,
    TicksTask,
)
from datapipeline.plugins import BUILD_OPERATIONS_EP
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
    VECTOR_STATS,
)


def _declared_entrypoints(group: str) -> dict[str, str]:
    repo_root = Path(__file__).resolve().parents[3]
    data = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    project = data.get("project", {})
    entrypoints = project.get("entry-points", {})
    group_values = entrypoints.get(group, {})
    if not isinstance(group_values, dict):
        return {}
    return {
        str(key): str(value)
        for key, value in group_values.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def test_artifact_build_order_follows_definition_precedence():
    ordered = artifact_build_order(
        {VECTOR_SCHEMA_METADATA, SCALER_STATISTICS},
    )
    assert ordered == [VECTOR_SCHEMA_METADATA, SCALER_STATISTICS]


def test_artifact_keys_for_task_ids():
    keys = artifact_keys_for_task_ids({"schema", "scaler", "stats"})
    assert keys == {VECTOR_SCHEMA, SCALER_STATISTICS, VECTOR_STATS}


def test_ticks_task_uses_task_id_as_artifact_key():
    context = build_planning_context(
        [
            TicksTask(
                id="dataset_ticks",
                entrypoint="core.artifact.ticks",
                stream="reference.stream",
                output="build/dataset_ticks.jsonl",
            )
        ]
    )

    keys = artifact_keys_for_task_ids({"dataset_ticks"}, context.definitions)

    assert keys == {"dataset_ticks"}


def test_artifact_definitions_have_runner_bound_entrypoints():
    declared = _declared_entrypoints(BUILD_OPERATIONS_EP)
    task_by_id = {
        "schema": SchemaTask(id="schema"),
        "metadata": MetadataTask(id="metadata"),
        "scaler": ScalerTask(id="scaler"),
        "stats": StatsTask(id="stats", mode="final"),
    }
    for definition in ARTIFACT_DEFINITIONS:
        task = task_by_id[definition.task_id]
        assert task.entrypoint in declared


def test_ticks_entrypoint_is_declared():
    declared = _declared_entrypoints(BUILD_OPERATIONS_EP)
    assert "core.artifact.ticks" in declared
