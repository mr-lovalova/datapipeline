from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from datapipeline.artifacts.planning import (
    ArtifactGraph,
    build_artifact_graph,
)
from datapipeline.artifacts.specs import (
    ARTIFACT_DEFINITIONS,
    ArtifactDefinition,
)
from datapipeline.build.state import BuildState
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    ScalerTask,
    SchemaTask,
    StatsTask,
    TicksTask,
    VectorInputsTask,
)
from datapipeline.plugins import BUILD_OPERATIONS_EP
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
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


def test_artifact_graph_uses_dependency_order_not_declaration_order():
    graph = ArtifactGraph(
        (
            ArtifactDefinition(key="result", task_id="result", dependencies=("input",)),
            ArtifactDefinition(key="input", task_id="input"),
        ),
        {},
    )

    assert graph.topological_order({"result", "input"}) == ("input", "result")


def test_artifact_keys_for_task_ids():
    graph = build_artifact_graph(
        [
            SchemaTask(id="schema"),
            ScalerTask(id="scaler"),
            StatsTask(id="stats", mode="raw"),
            VectorInputsTask(id="vector_inputs"),
        ]
    )

    keys = graph.keys_for_tasks({"schema", "scaler", "stats", "vector_inputs"})

    assert keys == {VECTOR_SCHEMA, SCALER_STATISTICS, VECTOR_STATS, VECTOR_INPUTS}


def test_raw_stats_build_selects_metadata_dependency_chain():
    graph = build_artifact_graph(
        [
            StatsTask(id="stats", mode="raw"),
            MetadataTask(id="metadata"),
            VectorInputsTask(id="vector_inputs"),
            ScalerTask(id="scaler"),
        ]
    )

    keys = graph.select_keys(
        profile_target="stats",
        profile_name="stats",
    )

    assert keys == {
        VECTOR_STATS,
        VECTOR_SCHEMA_METADATA,
        VECTOR_INPUTS,
        SCALER_STATISTICS,
    }


def test_final_stats_build_also_selects_schema():
    graph = build_artifact_graph(
        [
            StatsTask(id="stats", mode="final"),
            MetadataTask(id="metadata"),
            SchemaTask(id="schema"),
            VectorInputsTask(id="vector_inputs"),
            ScalerTask(id="scaler"),
        ]
    )

    keys = graph.select_keys(profile_target="stats", profile_name="stats")

    assert keys == {
        VECTOR_STATS,
        VECTOR_SCHEMA_METADATA,
        VECTOR_SCHEMA,
        VECTOR_INPUTS,
        SCALER_STATISTICS,
    }


def test_ticks_task_uses_task_id_as_artifact_key():
    graph = build_artifact_graph(
        [
            TicksTask(
                id="dataset_ticks",
                entrypoint="core.artifact.ticks",
                stream="reference.stream",
                output="build/dataset_ticks.jsonl",
            )
        ]
    )

    keys = graph.keys_for_tasks({"dataset_ticks"})

    assert keys == {"dataset_ticks"}


def test_generic_artifact_task_is_a_dependency_free_leaf():
    graph = build_artifact_graph(
        [
            ArtifactTask(
                id="custom_snapshot",
                entrypoint="plugin.snapshot",
                output="build/custom.json",
            )
        ]
    )

    assert graph.select_keys(
        profile_target="custom_snapshot",
        profile_name="custom",
    ) == {"custom_snapshot"}
    assert graph.definition("custom_snapshot").dependencies == ()


def test_artifact_graph_rejects_duplicate_output_paths():
    with pytest.raises(ValueError, match="write the same output 'build/shared.json'"):
        build_artifact_graph(
            [
                ArtifactTask(
                    id="first",
                    entrypoint="plugin.first",
                    output="build/shared.json",
                ),
                ArtifactTask(
                    id="second",
                    entrypoint="plugin.second",
                    output="build/shared.json",
                ),
            ]
        )


def test_artifact_graph_rejects_unknown_dependencies():
    with pytest.raises(ValueError, match="unknown dependency 'missing'"):
        ArtifactGraph(
            (
                ArtifactDefinition(
                    key="result",
                    task_id="result",
                    dependencies=("missing",),
                ),
            ),
            {},
        )


def test_artifact_graph_rejects_cycles_with_path():
    with pytest.raises(ValueError, match="first -> second -> first"):
        ArtifactGraph(
            (
                ArtifactDefinition(
                    key="first",
                    task_id="first",
                    dependencies=("second",),
                ),
                ArtifactDefinition(
                    key="second",
                    task_id="second",
                    dependencies=("first",),
                ),
            ),
            {},
        )


def test_artifact_graph_rejects_unknown_requested_artifact():
    graph = build_artifact_graph([])

    with pytest.raises(ValueError, match="Unknown artifact 'missing'"):
        graph.select_keys(required_artifacts={"missing"})


def test_stale_dependency_makes_current_dependent_outdated(tmp_path):
    graph = ArtifactGraph(
        (
            ArtifactDefinition(key="input", task_id="input"),
            ArtifactDefinition(
                key="result",
                task_id="result",
                dependencies=("input",),
            ),
        ),
        {},
    )
    (tmp_path / "input.json").write_text("{}", encoding="utf-8")
    (tmp_path / "result.json").write_text("{}", encoding="utf-8")
    state = BuildState(config_hash="current")
    state.register("input", "input.json", meta={"_config_hash": "old"})
    state.register("result", "result.json", meta={"_config_hash": "current"})

    freshness = graph.freshness(
        keys={"input", "result"},
        state=state,
        config_hash="current",
        artifacts_root=tmp_path,
    )

    assert freshness.stale == {"input"}
    assert freshness.outdated == {"input", "result"}


def test_artifact_with_missing_file_is_not_current(tmp_path):
    graph = ArtifactGraph(
        (ArtifactDefinition(key="result", task_id="result"),),
        {},
    )
    state = BuildState(config_hash="current")
    state.register("result", "missing.json", meta={"_config_hash": "current"})

    freshness = graph.freshness(
        keys={"result"},
        state=state,
        config_hash="current",
        artifacts_root=tmp_path,
    )

    assert freshness.missing == {"result"}
    assert freshness.outdated == {"result"}


def test_artifact_definitions_have_runner_bound_entrypoints():
    declared = _declared_entrypoints(BUILD_OPERATIONS_EP)
    task_by_id = {
        "schema": SchemaTask(id="schema"),
        "metadata": MetadataTask(id="metadata"),
        "scaler": ScalerTask(id="scaler"),
        "stats": StatsTask(id="stats", mode="final"),
        "vector_inputs": VectorInputsTask(id="vector_inputs"),
    }
    for definition in ARTIFACT_DEFINITIONS:
        task = task_by_id[definition.task_id]
        assert task.entrypoint in declared


def test_ticks_entrypoint_is_declared():
    declared = _declared_entrypoints(BUILD_OPERATIONS_EP)
    assert "core.artifact.ticks" in declared
