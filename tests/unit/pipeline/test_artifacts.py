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
    dataset_requires_scaler,
)
from datapipeline.artifacts.validation import (
    nested_tick_dependencies,
    validate_artifact_plan,
)
from datapipeline.build.state import BuildState
from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import (
    ArtifactTask,
    CoverageTask,
    MaterializeStreamTask,
    MetadataTask,
    OperationTask,
    PipelineTask,
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


def test_tick_artifacts_feed_scaler_and_vector_inputs() -> None:
    tick_task = TicksTask(
        id="dataset_ticks",
        stream="reference.stream",
        output="build/dataset_ticks.jsonl",
    )
    graph = build_artifact_graph(
        [
            tick_task,
            ScalerTask(id="scaler"),
            VectorInputsTask(id="vector_inputs"),
        ]
    )

    assert graph.definition(SCALER_STATISTICS).dependencies == ("dataset_ticks",)
    assert graph.definition(VECTOR_INPUTS).dependencies == (
        SCALER_STATISTICS,
        "dataset_ticks",
    )
    assert graph.dependents_of({"dataset_ticks"}) == {
        SCALER_STATISTICS,
        VECTOR_INPUTS,
        VECTOR_SCHEMA,
        VECTOR_SCHEMA_METADATA,
        VECTOR_STATS,
    }


def test_tick_artifact_rejects_artifact_cadence_in_upstream_stream(
    monkeypatch,
    tmp_path,
) -> None:
    tick_task = TicksTask(
        id="derived_ticks",
        stream="derived",
        output="build/derived-ticks.jsonl",
    )
    graph = build_artifact_graph([tick_task])
    streams = StreamsConfig.model_validate(
        {
            "streams": {
                "base": {
                    "id": "base",
                    "from": {"stream": "raw"},
                    "stream": [
                        {
                            "ensure_cadence": {
                                "field": "value",
                                "cadence": "base_ticks",
                            }
                        }
                    ],
                },
                "derived": {
                    "id": "derived",
                    "from": {"stream": "base"},
                },
            }
        }
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.validation.load_streams",
        lambda _project_path: streams,
    )

    dependencies = nested_tick_dependencies(
        tmp_path / "project.yaml",
        graph,
        {"derived_ticks"},
    )
    assert len(dependencies) == 1
    assert dependencies[0].task is tick_task
    assert dependencies[0].cadence_artifacts == {"base_ticks"}

    with pytest.raises(ValueError, match="Nested tick artifact dependencies"):
        validate_artifact_plan(tmp_path / "project.yaml", graph, {"derived_ticks"})


def test_tick_artifact_allows_duration_cadence(monkeypatch, tmp_path) -> None:
    tick_task = TicksTask(
        id="hourly_ticks",
        stream="hourly",
        output="build/hourly-ticks.jsonl",
    )
    graph = build_artifact_graph([tick_task])
    streams = StreamsConfig.model_validate(
        {
            "streams": {
                "hourly": {
                    "id": "hourly",
                    "from": {"stream": "raw"},
                    "stream": [
                        {
                            "ensure_cadence": {
                                "field": "value",
                                "cadence": "1h",
                            }
                        }
                    ],
                }
            }
        }
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.validation.load_streams",
        lambda _project_path: streams,
    )

    validate_artifact_plan(tmp_path / "project.yaml", graph, {"hourly_ticks"})


def test_inactive_scaler_prunes_its_tick_dependency() -> None:
    tick_task = TicksTask(
        id="dataset_ticks",
        stream="reference.stream",
        output="build/dataset_ticks.jsonl",
    )
    graph = build_artifact_graph([tick_task, ScalerTask(id="scaler")])
    dataset = FeatureDatasetConfig(group_by="1h", features=[], targets=[])

    assert (
        graph.active_dependency_closure(
            {SCALER_STATISTICS},
            dataset,
        )
        == ()
    )
    assert graph.active_dependency_closure(
        {SCALER_STATISTICS, "dataset_ticks"},
        dataset,
    ) == ("dataset_ticks",)


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


def test_artifact_at_path_other_than_declared_output_is_stale(tmp_path):
    task = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="declared.json",
    )
    graph = build_artifact_graph([task])
    (tmp_path / "legacy.json").write_text("{}", encoding="utf-8")
    state = BuildState(config_hash="current")
    state.register(
        "snapshot",
        "legacy.json",
        meta={"_config_hash": "current"},
    )

    freshness = graph.freshness(
        keys={"snapshot"},
        state=state,
        config_hash="current",
        artifacts_root=tmp_path,
    )

    assert freshness.stale == {"snapshot"}
    assert freshness.outdated == {"snapshot"}


@pytest.mark.parametrize(
    ("preview_index", "expected"),
    [
        (None, {VECTOR_SCHEMA_METADATA, VECTOR_SCHEMA}),
        (0, set()),
        (9, set()),
        (10, {SCALER_STATISTICS}),
        (11, {SCALER_STATISTICS}),
        (12, {VECTOR_SCHEMA_METADATA}),
        (13, {VECTOR_SCHEMA_METADATA, VECTOR_SCHEMA}),
        (14, {VECTOR_SCHEMA_METADATA, VECTOR_SCHEMA}),
    ],
)
def test_pipeline_runtime_requirements_follow_preview_stage(
    preview_index,
    expected,
):
    graph = build_artifact_graph([])
    task = PipelineTask(id="pipeline")

    assert (
        graph.runtime_requirements(
            task,
            preview_index=preview_index,
        )
        == expected
    )


@pytest.mark.parametrize("preview_index", [7, 8, 9, 10, 11])
def test_pipeline_stream_and_feature_previews_require_declared_ticks(
    preview_index: int,
) -> None:
    tick_task = TicksTask(
        id="dataset_ticks",
        stream="reference.stream",
        output="build/dataset_ticks.jsonl",
    )
    graph = build_artifact_graph([tick_task])
    task = PipelineTask(id="pipeline")

    assert "dataset_ticks" in graph.runtime_requirements(
        task,
        preview_index=preview_index,
    )


def test_runtime_stream_materialization_requires_declared_ticks() -> None:
    tick_task = TicksTask(
        id="dataset_ticks",
        stream="reference.stream",
        output="build/dataset_ticks.jsonl",
    )
    graph = build_artifact_graph([tick_task])
    task = MaterializeStreamTask(
        id="materialize",
        options={"stream": "derived.stream"},
    )

    assert graph.runtime_requirements(task, preview_index=None) == {"dataset_ticks"}


def test_invalid_pipeline_preview_is_rejected_for_empty_dataset() -> None:
    graph = build_artifact_graph([])
    task = PipelineTask(id="pipeline")
    dataset = FeatureDatasetConfig(group_by="1h")

    with pytest.raises(ValueError, match="preview_index must be between 0 and 14"):
        graph.runtime_dependency_closure(
            task,
            preview_index=15,
            dataset=dataset,
        )


def test_runtime_dependency_closure_uses_stats_task_mode():
    graph = build_artifact_graph(
        [
            VectorInputsTask(id="vector_inputs"),
            MetadataTask(id="metadata"),
            StatsTask(id="stats", mode="raw"),
        ]
    )
    task = CoverageTask(id="coverage")
    dataset = FeatureDatasetConfig(group_by="1h", features=[], targets=[])

    assert graph.runtime_dependency_closure(
        task,
        preview_index=None,
        dataset=dataset,
    ) == (VECTOR_INPUTS, VECTOR_SCHEMA_METADATA, VECTOR_STATS)


def test_external_scaler_model_does_not_require_managed_scaler_artifact():
    graph = build_artifact_graph(
        [
            VectorInputsTask(id="vector_inputs"),
            MetadataTask(id="metadata"),
            SchemaTask(id="schema"),
        ]
    )
    task = PipelineTask(id="pipeline")
    dataset = FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                id="price",
                record_stream="prices",
                field="close",
                scale={"model_path": "models/price-scaler.json"},
            )
        ],
    )

    assert (
        graph.runtime_dependency_closure(
            task,
            preview_index=10,
            dataset=dataset,
        )
        == ()
    )
    assert graph.runtime_dependency_closure(
        task,
        preview_index=None,
        dataset=dataset,
    ) == (VECTOR_INPUTS, VECTOR_SCHEMA, VECTOR_SCHEMA_METADATA)


@pytest.mark.parametrize(
    ("scale", "expected"),
    [
        (False, False),
        ({}, False),
        ({"model_path": "models/price-scaler.json"}, False),
        (True, True),
        ({"method": "standard"}, True),
    ],
)
def test_dataset_scaler_requirement_matches_transform_behavior(scale, expected):
    dataset = FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                id="price",
                record_stream="prices",
                field="close",
                scale=scale,
            )
        ],
    )

    assert dataset_requires_scaler(dataset) is expected


def test_empty_pipeline_dataset_has_no_runtime_artifact_requirements():
    graph = build_artifact_graph([])
    task = PipelineTask(id="pipeline")
    dataset = FeatureDatasetConfig(group_by="1h")

    assert (
        graph.runtime_dependency_closure(
            task,
            preview_index=None,
            dataset=dataset,
        )
        == ()
    )
    assert (
        graph.runtime_dependency_closure(
            task,
            preview_index=10,
            dataset=dataset,
        )
        == ()
    )


def test_custom_runtime_task_has_no_inferred_artifact_dependencies():
    graph = build_artifact_graph([])
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime.pipeline")

    assert graph.runtime_requirements(task, preview_index=None) == set()


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
