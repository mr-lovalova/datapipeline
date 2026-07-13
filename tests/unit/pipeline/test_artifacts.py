from pathlib import Path
import tomllib

import pytest

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
    stream_tick_artifacts,
    validate_artifact_plan,
)
from datapipeline.build.state import ArtifactFileFingerprint, BuildState
from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.config.tasks import (
    ArtifactTask,
    CoverageTask,
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
    VECTOR_METADATA,
    VECTOR_SCHEMA,
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
            ArtifactDefinition(key="result", dependencies=("input",)),
            ArtifactDefinition(key="input"),
        ),
        {},
    )

    assert graph.topological_order({"result", "input"}) == ("input", "result")


def test_artifact_keys_match_task_ids():
    graph = build_artifact_graph(
        [
            SchemaTask(id="schema"),
            ScalerTask(id="scaler"),
            StatsTask(id="stats", mode="raw"),
            VectorInputsTask(id="vector_inputs"),
        ]
    )

    assert graph.declared_artifact_keys() == {
        VECTOR_SCHEMA,
        SCALER_STATISTICS,
        VECTOR_STATS,
        VECTOR_INPUTS,
    }


def test_raw_stats_build_selects_metadata_dependency_chain():
    graph = build_artifact_graph(
        [
            StatsTask(id="stats", mode="raw"),
            MetadataTask(id="metadata"),
            VectorInputsTask(id="vector_inputs"),
            ScalerTask(id="scaler"),
        ]
    )

    keys = set(graph.dependency_closure({"stats"}))

    assert keys == {
        VECTOR_STATS,
        VECTOR_METADATA,
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

    keys = set(graph.dependency_closure({"stats"}))

    assert keys == {
        VECTOR_STATS,
        VECTOR_METADATA,
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

    assert graph.declared_artifact_keys() == {"dataset_ticks"}


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
        VECTOR_METADATA,
        VECTOR_STATS,
    }


def test_tick_artifact_rejects_nested_tick_artifact_in_any_upstream_stream(
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
                            "operation": "ensure_ticks",
                            "artifact": "base_ticks",
                        }
                    ],
                },
                "derived": {
                    "id": "derived",
                    "from": {"align": ["base", "duration"]},
                    "combine": {"entrypoint": "combine"},
                },
                "duration": {
                    "id": "duration",
                    "from": {"stream": "raw"},
                    "stream": [
                        {
                            "operation": "ensure_cadence",
                            "cadence": "1d",
                        }
                    ],
                },
            }
        }
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.validation.load_streams",
        lambda _project_path: streams,
    )

    assert stream_tick_artifacts("derived", streams) == {"base_ticks"}

    dependencies = nested_tick_dependencies(
        tmp_path / "project.yaml",
        graph,
        {"derived_ticks"},
    )
    assert len(dependencies) == 1
    assert dependencies[0].task is tick_task
    assert dependencies[0].tick_artifacts == {"base_ticks"}

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
                            "operation": "ensure_cadence",
                            "cadence": "1h",
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
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"), features=[], targets=[]
    )

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

    assert graph.dependency_closure({"custom_snapshot"}) == ("custom_snapshot",)
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
                    dependencies=("missing",),
                ),
            ),
            {},
        )


def test_artifact_graph_rejects_task_without_matching_definition():
    task = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="snapshot.json",
    )

    with pytest.raises(ValueError, match="has no matching artifact definition"):
        ArtifactGraph((), {task.id: task})


def test_artifact_graph_rejects_task_mapping_key_that_differs_from_id():
    task = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="snapshot.json",
    )

    with pytest.raises(ValueError, match="does not match task id 'snapshot'"):
        ArtifactGraph((ArtifactDefinition(key="wrong"),), {"wrong": task})


def test_artifact_graph_rejects_cycles_with_path():
    with pytest.raises(ValueError, match="first -> second -> first"):
        ArtifactGraph(
            (
                ArtifactDefinition(
                    key="first",
                    dependencies=("second",),
                ),
                ArtifactDefinition(
                    key="second",
                    dependencies=("first",),
                ),
            ),
            {},
        )


def test_artifact_graph_rejects_unknown_requested_artifact():
    graph = build_artifact_graph([])

    with pytest.raises(ValueError, match="Unknown artifact 'missing'"):
        graph.dependency_closure({"missing"})


def test_stale_dependency_makes_current_dependent_outdated(tmp_path):
    graph = ArtifactGraph(
        (
            ArtifactDefinition(key="input"),
            ArtifactDefinition(
                key="result",
                dependencies=("input",),
            ),
        ),
        {},
    )
    (tmp_path / "input.json").write_text("{}", encoding="utf-8")
    (tmp_path / "result.json").write_text("{}", encoding="utf-8")
    state = BuildState()
    state.register(
        "input",
        "input.json",
        config_hash="old",
        files=(
            ArtifactFileFingerprint.from_path(
                "input.json",
                tmp_path / "input.json",
            ),
        ),
    )
    state.register(
        "result",
        "result.json",
        config_hash="current",
        files=(
            ArtifactFileFingerprint.from_path(
                "result.json",
                tmp_path / "result.json",
            ),
        ),
    )

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
        (ArtifactDefinition(key="result"),),
        {},
    )
    state = BuildState()
    state.register(
        "result",
        "missing.json",
        config_hash="current",
        files=(
            ArtifactFileFingerprint(
                relative_path="missing.json",
                size=0,
                mtime_ns=0,
            ),
        ),
    )

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
    state = BuildState()
    state.register(
        "snapshot",
        "legacy.json",
        config_hash="current",
        files=(
            ArtifactFileFingerprint.from_path(
                "legacy.json",
                tmp_path / "legacy.json",
            ),
        ),
    )

    freshness = graph.freshness(
        keys={"snapshot"},
        state=state,
        config_hash="current",
        artifacts_root=tmp_path,
    )

    assert freshness.stale == {"snapshot"}
    assert freshness.outdated == {"snapshot"}


@pytest.mark.parametrize("change", ["remove", "alter"])
def test_artifact_companion_changes_affect_freshness(tmp_path, change):
    graph = ArtifactGraph((ArtifactDefinition(key="bundle"),), {})
    primary = tmp_path / "manifest.json"
    companion = tmp_path / "manifest.shards/000000.jsonl.gz"
    primary.write_text("{}", encoding="utf-8")
    companion.parent.mkdir()
    companion.write_bytes(b"original")
    files = (
        ArtifactFileFingerprint.from_path("manifest.json", primary),
        ArtifactFileFingerprint.from_path(
            "manifest.shards/000000.jsonl.gz",
            companion,
        ),
    )
    state = BuildState()
    state.register(
        "bundle",
        "manifest.json",
        config_hash="current",
        files=files,
    )

    if change == "remove":
        companion.unlink()
    else:
        companion.write_bytes(b"altered")

    freshness = graph.freshness(
        keys={"bundle"},
        state=state,
        config_hash="current",
        artifacts_root=tmp_path,
    )

    expected = freshness.missing if change == "remove" else freshness.stale
    assert expected == {"bundle"}
    assert freshness.outdated == {"bundle"}


@pytest.mark.parametrize(
    ("preview", "expected"),
    [
        (None, {VECTOR_METADATA, VECTOR_SCHEMA}),
        ("source", set()),
        ("mapped", set()),
        ("records", set()),
        ("features", {SCALER_STATISTICS}),
        ("samples", {VECTOR_METADATA}),
        ("postprocess", {VECTOR_METADATA, VECTOR_SCHEMA}),
    ],
)
def test_pipeline_runtime_requirements_follow_preview_stage(
    preview,
    expected,
):
    graph = build_artifact_graph([])
    task = PipelineTask(id="pipeline")

    assert (
        graph.runtime_requirements(
            task,
            preview=preview,
        )
        == expected
    )


@pytest.mark.parametrize("preview", ["source", "mapped", "records", "features"])
def test_record_and_feature_previews_require_declared_ticks(
    preview: PreviewStage,
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
        preview=preview,
    )


def test_invalid_pipeline_preview_is_rejected_for_empty_dataset() -> None:
    graph = build_artifact_graph([])
    task = PipelineTask(id="pipeline")
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1h"))

    with pytest.raises(ValueError, match="preview must be one of"):
        graph.runtime_dependency_closure(
            task,
            preview="unknown",  # type: ignore[arg-type]
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
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"), features=[], targets=[]
    )

    assert graph.runtime_dependency_closure(
        task,
        preview=None,
        dataset=dataset,
    ) == (VECTOR_INPUTS, VECTOR_METADATA, VECTOR_STATS)


@pytest.mark.parametrize(
    ("scale", "expected"),
    [
        (False, False),
        (True, True),
    ],
)
def test_dataset_scaler_requirement_matches_feature_config(scale, expected):
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
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
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1h"))

    assert (
        graph.runtime_dependency_closure(
            task,
            preview=None,
            dataset=dataset,
        )
        == ()
    )
    assert (
        graph.runtime_dependency_closure(
            task,
            preview="features",
            dataset=dataset,
        )
        == ()
    )


def test_custom_runtime_task_has_no_inferred_artifact_dependencies():
    graph = build_artifact_graph([])
    task = OperationTask(id="pipeline", entrypoint="plugin.runtime.pipeline")

    assert graph.runtime_requirements(task, preview=None) == set()


def test_custom_runtime_task_uses_declared_artifact_dependencies():
    snapshot = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph([snapshot])
    task = OperationTask(
        id="report",
        entrypoint="plugin.runtime.report",
        requires=("custom_snapshot",),
    )

    assert graph.runtime_dependency_closure(
        task,
        preview=None,
        dataset=None,
    ) == ("custom_snapshot",)


def test_empty_pipeline_dataset_keeps_explicit_artifact_dependencies():
    snapshot = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph([snapshot])
    task = PipelineTask(id="pipeline", requires=("custom_snapshot",))

    assert graph.runtime_dependency_closure(
        task,
        preview=None,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    ) == ("custom_snapshot",)


def test_runtime_task_rejects_unknown_declared_artifact_dependency():
    graph = build_artifact_graph([])
    task = OperationTask(
        id="report",
        entrypoint="plugin.runtime.report",
        requires=("missing",),
    )

    with pytest.raises(ValueError, match="Unknown artifact 'missing'"):
        graph.runtime_dependency_closure(
            task,
            preview=None,
            dataset=None,
        )


def test_runtime_task_rejects_inactive_declared_artifact_dependency():
    graph = build_artifact_graph([ScalerTask(id="scaler")])
    task = OperationTask(
        id="report",
        entrypoint="plugin.runtime.report",
        requires=("scaler",),
    )

    with pytest.raises(ValueError, match="inactive for this dataset: scaler"):
        graph.runtime_dependency_closure(
            task,
            preview=None,
            dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
        )


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
        task = task_by_id[definition.key]
        assert task.entrypoint in declared


def test_ticks_entrypoint_is_declared():
    declared = _declared_entrypoints(BUILD_OPERATIONS_EP)
    assert "core.artifact.ticks" in declared
