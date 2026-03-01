import tomllib
from pathlib import Path

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
import pytest

from datapipeline.artifacts.specs import (
    ARTIFACT_DEFINITIONS,
    ArtifactDefinition,
    StageDemand,
    artifact_definitions_with_task_dependencies,
    artifact_build_order,
    artifact_keys_for_task_ids,
    required_artifacts_for,
)
from datapipeline.config.tasks import (
    MetadataTask,
    ScalerTask,
    SchemaTask,
    StatsTask,
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


def _dataset(scale: bool = False) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                record_stream="records",
                id="temp",
                field="value",
                scale=scale,
            )
        ],
        targets=[],
    )


def test_metadata_not_required_for_preview_stages():
    required = required_artifacts_for(_dataset(), [StageDemand(stage=2)])

    assert required == set()
    assert VECTOR_SCHEMA_METADATA not in required


def test_scaler_but_not_metadata_for_transform_stage():
    required = required_artifacts_for(_dataset(scale=True), [StageDemand(stage=6)])

    assert SCALER_STATISTICS in required
    assert VECTOR_SCHEMA not in required
    assert VECTOR_SCHEMA_METADATA not in required


def test_metadata_not_required_for_feature_preview_tail_stage():
    required = required_artifacts_for(_dataset(), [StageDemand(stage=7)])

    assert VECTOR_SCHEMA not in required
    assert VECTOR_SCHEMA_METADATA not in required
    assert SCALER_STATISTICS not in required


def test_metadata_required_for_full_pipeline_run():
    required = required_artifacts_for(_dataset(), [StageDemand(stage=None)])

    assert VECTOR_SCHEMA in required
    assert VECTOR_SCHEMA_METADATA in required
    assert SCALER_STATISTICS not in required
    assert VECTOR_STATS not in required


def test_artifact_build_order_resolves_dependencies():
    definitions = artifact_definitions_with_task_dependencies(
        [
            SchemaTask(id="schema"),
            MetadataTask(id="metadata", dependencies=["schema"]),
            ScalerTask(id="scaler"),
        ]
    )
    ordered = artifact_build_order(
        {VECTOR_SCHEMA_METADATA, SCALER_STATISTICS},
        definitions=definitions,
    )
    assert ordered == [VECTOR_SCHEMA, VECTOR_SCHEMA_METADATA, SCALER_STATISTICS]


def test_artifact_keys_for_task_ids():
    keys = artifact_keys_for_task_ids({"schema", "scaler", "stats"})
    assert keys == {VECTOR_SCHEMA, SCALER_STATISTICS, VECTOR_STATS}


def test_artifact_build_order_detects_cycles():
    specs = (
        ArtifactDefinition(key="a", task_id="a", min_stage=0, dependencies=("b",)),
        ArtifactDefinition(key="b", task_id="b", min_stage=0, dependencies=("a",)),
    )
    with pytest.raises(ValueError, match="dependency cycle"):
        artifact_build_order({"a"}, definitions=specs)


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
