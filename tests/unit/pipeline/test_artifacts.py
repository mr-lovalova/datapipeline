from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
import pytest

from datapipeline.artifacts.specs import (
    ARTIFACT_DEFINITIONS,
    ArtifactDefinition,
    StageDemand,
    artifact_build_order,
    artifact_keys_for_task_kinds,
    required_artifacts_for,
)
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
)


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


def test_metadata_required_once_vectors_needed():
    required = required_artifacts_for(_dataset(), [StageDemand(stage=7)])

    assert VECTOR_SCHEMA in required
    assert VECTOR_SCHEMA_METADATA in required
    assert SCALER_STATISTICS not in required


def test_artifact_build_order_resolves_dependencies():
    ordered = artifact_build_order({VECTOR_SCHEMA_METADATA, SCALER_STATISTICS})
    assert ordered == [VECTOR_SCHEMA, VECTOR_SCHEMA_METADATA, SCALER_STATISTICS]


def test_artifact_keys_for_task_kinds():
    keys = artifact_keys_for_task_kinds({"schema", "scaler"})
    assert keys == {VECTOR_SCHEMA, SCALER_STATISTICS}


def test_artifact_build_order_detects_cycles():
    specs = (
        ArtifactDefinition(key="a", task_kind="a", min_stage=0, dependencies=("b",)),
        ArtifactDefinition(key="b", task_kind="b", min_stage=0, dependencies=("a",)),
    )
    with pytest.raises(ValueError, match="dependency cycle"):
        artifact_build_order({"a"}, definitions=specs)


def test_artifact_definitions_include_build_bindings():
    for definition in ARTIFACT_DEFINITIONS:
        assert definition.task_type is not None
        assert definition.materialize is not None
