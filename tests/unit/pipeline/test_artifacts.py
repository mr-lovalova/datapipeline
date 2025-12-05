from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.pipeline.artifacts import StageDemand, required_artifacts_for
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
)


def _dataset(scale: bool = False) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        group_by="1h",
        features=[FeatureRecordConfig(record_stream="records", id="temp", scale=scale)],
        targets=[],
    )


def test_metadata_not_required_for_preview_stages():
    required = required_artifacts_for(_dataset(), [StageDemand(stage=2)])

    assert required == set()
    assert VECTOR_SCHEMA_METADATA not in required


def test_scaler_but_not_metadata_for_transform_stage():
    required = required_artifacts_for(_dataset(scale=True), [StageDemand(stage=5)])

    assert SCALER_STATISTICS in required
    assert VECTOR_SCHEMA not in required
    assert VECTOR_SCHEMA_METADATA not in required


def test_metadata_required_once_vectors_needed():
    required = required_artifacts_for(_dataset(), [StageDemand(stage=6)])

    assert VECTOR_SCHEMA in required
    assert VECTOR_SCHEMA_METADATA in required
    assert SCALER_STATISTICS not in required
