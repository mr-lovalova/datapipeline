from pathlib import Path

import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.runtime import Runtime


def _runtime(tmp_path: Path) -> Runtime:
    return Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )


def test_partitioned_dataset_feature_requires_explicit_feature_id_by(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.registries.partition_by.register("prices", "security_id")
    runtime.registries.feature_id_by.register("prices", None)
    cfg = FeatureRecordConfig(record_stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(group_by="1d", features=[cfg])

    with pytest.raises(ValueError, match="Set feature_id_by"):
        validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_accepts_scalar_feature_id(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.registries.partition_by.register("prices", "security_id")
    runtime.registries.feature_id_by.register("prices", [])
    cfg = FeatureRecordConfig(record_stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(group_by="1d", features=[cfg])

    validate_dataset_feature_identity(runtime, dataset)


def test_unpartitioned_dataset_feature_does_not_require_feature_id_by(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.registries.partition_by.register("market", None)
    runtime.registries.feature_id_by.register("market", None)
    cfg = FeatureRecordConfig(record_stream="market", id="spy_close", field="close")
    dataset = FeatureDatasetConfig(group_by="1d", features=[cfg])

    validate_dataset_feature_identity(runtime, dataset)
