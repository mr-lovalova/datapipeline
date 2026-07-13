from pathlib import Path

import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.runtime import IngestRuntimeStream, Runtime


class _EmptySource:
    def stream(self):
        return iter(())


def _identity(records):
    return records


def _runtime(tmp_path: Path) -> Runtime:
    return Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )


def test_partitioned_dataset_feature_requires_explicit_feature_id_by(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("security_id",),
        feature_id_by=None,
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1d"), features=[cfg])

    with pytest.raises(ValueError, match="Set feature_id_by"):
        validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_requires_complete_identity(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("security_id",),
        feature_id_by=(),
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1d"), features=[cfg])

    with pytest.raises(ValueError, match=r"security_id.*sample\.keys"):
        validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_accepts_scalar_id_keyed_by_partition(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("exchange", "security_id"),
        feature_id_by=(),
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(
            cadence="1d",
            keys=["security_id", "exchange"],
        ),
        features=[cfg],
    )

    validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_rejects_partially_keyed_scalar_id(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("exchange", "security_id"),
        feature_id_by=(),
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1d", keys=["security_id"]),
        features=[cfg],
    )

    with pytest.raises(ValueError, match="exchange"):
        validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_accepts_wide_feature_identity(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("security_id",),
        feature_id_by=("security_id",),
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1d"), features=[cfg])

    validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_rejects_partially_suffixed_identity(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("exchange", "security_id"),
        feature_id_by=("security_id",),
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1d"), features=[cfg])

    with pytest.raises(ValueError, match="exchange"):
        validate_dataset_feature_identity(runtime, dataset)


def test_partitioned_dataset_feature_accepts_identity_split_across_sample_and_feature(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=("exchange", "security_id"),
        feature_id_by=("security_id",),
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="prices", id="close", field="close")
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1d", keys=["exchange"]),
        features=[cfg],
    )

    validate_dataset_feature_identity(runtime, dataset)


def test_unpartitioned_dataset_feature_does_not_require_feature_id_by(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.streams["market"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=(),
        feature_id_by=None,
        presorted=False,
    )
    cfg = FeatureRecordConfig(stream="market", id="spy_close", field="close")
    dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1d"), features=[cfg])

    validate_dataset_feature_identity(runtime, dataset)
