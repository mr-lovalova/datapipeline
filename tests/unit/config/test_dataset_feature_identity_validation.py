import pytest

from datapipeline.config.catalog import IngestConfig, StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.services.dataset import validate_dataset_streams


def _streams(
    partition_by: list[str],
    feature_id_by: list[str] | None,
) -> StreamsConfig:
    ingest = IngestConfig.model_validate(
        {
            "id": "prices",
            "from": {"source": "raw"},
            "map": {"entrypoint": "identity"},
            "partition_by": partition_by,
            "feature_id_by": feature_id_by,
        }
    )
    return StreamsConfig(ingests={"prices": ingest})


def _dataset(
    sample_keys: list[str] | None = None,
    stream: str = "prices",
    sequence: SequenceConfig | None = None,
) -> FeatureDatasetConfig:
    feature = FeatureRecordConfig(
        stream=stream,
        id="close",
        field="close",
        sequence=sequence,
    )
    return FeatureDatasetConfig(
        sample=SampleConfig(cadence="1d", keys=sample_keys or []),
        features=[feature],
    )


def test_dataset_feature_must_reference_known_stream() -> None:
    dataset = _dataset(stream="missing")

    with pytest.raises(ValueError, match="references unknown stream 'missing'"):
        validate_dataset_streams(dataset, StreamsConfig())


def test_partitioned_dataset_feature_requires_explicit_feature_id_by() -> None:
    streams = _streams(["security_id"], None)

    with pytest.raises(ValueError, match="Set feature_id_by"):
        validate_dataset_streams(_dataset(), streams)


def test_partitioned_dataset_feature_requires_complete_identity() -> None:
    streams = _streams(["security_id"], [])

    with pytest.raises(ValueError, match=r"security_id.*sample\.keys"):
        validate_dataset_streams(_dataset(), streams)


def test_partitioned_dataset_feature_accepts_scalar_id_keyed_by_partition() -> None:
    streams = _streams(["exchange", "security_id"], [])
    dataset = _dataset(sample_keys=["security_id", "exchange"])

    validate_dataset_streams(dataset, streams)


def test_partitioned_dataset_feature_rejects_partially_keyed_scalar_id() -> None:
    streams = _streams(["exchange", "security_id"], [])
    dataset = _dataset(sample_keys=["security_id"])

    with pytest.raises(ValueError, match="exchange"):
        validate_dataset_streams(dataset, streams)


def test_partitioned_dataset_feature_accepts_wide_feature_identity() -> None:
    streams = _streams(["security_id"], ["security_id"])

    validate_dataset_streams(_dataset(), streams)


def test_partitioned_dataset_feature_rejects_partially_suffixed_identity() -> None:
    streams = _streams(["exchange", "security_id"], ["security_id"])

    with pytest.raises(ValueError, match="exchange"):
        validate_dataset_streams(_dataset(), streams)


def test_partitioned_dataset_feature_accepts_split_identity() -> None:
    streams = _streams(["exchange", "security_id"], ["security_id"])
    dataset = _dataset(sample_keys=["exchange"])

    validate_dataset_streams(dataset, streams)


def test_unpartitioned_dataset_feature_does_not_require_feature_id_by() -> None:
    streams = _streams([], None)

    validate_dataset_streams(_dataset(), streams)


def test_sequenced_feature_identity_must_match_stream_partition() -> None:
    streams = _streams(["security_id"], ["security_id"])
    dataset = _dataset(
        sample_keys=["exchange"],
        sequence=SequenceConfig(size=2),
    )

    with pytest.raises(ValueError, match="match.*partition_by exactly"):
        validate_dataset_streams(dataset, streams)


def test_sequenced_feature_accepts_bounded_partition_identity() -> None:
    streams = _streams(["exchange", "security_id"], ["security_id"])
    dataset = _dataset(
        sample_keys=["exchange"],
        sequence=SequenceConfig(size=2),
    )

    validate_dataset_streams(dataset, streams)
