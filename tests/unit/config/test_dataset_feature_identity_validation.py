import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.config.dataset.split import DatasetFold, HashSplitConfig
from datapipeline.config.streams import SourceStreamConfig, StreamsConfig
from datapipeline.services.dataset import validate_dataset_streams


def _streams(partition_by: list[str]) -> StreamsConfig:
    stream = SourceStreamConfig.model_validate(
        {
            "id": "prices",
            "from": {"source": "raw"},
            "map": {"entrypoint": "identity"},
            "partition_by": partition_by,
        }
    )
    return StreamsConfig(streams={"prices": stream})


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


def test_dataset_accepts_long_identity() -> None:
    streams = _streams(["exchange", "security_id"])
    dataset = _dataset(sample_keys=["exchange", "security_id"])

    validate_dataset_streams(dataset, streams)


def test_dataset_accepts_wide_identity() -> None:
    validate_dataset_streams(_dataset(), _streams(["security_id"]))


def test_dataset_accepts_hybrid_identity() -> None:
    streams = _streams(["exchange", "security_id"])
    dataset = _dataset(sample_keys=["exchange"])

    validate_dataset_streams(dataset, streams)


def test_dataset_rejects_sample_key_outside_stream_partition() -> None:
    streams = _streams(["security_id"])
    dataset = _dataset(sample_keys=["exchange"])

    with pytest.raises(
        ValueError,
        match=r"sample\.keys \['exchange'\].*partition_by \['security_id'\]",
    ):
        validate_dataset_streams(dataset, streams)


def test_dataset_accepts_unpartitioned_scalar_feature() -> None:
    validate_dataset_streams(_dataset(), _streams([]))


def test_dataset_accepts_hybrid_sequence_identity() -> None:
    streams = _streams(["exchange", "security_id"])
    dataset = _dataset(
        sample_keys=["exchange"],
        sequence=SequenceConfig(size=2),
    )

    validate_dataset_streams(dataset, streams)


def test_dataset_rejects_sequences_with_hash_split() -> None:
    feature = FeatureRecordConfig(
        stream="prices",
        id="close",
        field="close",
        sequence=SequenceConfig(size=2),
    )

    with pytest.raises(ValueError, match="hash splits cannot be used.*close"):
        FeatureDatasetConfig(
            sample=SampleConfig(cadence="1d"),
            features=[feature],
            split=HashSplitConfig(
                ratios={"train": 1.0},
                folds=[DatasetFold(id="default", train=["train"])],
            ),
        )
