from datetime import datetime, timezone
from dataclasses import replace

import pytest

from datapipeline.artifacts.scaler import (
    StandardScalerArtifact,
    TemporalScalerArtifact,
    load_scaler_artifact,
)
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.config.dataset.split import HashSplitConfig, TimeSplitConfig
from datapipeline.config.tasks import ScalerTask
from datapipeline.domain.record import TemporalRecord
from datapipeline.operations.artifacts.scaler import materialize_scaler_statistics
from datapipeline.runtime import IngestRuntimeStream, Runtime


def _time(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _record(day: int, value: object, other: object | None = None) -> TemporalRecord:
    record = TemporalRecord(time=_time(day))
    record.value = value
    record.other = other
    return record


class _CountingSource:
    def __init__(self, rows=()) -> None:
        self.rows = tuple(rows)
        self.opens = 0
        self.closes = 0

    def stream(self):
        self.opens += 1
        try:
            yield from self.rows
        finally:
            self.closes += 1


def _identity(records):
    return records


def _runtime(
    tmp_path,
    dataset: FeatureDatasetConfig | None = None,
    rows=(),
) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\nartifact_revision: 1\n", encoding="utf-8")
    if dataset is None:
        dataset = FeatureDatasetConfig(sample=SampleConfig(cadence="1h"))
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=dataset,
    )
    runtime.streams["stream"] = IngestRuntimeStream(
        source=_CountingSource(rows),
        mapper=_identity,
        transforms=(),
        partition_by=(),
        presorted=False,
    )
    return runtime


def _dataset(
    *,
    scale: bool = True,
    sequence: SequenceConfig | None = None,
    split: HashSplitConfig | TimeSplitConfig | None = None,
) -> FeatureDatasetConfig:
    return FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(
                id="x",
                stream="stream",
                field="value",
                scale=scale,
                sequence=sequence,
            )
        ],
        split=split,
    )


def test_materialize_standard_scaler_uses_all_scalar_observations(
    tmp_path,
) -> None:
    runtime = _runtime(
        tmp_path,
        _dataset(
            split=TimeSplitConfig(
                boundaries=["2024-01-02T00:00:00Z"],
                labels=["train", "test"],
            )
        ),
        rows=[_record(1, 1.0), _record(3, 3.0)],
    )

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(split_label="all", output="scaler.json"),
    )

    assert result is not None
    artifact = load_scaler_artifact(runtime.artifacts_root / result.relative_path)
    assert isinstance(artifact, StandardScalerArtifact)
    assert artifact.version == 2
    assert artifact.split == "all"
    assert artifact.observations == 2
    assert artifact.statistics["x"].mean == 2.0
    assert result.meta == {
        "features": 1,
        "split": "all",
        "observations": 2,
    }


def test_materialize_standard_scaler_persists_build_options(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path, _dataset(), rows=[_record(1, 4.0)])

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(
            split_label="all",
            output="scaler.json",
            with_mean=False,
            with_std=False,
            epsilon=0.5,
        ),
    )

    assert result is not None
    artifact = load_scaler_artifact(runtime.artifacts_root / result.relative_path)
    assert isinstance(artifact, StandardScalerArtifact)
    assert artifact.with_mean is False
    assert artifact.with_std is False
    assert artifact.epsilon == 0.5


def test_materialize_scaler_rejects_feature_hash_split(
    tmp_path,
) -> None:
    runtime = _runtime(
        tmp_path,
        _dataset(
            split=HashSplitConfig(
                ratios={"train": 1.0},
                key="feature:bucket",
            )
        ),
    )

    with pytest.raises(ValueError, match="requires hash split key 'group'"):
        materialize_scaler_statistics(
            runtime,
            ScalerTask(split_label="train", output="scaler.json"),
        )


def test_materialize_scaler_skips_dataset_without_scaled_features(
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path, _dataset(scale=False))

    assert (
        materialize_scaler_statistics(
            runtime,
            ScalerTask(split_label="all", output="scaler.json"),
        )
        is None
    )


def test_scaler_fitting_observes_scalars_before_sequence(tmp_path) -> None:
    runtime = _runtime(
        tmp_path,
        _dataset(sequence=SequenceConfig(size=3)),
        rows=[_record(1, 1.0), _record(2, 3.0)],
    )

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(split_label="all", output="scaler.json"),
    )

    assert result is not None
    artifact = load_scaler_artifact(runtime.artifacts_root / result.relative_path)
    assert isinstance(artifact, StandardScalerArtifact)
    assert artifact.observations == 2
    assert artifact.statistics["x"].mean == 2.0


def test_materialize_temporal_scaler_assigns_scalar_records_by_record_time(
    tmp_path,
) -> None:
    runtime = _runtime(
        tmp_path,
        _dataset(
            sequence=SequenceConfig(size=2),
            split=TimeSplitConfig(
                boundaries=["2024-01-02T00:00:00Z"],
                labels=["train", "validation"],
            ),
        ),
        rows=[_record(1, 1.0), _record(3, 10.0)],
    )

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask.model_validate(
            {
                "output": "scaler.json",
                "folds": [
                    {"fit": ["train"], "apply": ["train"]},
                    {"fit": ["validation"], "apply": ["validation"]},
                ],
            }
        ),
    )

    assert result is not None
    artifact = load_scaler_artifact(runtime.artifacts_root / result.relative_path)
    assert isinstance(artifact, TemporalScalerArtifact)
    assert artifact.version == 2
    assert artifact.folds[0].scaler.statistics["x"].mean == 1.0
    assert artifact.folds[1].scaler.statistics["x"].mean == 10.0
    assert artifact.folds[0].scaler.observations == 1
    assert result.meta == {
        "mode": "temporal",
        "folds": 2,
        "observations": 2,
    }


def test_scaler_opens_a_shared_stream_once_for_all_scaled_fields(tmp_path) -> None:
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(
                id="value",
                stream="stream",
                field="value",
                scale=True,
            ),
            FeatureRecordConfig(
                id="other",
                stream="stream",
                field="other",
                scale=True,
            ),
        ],
    )
    runtime = _runtime(
        tmp_path,
        dataset,
        rows=[_record(1, 1.0, 10.0), _record(2, 3.0, 30.0)],
    )
    source = runtime.streams["stream"].source

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(split_label="all", output="scaler.json"),
    )

    assert result is not None
    artifact = load_scaler_artifact(runtime.artifacts_root / result.relative_path)
    assert isinstance(artifact, StandardScalerArtifact)
    assert artifact.statistics["value"].mean == 2.0
    assert artifact.statistics["other"].mean == 20.0
    assert source.opens == 1
    assert source.closes == 1


def test_grouped_scaler_preserves_global_scalar_statistics_across_sample_keys(
    tmp_path,
) -> None:
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h", keys=["security_id"]),
        features=[
            FeatureRecordConfig(
                id="value",
                stream="stream",
                field="value",
                scale=True,
            )
        ],
    )
    records = [
        _record(2, 30.0),
        _record(1, 1.0),
        _record(2, 3.0),
        _record(1, 10.0),
    ]
    for record, security_id in zip(records, ["B", "A", "A", "B"]):
        record.security_id = security_id
    runtime = _runtime(tmp_path, dataset, rows=records)
    runtime.streams["stream"] = replace(
        runtime.streams["stream"],
        partition_by=("security_id",),
    )

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(split_label="all", output="scaler.json"),
    )

    assert result is not None
    artifact = load_scaler_artifact(runtime.artifacts_root / result.relative_path)
    assert isinstance(artifact, StandardScalerArtifact)
    statistics = artifact.statistics["value"]
    assert statistics.count == 4
    assert statistics.mean == pytest.approx(11.0)
    assert statistics.std == pytest.approx(131.5**0.5)


def test_scaler_validates_excluded_split_records_before_filtering(tmp_path) -> None:
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(
                id="other",
                stream="stream",
                field="other",
                scale=True,
            )
        ],
        split=TimeSplitConfig(
            boundaries=["2024-01-02T00:00:00Z"],
            labels=["train", "test"],
        ),
    )
    included = _record(1, 0.0, other=1.0)
    excluded = TemporalRecord(time=_time(3))
    excluded.value = 3.0
    runtime = _runtime(tmp_path, dataset, rows=[included, excluded])
    source = runtime.streams["stream"].source
    assert isinstance(source, _CountingSource)

    with pytest.raises(KeyError, match="Record field 'other'"):
        materialize_scaler_statistics(
            runtime,
            ScalerTask(split_label="train", output="scaler.json"),
        )

    assert source.opens == 1
    assert source.closes == 1


def test_scaler_closes_shared_stream_after_invalid_value(tmp_path) -> None:
    runtime = _runtime(tmp_path, _dataset(), rows=[_record(1, "not numeric")])
    source = runtime.streams["stream"].source
    assert isinstance(source, _CountingSource)

    with pytest.raises(TypeError, match="numeric or None"):
        materialize_scaler_statistics(
            runtime,
            ScalerTask(split_label="all", output="scaler.json"),
        )

    assert source.opens == 1
    assert source.closes == 1


def test_materialize_temporal_scaler_requires_time_split(
    tmp_path,
) -> None:
    runtime = _runtime(
        tmp_path,
        _dataset(split=HashSplitConfig(ratios={"train": 1.0})),
    )

    with pytest.raises(RuntimeError, match="dataset split mode 'time'"):
        materialize_scaler_statistics(
            runtime,
            ScalerTask.model_validate(
                {
                    "output": "scaler.json",
                    "folds": [{"fit": ["train"], "apply": ["train"]}],
                }
            ),
        )


def test_materialize_temporal_scaler_requires_apply_fold_for_every_split(
    tmp_path,
) -> None:
    runtime = _runtime(
        tmp_path,
        _dataset(
            split=TimeSplitConfig(
                boundaries=["2024-01-02T00:00:00Z"],
                labels=["train", "validation"],
            )
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="do not apply to split labels: validation",
    ):
        materialize_scaler_statistics(
            runtime,
            ScalerTask.model_validate(
                {
                    "output": "scaler.json",
                    "folds": [{"fit": ["train"], "apply": ["train"]}],
                }
            ),
        )
