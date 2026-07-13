from datetime import datetime, timezone

import pytest

from datapipeline.artifacts.scaler import (
    StandardScalerArtifact,
    TemporalScalerArtifact,
    load_scaler_artifact,
)
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.config.split import HashSplitConfig, TimeSplitConfig
from datapipeline.config.tasks import ScalerTask
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.operations.artifacts.scaler import (
    _scaled_configs,
    materialize_scaler_statistics,
)
from datapipeline.runtime import IngestRuntimeStream, Runtime


def _time(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _feature(day: int, value: object, feature_id: str = "x") -> FeatureRecord:
    return FeatureRecord(
        record=TemporalRecord(time=_time(day)),
        id=feature_id,
        value=value,
    )


class _EmptySource:
    def stream(self):
        return iter(())


def _identity(records):
    return records


def _runtime(tmp_path) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    runtime.streams["stream"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=(),
        feature_id_by=None,
        presorted=False,
    )
    return runtime


def _dataset(*, scale: bool = True, sequence: SequenceConfig | None = None):
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
    )


def test_materialize_standard_scaler_uses_all_scalar_observations(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.split = TimeSplitConfig(
        boundaries=["2024-01-02T00:00:00Z"],
        labels=["train", "test"],
    )
    dataset = _dataset()
    features = [
        ((_time(1),), _feature(1, 1.0)),
        ((_time(3),), _feature(3, 3.0)),
    ]
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: dataset,
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler._iter_unscaled_features",
        lambda *_args: iter(features),
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
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    dataset = _dataset()
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: dataset,
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler._iter_unscaled_features",
        lambda *_args: iter([((_time(1),), _feature(1, 4.0))]),
    )

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
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.split = HashSplitConfig(
        ratios={"train": 1.0},
        key="feature:bucket",
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: _dataset(),
    )

    with pytest.raises(ValueError, match="requires hash split key 'group'"):
        materialize_scaler_statistics(
            runtime,
            ScalerTask(split_label="train", output="scaler.json"),
        )


def test_materialize_scaler_skips_dataset_without_scaled_features(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: _dataset(scale=False),
    )

    assert (
        materialize_scaler_statistics(
            runtime,
            ScalerTask(split_label="all", output="scaler.json"),
        )
        is None
    )


def test_scaler_fitting_disables_scale_and_sequence() -> None:
    [config] = _scaled_configs(
        [
            FeatureRecordConfig(
                id="x",
                stream="stream",
                field="value",
                scale=True,
                sequence=SequenceConfig(size=3),
            )
        ]
    )

    assert config.scale is False
    assert config.sequence is None


def test_materialize_temporal_scaler_assigns_scalar_records_by_record_time(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.split = TimeSplitConfig(
        boundaries=["2024-01-02T00:00:00Z"],
        labels=["train", "validation"],
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: _dataset(sequence=SequenceConfig(size=2)),
    )
    # The sample key deliberately points at validation. Fitting must use the scalar
    # record time, just as runtime temporal scaling does, rather than a sequence key.
    features = [
        ((_time(3),), _feature(1, 1.0)),
        ((_time(1),), _feature(3, 10.0)),
    ]
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler._iter_unscaled_features",
        lambda *_args: iter(features),
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


def test_materialize_temporal_scaler_requires_time_split(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.split = HashSplitConfig(ratios={"train": 1.0})
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: _dataset(),
    )

    with pytest.raises(RuntimeError, match="project split mode 'time'"):
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
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime.split = TimeSplitConfig(
        boundaries=["2024-01-02T00:00:00Z"],
        labels=["train", "validation"],
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args: _dataset(),
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
