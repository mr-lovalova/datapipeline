import json
from datetime import datetime, timedelta, timezone
from math import isclose

import pytest
from pydantic import ValidationError

from datapipeline.artifacts.scaler import (
    ScalerStatistics,
    StandardScalerArtifact,
    TemporalScalerArtifact,
    TemporalScalerFold,
    TemporalScalerSplit,
    load_scaler_artifact,
    save_scaler_artifact,
)
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.feature.scaler import (
    FeatureScaler,
    ScalerAccumulator,
)

_SAMPLE_CADENCE = timedelta(days=1)


def _feature(value: object, day: int = 1, feature_id: str = "x") -> FeatureRecord:
    record = TemporalRecord(
        time=datetime(2024, 1, day, tzinfo=timezone.utc),
    )
    return FeatureRecord(record=record, id=feature_id, value=value)


def _standard_artifact(
    *,
    mean: float = 2.0,
    std: float = 1.0,
    count: int = 2,
    with_mean: bool = True,
    with_std: bool = True,
    epsilon: float = 1e-12,
) -> StandardScalerArtifact:
    return StandardScalerArtifact(
        with_mean=with_mean,
        with_std=with_std,
        epsilon=epsilon,
        observations=count,
        statistics={
            "x": ScalerStatistics(mean=mean, std=std, count=count),
        },
    )


def _temporal_artifact(
    boundary: str = "2024-01-02T00:00:00Z",
) -> TemporalScalerArtifact:
    return TemporalScalerArtifact(
        split=TemporalScalerSplit(
            boundaries=(boundary,),
            labels=("train", "validation"),
        ),
        folds=(
            TemporalScalerFold(
                fit=("train",),
                apply=("train",),
                scaler=_standard_artifact(mean=1.0, std=1.0, count=1),
            ),
            TemporalScalerFold(
                fit=("validation",),
                apply=("validation",),
                scaler=_standard_artifact(mean=10.0, std=2.0, count=1),
            ),
        ),
    )


def test_accumulator_fits_population_statistics() -> None:
    accumulator = ScalerAccumulator()

    accumulator.observe("x", 1.0)
    accumulator.observe("x", 2.0)
    accumulator.observe("x", 3.0)

    artifact = accumulator.artifact(split="train")
    statistics = artifact.statistics["x"]
    assert statistics.count == 3
    assert statistics.mean == 2.0
    assert isclose(statistics.std, 0.816496580927726)
    assert artifact.observations == 3
    assert artifact.split == "train"


def test_scaler_applies_fitted_options() -> None:
    artifact = _standard_artifact(mean=10.0, std=2.0)
    scaled = list(
        FeatureScaler(artifact, _SAMPLE_CADENCE).apply(
            iter([_feature(10.0), _feature(12.0, day=2)])
        )
    )

    assert [feature.value for feature in scaled] == [0.0, 1.0]


def test_scaler_does_not_override_disabled_fitted_options() -> None:
    artifact = _standard_artifact(
        mean=10.0,
        std=2.0,
        with_mean=False,
        with_std=False,
    )

    [scaled] = FeatureScaler(artifact, _SAMPLE_CADENCE).apply(iter([_feature(12)]))

    assert scaled.value == 12.0


def test_constant_feature_uses_fitted_epsilon() -> None:
    accumulator = ScalerAccumulator(epsilon=0.25)
    accumulator.observe("x", 4.0)
    accumulator.observe("x", 4.0)

    artifact = accumulator.artifact()
    [scaled] = FeatureScaler(artifact, _SAMPLE_CADENCE).apply(iter([_feature(4.5)]))

    assert artifact.statistics["x"].std == 0.25
    assert scaled.value == 2.0


def test_none_is_not_fitted_and_passes_through_scaling() -> None:
    accumulator = ScalerAccumulator()
    accumulator.observe("x", None)
    accumulator.observe("x", 1.0)

    assert accumulator.observations == 1

    missing = _feature(None)
    [scaled] = FeatureScaler(
        accumulator.artifact(),
        _SAMPLE_CADENCE,
    ).apply(iter([missing]))

    assert scaled is missing
    assert scaled.value is None


def test_none_passes_through_without_fitted_statistics() -> None:
    missing = _feature(None, feature_id="all_null")

    [scaled] = FeatureScaler(
        _standard_artifact(),
        _SAMPLE_CADENCE,
    ).apply(iter([missing]))

    assert scaled is missing


@pytest.mark.parametrize("value", [True, "1.0", [1.0]])
def test_scaler_rejects_non_numeric_values(value: object) -> None:
    with pytest.raises(TypeError, match="numeric or None"):
        ScalerAccumulator().observe("x", value)

    with pytest.raises(TypeError, match="numeric or None"):
        list(
            FeatureScaler(_standard_artifact(), _SAMPLE_CADENCE).apply(
                iter([_feature(value)])
            )
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_scaler_rejects_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError, match="must be finite"):
        ScalerAccumulator().observe("x", value)

    with pytest.raises(ValueError, match="must be finite"):
        list(
            FeatureScaler(_standard_artifact(), _SAMPLE_CADENCE).apply(
                iter([_feature(value)])
            )
        )


def test_scaler_requires_statistics_for_every_feature() -> None:
    with pytest.raises(KeyError, match="Missing scaler statistics"):
        list(
            FeatureScaler(_standard_artifact(), _SAMPLE_CADENCE).apply(
                iter([_feature(1.0, feature_id="unknown")])
            )
        )


def test_scaler_artifact_round_trip(tmp_path) -> None:
    path = tmp_path / "scaler.json"
    artifact = _standard_artifact()

    save_scaler_artifact(path, artifact)

    assert load_scaler_artifact(path) == artifact


def test_scaler_artifact_rejects_inconsistent_observation_count() -> None:
    with pytest.raises(ValidationError, match="observations"):
        StandardScalerArtifact(
            with_mean=True,
            with_std=True,
            epsilon=1e-12,
            observations=2,
            statistics={
                "x": ScalerStatistics(mean=0.0, std=1.0, count=1),
            },
        )


@pytest.mark.parametrize(
    "payload",
    [
        {
            "kind": "standard_scaler",
            "version": 1,
            "with_mean": True,
            "with_std": True,
            "epsilon": 1e-12,
            "observations": 1,
            "statistics": {"x": {"mean": 0.0, "std": 1.0, "count": 1}},
        },
        {
            "kind": "standard_scaler",
            "version": 2,
            "with_mean": True,
            "with_std": True,
            "epsilon": 1e-12,
            "observations": 1,
            "statistics": {"x": {"mean": 0.0, "count": 1}},
        },
        {
            "kind": "standard_scaler",
            "version": 2,
            "with_mean": "false",
            "with_std": True,
            "epsilon": 1e-12,
            "observations": 1,
            "statistics": {"x": {"mean": 0.0, "std": 1.0, "count": 1}},
        },
    ],
)
def test_scaler_artifact_rejects_old_incomplete_or_coerced_payloads(
    tmp_path,
    payload,
) -> None:
    path = tmp_path / "scaler.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValidationError):
        load_scaler_artifact(path)


def test_temporal_scaler_selects_fold_before_scaling() -> None:
    scaled = list(
        FeatureScaler(_temporal_artifact(), _SAMPLE_CADENCE).apply(
            iter([_feature(2.0, day=1), _feature(12.0, day=3)])
        )
    )

    assert [feature.value for feature in scaled] == [1.0, 1.0]


def test_temporal_scaler_selects_fold_from_sample_cadence() -> None:
    feature = FeatureRecord(
        record=TemporalRecord(
            time=datetime(2024, 1, 1, 13, tzinfo=timezone.utc),
        ),
        id="x",
        value=2.0,
    )

    scaled = FeatureScaler(
        _temporal_artifact("2024-01-01T12:00:00Z"),
        _SAMPLE_CADENCE,
    ).scale(feature)

    assert scaled.value == 1.0


def test_temporal_scaler_applies_one_fold_to_an_entire_sequence() -> None:
    sequence = FeatureSequence(
        time=datetime(2024, 1, 3, tzinfo=timezone.utc),
        id="x",
        values=[10.0, 12.0],
    )

    scaled = FeatureScaler(
        _temporal_artifact(),
        _SAMPLE_CADENCE,
    ).scale(sequence)

    assert scaled.values == [0.0, 1.0]


def test_temporal_scaler_treats_naive_boundary_as_utc() -> None:
    artifact = _temporal_artifact("2024-01-02T00:00:00")

    assert artifact.split.boundaries == ("2024-01-02T00:00:00",)


def test_temporal_scaler_artifact_rejects_unassigned_split() -> None:
    with pytest.raises(ValidationError, match="no apply fold: validation"):
        TemporalScalerArtifact(
            split=TemporalScalerSplit(
                boundaries=("2024-01-02T00:00:00Z",),
                labels=("train", "validation"),
            ),
            folds=(
                TemporalScalerFold(
                    fit=("train",),
                    apply=("train",),
                    scaler=_standard_artifact(count=1),
                ),
            ),
        )


def test_temporal_artifact_rejects_overlapping_apply_folds() -> None:
    fold = TemporalScalerFold(
        fit=("train",),
        apply=("train",),
        scaler=_standard_artifact(count=1),
    )

    with pytest.raises(ValidationError, match="multiple apply folds"):
        TemporalScalerArtifact(
            split=TemporalScalerSplit(
                boundaries=(),
                labels=("train",),
            ),
            folds=(fold, fold),
        )
