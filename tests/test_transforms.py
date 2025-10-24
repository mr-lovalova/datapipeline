from __future__ import annotations

from datetime import datetime, timezone
from math import isclose
from typing import Any

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TimeSeriesRecord
from datapipeline.transforms.feature.scaler import StandardScaler, StandardScalerTransform
from datapipeline.transforms.stream.ensure_ticks import drop_missing_values
from datapipeline.transforms.stream.fill import FillTransformer as FeatureFill
from datapipeline.transforms.vector import (
    VectorDropMissingTransform,
    VectorFillAcrossPartitionsTransform,
    VectorFillConstantTransform,
    VectorFillHistoryTransform,
)
from datapipeline.domain.vector import Vector


def _make_time_record(value: float, hour: int) -> TimeSeriesRecord:
    return TimeSeriesRecord(
        time=datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc),
        value=value,
    )


def _make_feature_record(value: float, hour: int, feature_id: str) -> FeatureRecord:
    return FeatureRecord(
        record=_make_time_record(value, hour),
        feature_id=feature_id,
        group_key=(hour,),
    )


def _make_vector(group: int, values: dict[str, Any]) -> tuple[tuple[int], Vector]:
    return ((group,), Vector(values=values))


class _StubVectorContext:
    def __init__(self, expected: list[str]):
        self._expected = list(expected)

    def load_expected_ids(self) -> list[str]:
        return list(self._expected)


def test_drop_missing_values_filters_none_and_nan():
    stream = iter(
        [
            _make_time_record(1.0, 1),
            _make_time_record(float("nan"), 2),
            _make_time_record(3.0, 3),
            _make_time_record(0.0, 4),
        ]
    )

    cleaned = list(drop_missing_values(stream))

    assert [rec.value for rec in cleaned] == [1.0, 3.0, 0.0]


def test_standard_scaler_normalizes_feature_stream():
    training_vectors = iter(
        [
            ((0,), Vector(values={"radiation": 1.0})),
            ((1,), Vector(values={"radiation": 2.0})),
            ((2,), Vector(values={"radiation": 3.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    scaler = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            _make_feature_record(1.0, 0, "radiation"),
            _make_feature_record(2.0, 1, "radiation"),
            _make_feature_record(3.0, 2, "radiation"),
        ]
    )

    transformed = list(scaler.apply(stream))

    values = [fr.record.value for fr in transformed]
    expected = [-1.22474487, 0.0, 1.22474487]
    for observed, target in zip(values, expected):
        assert isclose(observed, target, rel_tol=1e-6)


def test_standard_scaler_uses_provided_statistics():
    training_vectors = iter(
        [
            ((0,), Vector(values={"temperature": 0.0})),
            ((1,), Vector(values={"temperature": 10.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    scaler = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            _make_feature_record(10.0, 0, "temperature"),
            _make_feature_record(11.0, 1, "temperature"),
        ]
    )

    transformed = list(scaler.apply(stream))

    assert [fr.record.value for fr in transformed] == [1.0, 1.2]


def test_standard_scaler_fit_and_serialize(tmp_path):
    vectors = iter(
        [
            ((0,), Vector(values={"temp": 10.0, "wind": 5.0})),
            ((1,), Vector(values={"temp": 14.0, "wind": 7.0})),
        ]
    )

    scaler = StandardScaler()
    total = scaler.fit(vectors)
    assert total == 4
    path = tmp_path / "scaler.pkl"
    scaler.save(path)
    restored = StandardScaler.load(path)
    transform = StandardScalerTransform(model_path=path)

    stream = iter(
        [
            _make_feature_record(12.0, 0, "temp"),
            _make_feature_record(6.0, 0, "wind"),
        ]
    )

    transformed = list(transform.apply(stream))
    assert len(restored.statistics) == 2
    assert len(transformed) == 2


def test_time_mean_fill_uses_running_average():
    stream = iter(
        [
            _make_feature_record(10.0, 0, "temp"),
            _make_feature_record(12.0, 1, "temp"),
            _make_feature_record(None, 2, "temp"),
            _make_feature_record(16.0, 3, "temp"),
            _make_feature_record(float("nan"), 4, "temp"),
        ]
    )

    transformer = FeatureFill(statistic="mean")

    transformed = list(transformer.apply(stream))
    values = [fr.record.value for fr in transformed]

    assert values[2] == 11.0  # mean of 10 and 12
    assert isclose(values[4], 38.0 / 3.0, rel_tol=1e-9)


def test_time_median_fill_honours_window():
    stream = iter(
        [
            _make_feature_record(1.0, 0, "wind"),
            _make_feature_record(100.0, 1, "wind"),
            _make_feature_record(2.0, 2, "wind"),
            _make_feature_record(None, 3, "wind"),
            _make_feature_record(None, 4, "wind"),
        ]
    )

    transformer = FeatureFill(statistic="median", window=2)

    transformed = list(transformer.apply(stream))
    values = [fr.record.value for fr in transformed]

    # history window restricted to last two valid values -> [100, 2]
    assert values[3] == 51.0
    # second missing reuses same history (still [100,2])
    assert values[4] == 51.0


def test_vector_fill_history_uses_running_statistics():
    stream = iter(
        [
            _make_vector(0, {"temp__A": 10.0}),
            _make_vector(1, {"temp__A": 12.0}),
            _make_vector(2, {}),
        ]
    )

    transform = VectorFillHistoryTransform(
        statistic="mean", window=2, min_samples=2)
    transform.bind_context(_StubVectorContext(["temp__A"]))

    out = list(transform.apply(stream))
    assert out[2][1].values["temp__A"] == 11.0


def test_vector_fill_horizontal_averages_siblings():
    stream = iter(
        [
            _make_vector(0, {"wind__A": 10.0, "wind__B": 14.0}),
            _make_vector(1, {"wind__A": 12.0}),
        ]
    )

    transform = VectorFillAcrossPartitionsTransform(
        statistic="median")
    transform.bind_context(_StubVectorContext(["wind__A", "wind__B"]))

    out = list(transform.apply(stream))
    # First bucket remains unchanged
    assert out[0][1].values == {"wind__A": 10.0, "wind__B": 14.0}
    # Second bucket fills missing wind__B using value from same timestamp (only A present -> not enough samples)
    # Wait we need at least min_samples=1 -> default 1 so fill uses available values
    assert out[1][1].values["wind__B"] == 12.0


def test_vector_fill_constant_injects_value():
    stream = iter([_make_vector(0, {"time": 1.0})])
    transform = VectorFillConstantTransform(value=0.0)
    transform.bind_context(_StubVectorContext(["time", "wind"]))
    out = list(transform.apply(stream))
    assert out[0][1].values["wind"] == 0.0


def test_vector_drop_missing_respects_coverage():
    stream = iter(
        [
            _make_vector(0, {"a": 1.0, "b": 2.0}),
            _make_vector(1, {"a": 3.0}),
        ]
    )

    transform = VectorDropMissingTransform(min_coverage=1.0)
    transform.bind_context(_StubVectorContext(["a", "b"]))

    out = list(transform.apply(stream))
    assert len(out) == 1
    assert out[0][1].values == {"a": 1.0, "b": 2.0}
