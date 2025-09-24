from __future__ import annotations

from datetime import datetime, timezone
from math import isclose
import json
from pathlib import Path

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TimeFeatureRecord
from datapipeline.domain.vector import Vector
from datapipeline.transforms.feature import StandardScalerTransform
from datapipeline.transforms.record import drop_missing_values
from datapipeline.transforms.vector import (
    DropIncompleteVectorTransform,
    FillMissingVectorTransform,
)


def _make_time_record(value: float, hour: int) -> TimeFeatureRecord:
    return TimeFeatureRecord(
        time=datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc),
        value=value,
    )


def _make_feature_record(value: float, hour: int, feature_id: str) -> FeatureRecord:
    return FeatureRecord(
        record=_make_time_record(value, hour),
        feature_id=feature_id,
        group_key=(hour,),
    )


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
    stream = iter(
        [
            _make_feature_record(1.0, 0, "radiation"),
            _make_feature_record(2.0, 1, "radiation"),
            _make_feature_record(3.0, 2, "radiation"),
        ]
    )
    scaler = StandardScalerTransform()

    transformed = list(scaler.apply(stream))

    values = [fr.record.value for fr in transformed]
    expected = [-1.22474487, 0.0, 1.22474487]
    for observed, target in zip(values, expected):
        assert isclose(observed, target, rel_tol=1e-6)
    assert isclose(scaler.stats_["radiation"]["mean"], 2.0, rel_tol=1e-6)


def test_standard_scaler_uses_provided_statistics():
    stream = iter(
        [
            _make_feature_record(10.0, 0, "temperature"),
            _make_feature_record(11.0, 1, "temperature"),
        ]
    )
    scaler = StandardScalerTransform(
        statistics={"temperature": {"mean": 5.0, "std": 5.0}}
    )

    transformed = list(scaler.apply(stream))

    assert [fr.record.value for fr in transformed] == [1.0, 1.2]


def test_drop_incomplete_vector_transform_filters_missing_features():
    stream = iter(
        [
            (("2024-01-01",), Vector(values={"time": 1.0, "wind": 2.0})),
            (("2024-01-02",), Vector(values={"time": 2.0})),
        ]
    )

    transform = DropIncompleteVectorTransform(required=["time", "wind"], min_coverage=1.0)

    remaining = list(transform.apply(stream))

    assert len(remaining) == 1
    assert remaining[0][1].values == {"time": 1.0, "wind": 2.0}


def test_fill_missing_vector_transform_injects_defaults():
    stream = iter(
        [
            (("2024-01-01",), Vector(values={"time": 1.0})),
        ]
    )

    transform = FillMissingVectorTransform(expected=["time", "wind"], value=-1.0)

    filled = list(transform.apply(stream))

    assert filled[0][1].values["wind"] == -1.0


def test_drop_incomplete_vector_transform_uses_manifest(tmp_path: Path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"features": ["time", "wind"]}), encoding="utf-8")

    stream = iter(
        [
            (("2024-01-01",), Vector(values={"time": 1.0, "wind": 2.0})),
            (("2024-01-02",), Vector(values={"time": 2.0})),
        ]
    )

    transform = DropIncompleteVectorTransform(manifest=str(manifest), min_coverage=1.0)

    remaining = list(transform.apply(stream))

    assert len(remaining) == 1
    assert remaining[0][1].values == {"time": 1.0, "wind": 2.0}


def test_fill_missing_vector_transform_uses_manifest(tmp_path: Path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"features": ["time", "wind"]}), encoding="utf-8")

    stream = iter(
        [
            (("2024-01-01",), Vector(values={"time": 1.0})),
        ]
    )

    transform = FillMissingVectorTransform(manifest=str(manifest), value=-1.0)

    filled = list(transform.apply(stream))

    assert filled[0][1].values["wind"] == -1.0
