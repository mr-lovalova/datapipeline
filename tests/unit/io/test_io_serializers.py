import json
from datetime import datetime, timezone

import pytest

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.io.serializers import (
    csv_row_serializer,
    json_line_serializer,
)


def test_record_json_serializer_handles_temporal_record() -> None:
    rec = TemporalRecord(time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    setattr(rec, "value", 42.5)
    serializer = json_line_serializer("record")

    line = serializer(rec)
    payload = json.loads(line)

    assert payload["kind"] == "TemporalRecord"
    assert payload["key"] == "2024-01-01 00:00:00+00:00"
    assert payload["fields"]["value"] == 42.5


def test_record_csv_serializer_uses_nested_record_time() -> None:
    record = TemporalRecord(time=datetime(2024, 7, 4, tzinfo=timezone.utc))
    setattr(record, "value", 7.0)
    feature = FeatureRecord(id="feature_a", record=record, value=7.0)
    serializer = csv_row_serializer("record")

    row = serializer(feature)

    assert row["key"] == "2024-07-04 00:00:00+00:00"
    assert row["kind"] == "FeatureRecord"
    assert row["field.id"] == "feature_a"
    assert row["field.record.value"] == 7.0


def test_vector_csv_serializer_flattens_feature_values() -> None:
    serializer = csv_row_serializer()
    sample = Sample(
        key=(datetime(2024, 1, 1, tzinfo=timezone.utc),),
        features=Vector(values={"temp": 1.5, "lag2": [1.0, 1.1]}),
        targets=Vector(values={"y": 2.0}),
    )

    row = serializer(sample)

    assert row["key_0"] == "2024-01-01 00:00:00+00:00"
    assert row["kind"] == "Sample"
    assert row["field.features.temp"] == 1.5
    assert row["field.features.lag2.0"] == 1.0
    assert row["field.features.lag2.1"] == 1.1
    assert row["field.targets.y"] == 2.0


def test_sample_csv_serializer_flattens_with_features_prefix() -> None:
    serializer = csv_row_serializer()
    sample = Sample(
        key="2024-01-01T00:00:00+00:00",
        features=Vector(values={"wind": [5.0, 6.0]}),
    )

    row = serializer(sample)

    assert row["key"] == "2024-01-01T00:00:00+00:00"
    assert row["kind"] == "Sample"
    assert row["field.features.wind.0"] == 5.0
    assert row["field.features.wind.1"] == 6.0


def test_json_serializer_flat_view_excludes_raw() -> None:
    serializer = json_line_serializer(view="flat")
    sample = Sample(
        key="k1",
        features=Vector(values={"x": 1.0}),
    )

    payload = json.loads(serializer(sample))
    assert payload["key"] == "k1"
    assert payload["kind"] == "Sample"
    assert payload["fields"]["features.x"] == 1.0
    assert "raw" not in payload


def test_json_serializer_numeric_view_includes_values_only() -> None:
    serializer = json_line_serializer(view="numeric")
    sample = Sample(
        key="k1",
        features=Vector(values={"x": 1.0}),
    )

    payload = json.loads(serializer(sample))
    assert payload["values"] == [1.0]
    assert "fields" not in payload
    assert "raw" not in payload


def test_csv_serializer_rejects_non_flat_view() -> None:
    with pytest.raises(ValueError, match="csv output supports only view='flat' or view='numeric'"):
        csv_row_serializer(view="raw")


def test_csv_serializer_numeric_view_emits_value_columns() -> None:
    serializer = csv_row_serializer(view="numeric")
    sample = Sample(
        key="k1",
        features=Vector(values={"x": 1.0, "y": 2.0}),
    )

    row = serializer(sample)
    assert row["key"] == "k1"
    assert row["kind"] == "Sample"
    assert row["value_0"] == 1.0
    assert row["value_1"] == 2.0
