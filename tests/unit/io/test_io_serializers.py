import json
from datetime import datetime, timezone

import pytest

from datapipeline.io.normalization import normalized_record_row, normalized_row
from datapipeline.io.serializers import (
    csv_row_serializer,
    json_line_serializer,
    text_line_serializer,
)


def test_record_json_serializer_handles_temporal_record() -> None:
    class Record:
        def __init__(self, time, value):
            self.time = time
            self.value = value

    rec = Record(datetime(2024, 1, 1, tzinfo=timezone.utc), 42.5)
    serializer = json_line_serializer()

    line = serializer(normalized_record_row(rec))
    payload = json.loads(line)

    assert payload["kind"] == "Record"
    assert payload["key"] == "2024-01-01 00:00:00+00:00"
    assert payload["fields"]["value"] == 42.5


def test_record_csv_serializer_uses_nested_record_time() -> None:
    class TemporalRecord:
        def __init__(self, time):
            self.time = time

    class FeatureRecord:
        def __init__(self, id, record, value):
            self.id = id
            self.record = record
            self.value = value

    record = TemporalRecord(datetime(2024, 7, 4, tzinfo=timezone.utc))
    feature = FeatureRecord("feature_a", record, 7.0)
    serializer = csv_row_serializer()

    row = serializer(normalized_record_row(feature))

    assert row["key"] == "2024-07-04 00:00:00+00:00"
    assert row["kind"] == "FeatureRecord"
    assert row["field.id"] == "feature_a"
    assert row["field.value"] == 7.0


def test_vector_csv_serializer_flattens_feature_values() -> None:
    serializer = csv_row_serializer()
    row = normalized_row(
        key=(datetime(2024, 1, 1, tzinfo=timezone.utc),),
        kind="Sample",
        raw={"features": {"temp": 1.5, "lag2": [1.0, 1.1]}, "targets": {"y": 2.0}},
        fields={
            "features.temp": 1.5,
            "features.lag2.0": 1.0,
            "features.lag2.1": 1.1,
            "targets.y": 2.0,
        },
    )

    serialized = serializer(row)

    assert serialized["key_0"] == "2024-01-01 00:00:00+00:00"
    assert serialized["kind"] == "Sample"
    assert serialized["field.features.temp"] == 1.5
    assert serialized["field.features.lag2.0"] == 1.0
    assert serialized["field.features.lag2.1"] == 1.1
    assert serialized["field.targets.y"] == 2.0


def test_sample_csv_serializer_flattens_with_features_prefix() -> None:
    serializer = csv_row_serializer()
    row = normalized_row(
        key="2024-01-01T00:00:00+00:00",
        kind="Sample",
        raw={"features": {"wind": [5.0, 6.0]}},
        fields={"features.wind.0": 5.0, "features.wind.1": 6.0},
    )

    serialized = serializer(row)

    assert serialized["key"] == "2024-01-01T00:00:00+00:00"
    assert serialized["kind"] == "Sample"
    assert serialized["field.features.wind.0"] == 5.0
    assert serialized["field.features.wind.1"] == 6.0


def test_json_serializer_flat_view_excludes_raw() -> None:
    serializer = json_line_serializer(view="flat")
    row = normalized_row(
        key="k1",
        kind="Sample",
        raw={"features": {"x": 1.0}},
        fields={"features.x": 1.0},
    )

    payload = json.loads(serializer(row))
    assert payload["key"] == "k1"
    assert payload["kind"] == "Sample"
    assert payload["fields"]["features.x"] == 1.0
    assert "raw" not in payload


def test_json_serializer_values_view_includes_values_only() -> None:
    serializer = json_line_serializer(view="values")
    row = normalized_row(
        key="k1",
        kind="Sample",
        raw={"features": {"x": 1.0, "label": "sunny"}},
        fields={"features.x": 1.0, "features.label": "sunny"},
    )

    payload = json.loads(serializer(row))
    assert payload["values"] == ["sunny", 1.0]
    assert "fields" not in payload
    assert "raw" not in payload


def test_csv_serializer_rejects_non_flat_view() -> None:
    with pytest.raises(ValueError, match="csv output supports only view='flat' or view='values'"):
        csv_row_serializer(view="raw")


def test_csv_serializer_values_view_emits_value_columns() -> None:
    serializer = csv_row_serializer(view="values")
    row = normalized_row(
        key="k1",
        kind="Sample",
        raw={"features": {"x": 1.0, "y": 2.0}},
        fields={"features.x": 1.0, "features.y": 2.0},
    )

    serialized = serializer(row)
    assert serialized["key"] == "k1"
    assert serialized["kind"] == "Sample"
    assert serialized["value_0"] == 1.0
    assert serialized["value_1"] == 2.0


def test_text_serializer_writes_text_payload() -> None:
    serializer = text_line_serializer()
    row = normalized_record_row({"text": "hello world"})
    assert serializer(row) == "hello world\n"
