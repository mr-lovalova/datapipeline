import json
from datetime import datetime, timezone

import pytest

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.io.serializers import (
    csv_row_serializer,
    json_line_serializer,
    pickle_serializer,
    text_line_serializer,
)


def test_record_json_serializer_emits_plain_payload() -> None:
    class Record:
        def __init__(self, time, value):
            self.time = time
            self.value = value

    rec = Record(datetime(2024, 1, 1, tzinfo=timezone.utc), 42.5)
    serializer = json_line_serializer()

    payload = json.loads(serializer(rec))

    assert payload == {
        "time": "2024-01-01 00:00:00+00:00",
        "value": 42.5,
    }


def test_record_csv_serializer_flattens_nested_payload() -> None:
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

    row = serializer(feature)

    assert row["id"] == "feature_a"
    assert row["record.time"] == datetime(2024, 7, 4, tzinfo=timezone.utc)
    assert row["value"] == 7.0
    assert "kind" not in row


def test_vector_csv_serializer_flattens_feature_and_target_values() -> None:
    serializer = csv_row_serializer()
    row = serializer(
        Sample(
            key=(datetime(2024, 1, 1, tzinfo=timezone.utc),),
            features=Vector(values={"temp": 1.5, "lag2": [1.0, 1.1]}),
            targets=Vector(values={"y": 2.0}),
        )
    )

    assert row["key.0"] == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert row["features.temp"] == 1.5
    assert row["features.lag2.0"] == 1.0
    assert row["features.lag2.1"] == 1.1
    assert row["targets.y"] == 2.0


def test_sample_csv_serializer_flattens_with_feature_prefix() -> None:
    serializer = csv_row_serializer()

    row = serializer(
        Sample(
            key="2024-01-01T00:00:00+00:00",
            features=Vector(values={"wind": [5.0, 6.0]}),
        )
    )

    assert row["key"] == "2024-01-01T00:00:00+00:00"
    assert row["features.wind.0"] == 5.0
    assert row["features.wind.1"] == 6.0


def test_json_serializer_flat_view_emits_flattened_payload() -> None:
    serializer = json_line_serializer(view="flat")

    payload = json.loads(serializer({"features": {"x": 1.0}}))

    assert payload == {"features.x": 1.0}


def test_json_serializer_raw_sample_emits_nested_payload() -> None:
    serializer = json_line_serializer(view="raw")

    payload = json.loads(
        serializer(
            Sample(
                key=("k1",),
                features=Vector(values={"x": 1.0}),
                targets=Vector(values={"y": 2.0}),
            )
        )
    )

    assert payload == {
        "key": ["k1"],
        "features": {"values": {"x": 1.0}},
        "targets": {"values": {"y": 2.0}},
    }


def test_csv_serializer_rejects_non_flat_view() -> None:
    with pytest.raises(ValueError, match="csv output supports only view='flat'"):
        csv_row_serializer(view="raw")


def test_pickle_serializer_rejects_non_raw_view() -> None:
    with pytest.raises(ValueError, match="pickle output supports only view='raw'"):
        pickle_serializer(view="flat")


def test_text_serializer_writes_text_payload() -> None:
    serializer = text_line_serializer()
    assert serializer({"text": "hello world"}) == "hello world\n"
