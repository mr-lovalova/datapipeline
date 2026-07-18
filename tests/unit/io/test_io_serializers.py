import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pytest

from datapipeline.domain.sample import Sample
from datapipeline.domain.variable import VariableRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.vector import Vector
from datapipeline.io.serializers import (
    csv_row_serializer,
    json_line_serializer,
    pickle_serializer,
    text_line_serializer,
)


def test_record_json_serializer_emits_plain_payload() -> None:
    @dataclass
    class Record:
        time: datetime
        value: float

    rec = Record(datetime(2024, 1, 1, tzinfo=timezone.utc), 42.5)
    serializer = json_line_serializer()

    payload = json.loads(serializer(rec))

    assert payload == {
        "time": "2024-01-01 00:00:00+00:00",
        "value": 42.5,
    }


def test_variable_csv_serializer_flattens_payload() -> None:
    time = datetime(2024, 7, 4, tzinfo=timezone.utc)
    variable = VariableRecord("feature_a", time, 7.0)
    serializer = csv_row_serializer()

    row = serializer(variable)

    assert row["id"] == "feature_a"
    assert row["time"] == time
    assert row["value"] == 7.0
    assert "kind" not in row


def test_variable_json_serializer_emits_only_projected_fields() -> None:
    time = datetime(2024, 7, 4, tzinfo=timezone.utc)
    variable = VariableRecord("feature_a", time, 7.0, ("AAPL",))

    payload = json.loads(json_line_serializer()(variable))

    assert payload == {
        "id": "feature_a",
        "time": "2024-07-04 00:00:00+00:00",
        "value": 7.0,
        "entity_key": ["AAPL"],
    }


def test_temporal_record_serializer_includes_mapped_fields() -> None:
    record = TemporalRecord(datetime(2024, 7, 4, tzinfo=timezone.utc))
    record.security_id = "AAPL"
    record.close = 42.5

    payload = json.loads(json_line_serializer()(record))

    assert payload == {
        "time": "2024-07-04 00:00:00+00:00",
        "security_id": "AAPL",
        "close": 42.5,
    }


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


def test_flat_json_serializer_rejects_colliding_sample_feature_ids() -> None:
    serializer = json_line_serializer(view="flat")
    sample = Sample(
        key=("sample",),
        features=Vector(values={"x": [10, 11], "x.0": 99}),
    )

    with pytest.raises(ValueError, match=r"features\.x\.0"):
        serializer(sample)


def test_csv_serializer_rejects_colliding_nested_fields() -> None:
    serializer = csv_row_serializer()

    with pytest.raises(ValueError, match=r"x\.0"):
        serializer({"x": [10, 11], "x.0": 99})


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


def test_json_serializer_supports_dataclasses_sequences_and_dates() -> None:
    @dataclass
    class Payload:
        day: date
        values: tuple[int, ...]

    payload = json.loads(json_line_serializer()(Payload(date(2024, 7, 4), (1, 2))))

    assert payload == {"day": "2024-07-04", "values": [1, 2]}


def test_serializers_reject_arbitrary_objects() -> None:
    class Unsupported:
        def __init__(self) -> None:
            self.value = 1

    with pytest.raises(TypeError, match="Unsupported output value type: Unsupported"):
        json_line_serializer()(Unsupported())

    with pytest.raises(TypeError, match="Unsupported output value type: Unsupported"):
        csv_row_serializer()(Unsupported())


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_json_serializer_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError, match="Out of range float values"):
        json_line_serializer()(value)


def test_json_serializer_rejects_non_string_mapping_keys() -> None:
    with pytest.raises(
        TypeError,
        match="Unsupported output mapping key type: int",
    ):
        json_line_serializer()({1: "value"})
