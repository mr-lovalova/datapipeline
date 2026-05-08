from datetime import datetime, timezone

from datapipeline.transforms.where import where
from datapipeline.utils.placeholders import MissingInterpolation


def test_where_rejects_missing_interpolation_comparand():
    stream = iter([{"time": "2024-01-01T00:00:00Z"}])

    try:
        where(
            stream,
            field="time",
            operator="ge",
            comparand=MissingInterpolation("start_time"),
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("where should reject missing interpolation comparands")

    assert "field 'time'" in message
    assert "operator 'ge'" in message
    assert "missing" in message


def test_time_where_rejects_invalid_datetime_comparand():
    stream = iter([{"time": "2024-01-01T00:00:00Z"}])

    try:
        where(stream, field="time", operator="ge", comparand="not-a-date")
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("time where should reject invalid datetime comparands")

    assert "field 'time'" in message
    assert "operator 'ge'" in message
    assert "valid datetime" in message


def test_time_where_rejects_invalid_record_time():
    stream = iter([{"time": "not-a-date"}])
    selected = where(stream, field="time", operator="ge", comparand="2024-01-01T00:00:00Z")

    try:
        list(selected)
    except TypeError as exc:
        message = str(exc)
    else:
        raise AssertionError("time where should reject invalid record timestamps")

    assert "field 'time'" in message
    assert "datetime value" in message


def test_time_where_compares_valid_datetimes():
    stream = iter(
        [
            {"time": datetime(2023, 12, 31, tzinfo=timezone.utc)},
            {"time": datetime(2024, 1, 1, tzinfo=timezone.utc)},
        ]
    )

    selected = where(stream, field="time", operator="ge", comparand="2024-01-01T00:00:00Z")

    assert list(selected) == [{"time": datetime(2024, 1, 1, tzinfo=timezone.utc)}]
