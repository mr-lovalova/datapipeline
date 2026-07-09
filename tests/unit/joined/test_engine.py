from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

import pytest

from datapipeline.domain.record import TemporalRecord
from datapipeline.joined.engine import JoinInput, JoinMetrics, JoinSpec, join_rows


@dataclass
class _Record(TemporalRecord):
    key: str | None
    value: int


@dataclass
class _RecordWithoutKey(TemporalRecord):
    value: int


def _time(day: int) -> datetime:
    return datetime(2025, 1, day, tzinfo=timezone.utc)


def _spec(
    *,
    fields: tuple[str, ...],
    mode: Literal["inner", "left"] = "inner",
) -> JoinSpec:
    return JoinSpec(
        output_stream_id="joined.out",
        primary=JoinInput(alias="a", stream_id="stream.a"),
        secondary_inputs=(
            JoinInput(alias="b", stream_id="stream.b"),
        ),
        fields=fields,
        mode=mode,
    )


def test_join_rows_rejects_missing_mapping_field() -> None:
    metrics = JoinMetrics()
    inputs = {
        "a": iter([_Record(time=_time(1), key="A", value=1)]),
        "b": iter([{"time": _time(1), "value": 20}]),
    }

    with pytest.raises(
        ValueError,
        match=(
            "Joined stream 'joined.out' input 'b' from 'stream.b' row 1 "
            "is missing join field 'key'"
        ),
    ):
        list(join_rows(inputs, _spec(fields=("key",)), metrics))


def test_join_rows_rejects_missing_primary_field() -> None:
    metrics = JoinMetrics()
    inputs = {
        "a": iter([_RecordWithoutKey(time=_time(1), value=1)]),
        "b": iter([_Record(time=_time(1), key="A", value=20)]),
    }

    with pytest.raises(
        ValueError,
        match=(
            "Joined stream 'joined.out' input 'a' from 'stream.a' row 1 "
            "is missing join field 'key'"
        ),
    ):
        list(join_rows(inputs, _spec(fields=("key",)), metrics))


def test_join_rows_preserves_explicit_null_mapping_field() -> None:
    metrics = JoinMetrics()
    secondary = {"time": _time(1), "key": None, "value": 20}
    inputs = {
        "a": iter([_Record(time=_time(1), key=None, value=1)]),
        "b": iter([secondary]),
    }

    rows = list(join_rows(inputs, _spec(fields=("key",)), metrics))

    assert len(rows) == 1
    assert rows[0].values["b"] is secondary


def test_join_rows_reports_missing_primary_time_separately() -> None:
    metrics = JoinMetrics()
    inputs = {
        "a": iter([{"key": "A", "value": 1}]),
        "b": iter([_Record(time=_time(1), key="A", value=20)]),
    }

    with pytest.raises(
        ValueError,
        match=(
            "Joined stream 'joined.out' primary input 'a' from 'stream.a' row 1 "
            "is missing required time field"
        ),
    ):
        list(join_rows(inputs, _spec(fields=("key",)), metrics))


def test_join_rows_rejects_invalid_secondary_time() -> None:
    metrics = JoinMetrics()
    inputs = {
        "a": iter([_Record(time=_time(1), key="A", value=1)]),
        "b": iter([{"time": "2025-01-01T00:00:00Z", "value": 20}]),
    }

    with pytest.raises(
        ValueError,
        match=(
            "Joined stream 'joined.out' input 'b' from 'stream.b' row 1 "
            "join field 'time' must be a timezone-aware datetime"
        ),
    ):
        list(join_rows(inputs, _spec(fields=("time",)), metrics))


def test_join_rows_normalizes_mapping_time_to_utc() -> None:
    metrics = JoinMetrics()
    primary = {
        "time": datetime(
            2025,
            1,
            1,
            2,
            tzinfo=timezone(timedelta(hours=2)),
        ),
        "value": 1,
    }
    inputs = {
        "a": iter([primary]),
        "b": iter([_Record(time=_time(1), key="A", value=20)]),
    }

    rows = list(join_rows(inputs, _spec(fields=("time",)), metrics))

    assert len(rows) == 1
    assert rows[0].time == _time(1)
    assert rows[0].time.tzinfo is timezone.utc


def test_join_metrics_count_primary_rows_not_cartesian_matches() -> None:
    metrics = JoinMetrics()
    inputs = {
        "a": iter(
            [
                _Record(time=_time(1), key="A", value=1),
                _Record(time=_time(1), key="A", value=2),
                _Record(time=_time(2), key="A", value=3),
            ]
        ),
        "b": iter(
            [
                _Record(time=_time(1), key="A", value=10),
                _Record(time=_time(1), key="A", value=20),
            ]
        ),
    }

    rows = list(join_rows(inputs, _spec(fields=("time",)), metrics))

    assert len(rows) == 4
    assert metrics.primary_rows == 3
    assert metrics.output_rows == 4
    input_metrics = metrics.inputs["b"]
    assert input_metrics.matched_primary_rows == 2
    assert input_metrics.missed_primary_rows == 1
    assert input_metrics.match_rate == pytest.approx(2 / 3)


def test_join_metrics_have_no_rate_without_primary_rows() -> None:
    metrics = JoinMetrics()
    inputs = {
        "a": iter([]),
        "b": iter([_Record(time=_time(1), key="A", value=10)]),
    }

    assert list(join_rows(inputs, _spec(fields=("time",)), metrics)) == []
    assert metrics.inputs["b"].match_rate is None
