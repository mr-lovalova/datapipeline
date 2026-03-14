from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from datapipeline.composed import align_many
from datapipeline.domain.record import TemporalRecord


@dataclass
class _Rec(TemporalRecord):
    value: float


@dataclass
class _PartitionedRec(TemporalRecord):
    symbol: str
    value: float


def _t(day: int) -> datetime:
    return datetime(2021, 1, day, tzinfo=timezone.utc)


def test_align_many_inner_join_emits_only_intersections() -> None:
    a = iter([_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2), _Rec(time=_t(4), value=4)])
    b = iter([_Rec(time=_t(2), value=20), _Rec(time=_t(3), value=30), _Rec(time=_t(4), value=40)])

    rows = list(align_many({"a": a, "b": b}, driver="a", join="inner"))

    assert [row.time for row in rows] == [_t(2), _t(4)]
    assert [row.values["a"].value for row in rows if row.values["a"] is not None] == [2, 4]
    assert [row.values["b"].value for row in rows if row.values["b"] is not None] == [20, 40]


def test_align_many_left_join_keeps_driver_ticks() -> None:
    a = iter([_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2), _Rec(time=_t(4), value=4)])
    b = iter([_Rec(time=_t(2), value=20), _Rec(time=_t(4), value=40)])

    rows = list(align_many({"a": a, "b": b}, driver="a", join="left"))

    assert [row.time for row in rows] == [_t(1), _t(2), _t(4)]
    assert rows[0].values["b"] is None
    assert rows[1].values["b"] is not None and rows[1].values["b"].value == 20
    assert rows[2].values["b"] is not None and rows[2].values["b"].value == 40


def test_align_many_rejects_unknown_driver() -> None:
    a = iter([_Rec(time=_t(1), value=1)])
    with pytest.raises(ValueError, match="Unknown driver alias"):
        list(align_many({"a": a}, driver="missing"))


def test_align_many_rejects_unknown_join_mode() -> None:
    a = iter([_Rec(time=_t(1), value=1)])
    with pytest.raises(ValueError, match="Unsupported join mode"):
        list(align_many({"a": a}, join="outer"))


def test_align_many_inner_join_respects_partition_key() -> None:
    a = iter(
        [
            _PartitionedRec(time=_t(1), symbol="AAPL", value=1),
            _PartitionedRec(time=_t(1), symbol="MSFT", value=2),
        ]
    )
    b = iter([_PartitionedRec(time=_t(1), symbol="MSFT", value=20)])

    rows = list(
        align_many(
            {"a": a, "b": b},
            driver="a",
            join="inner",
            partition_by="symbol",
        )
    )

    assert len(rows) == 1
    assert rows[0].values["a"] is not None
    assert rows[0].values["b"] is not None
    assert rows[0].values["a"].symbol == "MSFT"
    assert rows[0].values["b"].symbol == "MSFT"


def test_align_many_left_join_respects_partition_key() -> None:
    a = iter(
        [
            _PartitionedRec(time=_t(1), symbol="AAPL", value=1),
            _PartitionedRec(time=_t(1), symbol="MSFT", value=2),
        ]
    )
    b = iter([_PartitionedRec(time=_t(1), symbol="MSFT", value=20)])

    rows = list(
        align_many(
            {"a": a, "b": b},
            driver="a",
            join="left",
            partition_by="symbol",
        )
    )

    assert len(rows) == 2
    assert rows[0].values["a"] is not None and rows[0].values["a"].symbol == "AAPL"
    assert rows[0].values["b"] is None
    assert rows[1].values["a"] is not None and rows[1].values["a"].symbol == "MSFT"
    assert rows[1].values["b"] is not None and rows[1].values["b"].symbol == "MSFT"
