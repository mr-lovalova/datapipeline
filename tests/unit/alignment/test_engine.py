from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from datapipeline.alignment.engine import align_streams
from datapipeline.domain.record import TemporalRecord


@dataclass
class _Record(TemporalRecord):
    id_: str
    value: str


def _record(id_: str, day: int, value: str) -> _Record:
    return _Record(
        time=datetime(2025, 1, day, tzinfo=UTC),
        id_=id_,
        value=value,
    )


def test_align_streams_intersects_ordered_inputs_in_configured_order() -> None:
    left = [
        _record("A", 1, "left-A1"),
        _record("A", 2, "left-A2"),
        _record("B", 2, "left-B2"),
    ]
    right = [
        _record("A", 1, "right-A1"),
        _record("A", 2, "right-A2"),
        _record("B", 2, "right-B2"),
    ]

    rows = list(
        align_streams(
            [("left", iter(left)), ("right", iter(right))],
            partition_by=("id_",),
        )
    )

    assert [[record.value for record in row] for row in rows] == [
        ["left-A1", "right-A1"],
        ["left-A2", "right-A2"],
        ["left-B2", "right-B2"],
    ]


def test_align_streams_intersects_three_inputs() -> None:
    rows = list(
        align_streams(
            [
                (
                    "first",
                    iter(
                        [
                            _record("A", 1, "first-1"),
                            _record("A", 2, "first-2"),
                            _record("A", 4, "first-4"),
                        ]
                    ),
                ),
                (
                    "second",
                    iter(
                        [
                            _record("A", 2, "second-2"),
                            _record("A", 3, "second-3"),
                            _record("A", 4, "second-4"),
                        ]
                    ),
                ),
                (
                    "third",
                    iter(
                        [
                            _record("A", 1, "third-1"),
                            _record("A", 2, "third-2"),
                            _record("A", 4, "third-4"),
                            _record("A", 5, "third-5"),
                        ]
                    ),
                ),
            ],
            partition_by=("id_",),
        )
    )

    assert [[record.value for record in row] for row in rows] == [
        ["first-2", "second-2", "third-2"],
        ["first-4", "second-4", "third-4"],
    ]


def test_align_streams_skips_keys_without_a_complete_match() -> None:
    rows = list(
        align_streams(
            [
                ("left", iter([_record("A", 1, "left")])),
                ("right", iter([_record("A", 2, "right")])),
            ],
            partition_by=("id_",),
        )
    )

    assert rows == []


@pytest.mark.parametrize("duplicate_input", [0, 1, 2])
def test_align_streams_rejects_duplicates(duplicate_input: int) -> None:
    inputs = [
        (
            "first",
            [_record("A", 1, "first-1"), _record("A", 2, "first-2")],
        ),
        (
            "second",
            [_record("A", 1, "second-1"), _record("A", 2, "second-2")],
        ),
        (
            "third",
            [_record("A", 1, "third-1"), _record("A", 2, "third-2")],
        ),
    ]
    stream_id, records = inputs[duplicate_input]
    inputs[duplicate_input] = (
        stream_id,
        [records[0], _record("A", 1, f"{stream_id}-duplicate"), records[1]],
    )

    with pytest.raises(ValueError) as error:
        list(
            align_streams(
                [(name, iter(stream)) for name, stream in inputs],
                partition_by=("id_",),
            )
        )

    assert str(error.value) == (
        f"Alignment input {stream_id!r} has duplicate canonical key "
        "partition=('A',), time=2025-01-01T00:00:00+00:00"
    )


@pytest.mark.parametrize("unordered_input", [0, 1])
def test_align_streams_rejects_unordered_inputs(unordered_input: int) -> None:
    inputs = [
        (
            "left",
            [_record("A", 2, "left-2"), _record("A", 3, "left-3")],
        ),
        (
            "right",
            [_record("A", 2, "right-2"), _record("A", 3, "right-3")],
        ),
    ]
    stream_id, records = inputs[unordered_input]
    inputs[unordered_input] = (
        stream_id,
        [records[0], _record("A", 1, f"{stream_id}-1")],
    )

    with pytest.raises(
        ValueError, match=f"Alignment input {stream_id!r} is not ordered"
    ):
        list(
            align_streams(
                [(name, iter(stream)) for name, stream in inputs],
                partition_by=("id_",),
            )
        )


@pytest.mark.parametrize("empty_input", [0, 1, 2])
def test_align_streams_returns_empty_when_any_input_is_empty(empty_input: int) -> None:
    inputs = [
        ("first", [_record("A", 1, "first")]),
        ("second", [_record("A", 1, "second")]),
        ("third", [_record("A", 1, "third")]),
    ]
    stream_id, _ = inputs[empty_input]
    inputs[empty_input] = (stream_id, [])

    assert (
        list(
            align_streams(
                [(name, iter(stream)) for name, stream in inputs],
                partition_by=("id_",),
            )
        )
        == []
    )


def test_align_streams_stops_opening_inputs_after_an_empty_stream() -> None:
    def unused_records():
        raise AssertionError("input after empty stream must not be consumed")
        yield

    rows = align_streams(
        [
            ("empty", iter(())),
            ("unused", unused_records()),
        ],
        partition_by=("id_",),
    )

    assert list(rows) == []


def test_align_streams_stops_when_an_input_is_exhausted() -> None:
    def unused_tail():
        yield _record("A", 1, "right")
        raise AssertionError("tail after exhausted input must not be consumed")

    rows = align_streams(
        [
            ("short", iter([_record("A", 1, "left")])),
            ("long", unused_tail()),
        ],
        partition_by=("id_",),
    )

    assert [[record.value for record in row] for row in rows] == [["left", "right"]]


def test_align_streams_closes_open_input_when_another_input_is_empty() -> None:
    closed = False

    def opened_input():
        nonlocal closed
        try:
            yield _record("A", 1, "left")
        finally:
            closed = True

    assert (
        list(
            align_streams(
                [("opened", opened_input()), ("empty", iter(()))],
                partition_by=("id_",),
            )
        )
        == []
    )
    assert closed


def test_align_streams_closes_inputs_when_consumer_stops() -> None:
    closed: set[str] = set()

    def records(stream_id: str):
        try:
            yield _record("A", 1, stream_id)
            yield _record("A", 2, stream_id)
        finally:
            closed.add(stream_id)

    aligned = align_streams(
        [("left", records("left")), ("right", records("right"))],
        partition_by=("id_",),
    )
    next(aligned)
    aligned.close()

    assert closed == {"left", "right"}


def test_align_streams_requires_at_least_two_inputs() -> None:
    class Records:
        def __init__(self) -> None:
            self.closed = False

        def __iter__(self):
            return self

        def __next__(self) -> _Record:
            return _record("A", 1, "only")

        def close(self) -> None:
            self.closed = True

    records = Records()

    with pytest.raises(ValueError, match="at least two input streams"):
        list(
            align_streams(
                [("only", records)],
                partition_by=("id_",),
            )
        )
    assert records.closed
