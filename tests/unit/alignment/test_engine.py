from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

import datapipeline.alignment.engine as alignment_engine
from datapipeline.dag import runner as dag_runner
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.dag import Dag
from datapipeline.dag.events import NodeProgressEvent
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.observer import NoopExecutionObserver
from datapipeline.dag.runner import run_dag
from datapipeline.alignment.engine import align_streams
from datapipeline.domain.record import TemporalRecord
from datapipeline.runtime import Runtime


@dataclass
class _Record(TemporalRecord):
    id_: str
    value: str


class _ProgressObserver(NoopExecutionObserver):
    def __init__(self) -> None:
        self.events: list[NodeProgressEvent] = []

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        self.events.append(event)


def _record(id_: str, day: int, value: str) -> _Record:
    return _Record(
        time=datetime(2025, 1, day, tzinfo=UTC),
        id_=id_,
        value=value,
    )


def test_align_streams_sorts_inputs_and_preserves_configured_order() -> None:
    left = [
        _record("B", 2, "left-B2"),
        _record("A", 2, "left-A2"),
        _record("A", 1, "left-A1"),
    ]
    right = [
        _record("A", 1, "right-A1"),
        _record("B", 2, "right-B2"),
        _record("A", 2, "right-A2"),
    ]

    rows = list(
        align_streams(
            [("left", iter(left)), ("right", iter(right))],
            partition_by="id_",
            buffer_bytes=1,
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
            partition_by="id_",
            buffer_bytes=1,
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
            partition_by="id_",
            buffer_bytes=1_000,
        )
    )

    assert rows == []


@pytest.mark.parametrize("duplicate_input", [0, 1, 2])
def test_align_streams_rejects_duplicates_in_every_input(duplicate_input: int) -> None:
    inputs = [
        ("first", [_record("A", 1, "first")]),
        ("second", [_record("A", 1, "second")]),
        ("third", [_record("A", 1, "third")]),
    ]
    stream_id, records = inputs[duplicate_input]
    inputs[duplicate_input] = (
        stream_id,
        [*records, _record("A", 1, f"{stream_id}-duplicate")],
    )

    with pytest.raises(ValueError) as error:
        list(
            align_streams(
                [(name, iter(stream)) for name, stream in inputs],
                partition_by="id_",
                buffer_bytes=1,
            )
        )

    assert str(error.value) == (
        f"Alignment input {stream_id!r} has duplicate canonical key "
        "partition=('A',), time=2025-01-01T00:00:00+00:00"
    )


def test_align_streams_validates_duplicates_after_intersection_is_exhausted() -> None:
    with pytest.raises(ValueError, match="Alignment input 'long'"):
        list(
            align_streams(
                [
                    ("short", iter([_record("A", 1, "short")])),
                    (
                        "long",
                        iter(
                            [
                                _record("A", 1, "match"),
                                _record("A", 3, "first-duplicate"),
                                _record("A", 3, "second-duplicate"),
                            ]
                        ),
                    ),
                ],
                partition_by="id_",
                buffer_bytes=1,
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
                partition_by="id_",
                buffer_bytes=1,
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
        partition_by="id_",
        buffer_bytes=1,
    )

    assert list(rows) == []


def test_alignment_resets_sort_total_before_emitting_rows(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setattr(dag_runner, "_LIVE_PROGRESS_INTERVAL_SECONDS", 0)
    observer = _ProgressObserver()
    context = PipelineContext(
        Runtime(
            project_yaml=tmp_path / "project.yaml",
            artifacts_root=tmp_path / "artifacts",
        )
    )
    context.heartbeat_interval_seconds = 0

    def aligned_records():
        return align_streams(
            [
                (
                    "left",
                    iter(
                        [
                            _record("A", 1, "left-1"),
                            _record("A", 2, "left-2"),
                        ]
                    ),
                ),
                (
                    "right",
                    iter(
                        [
                            _record("A", 1, "right-1"),
                            _record("A", 2, "right-2"),
                        ]
                    ),
                ),
            ],
            partition_by="id_",
            buffer_bytes=10_000,
        )

    dag = Dag(
        name="alignment-progress",
        nodes=(PipelineNode(name="align", op=aligned_records),),
    )

    assert len(list(run_dag(context, dag, observer=observer))) == 2

    progress = [event.progress for event in observer.events]
    sort_total_index = next(
        index
        for index, snapshot in enumerate(progress)
        if snapshot.phase == "emitting" and snapshot.total == 4
    )
    aligning_index = next(
        index
        for index, snapshot in enumerate(
            progress[sort_total_index + 1 :],
            start=sort_total_index + 1,
        )
        if snapshot.phase == "aligning"
    )

    assert progress[aligning_index].total is None
    assert all(snapshot.total is None for snapshot in progress[aligning_index:])
    assert [
        snapshot.completed
        for snapshot in progress[aligning_index:]
        if snapshot.phase == "aligning"
    ] == [0, 1, 2]


def test_align_streams_requires_at_least_two_inputs() -> None:
    with pytest.raises(ValueError, match="at least two input streams"):
        list(
            align_streams(
                [("only", iter([_record("A", 1, "only")]))],
                partition_by="id_",
                buffer_bytes=1,
            )
        )


@pytest.mark.parametrize("exit_kind", ["exhaustion", "error", "early_close"])
def test_align_streams_closes_combined_sorter(monkeypatch, exit_kind: str) -> None:
    closed: list[bool] = []

    def tracking_sort(records, buffer_bytes, key):
        assert buffer_bytes == 37
        position = len(closed)
        closed.append(False)
        try:
            yield from sorted(records, key=key)
        finally:
            closed[position] = True

    monkeypatch.setattr(alignment_engine, "batch_sort", tracking_sort)
    right = [_record("A", 1, "right")]
    if exit_kind == "error":
        right.append(_record("A", 1, "right-duplicate"))

    rows = align_streams(
        [
            ("left", iter([_record("A", 1, "left")])),
            ("right", iter(right)),
        ],
        partition_by="id_",
        buffer_bytes=37,
    )

    if exit_kind == "exhaustion":
        list(rows)
    elif exit_kind == "error":
        with pytest.raises(ValueError, match="duplicate canonical key"):
            list(rows)
    else:
        next(rows)
        rows.close()

    assert closed == [True]
