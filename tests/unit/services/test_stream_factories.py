from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace

from datapipeline.config.catalog import StreamConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.services.streams.aligned import (
    AlignedStream,
    build_aligned_mapper,
)


@dataclass
class _Record(TemporalRecord):
    id_: str
    value: float


class _ClosableIterator:
    def __init__(self, values) -> None:
        self._values = iter(values)
        self.closed = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._values)

    def close(self) -> None:
        self.closed += 1


def _record(day: int, value: float) -> _Record:
    return _Record(
        time=datetime(2025, 1, day, tzinfo=UTC),
        id_="A",
        value=value,
    )


def _config() -> StreamConfig:
    return StreamConfig.model_validate(
        {
            "id": "aligned.out",
            "from": {"align": ["stream.a", "stream.b"]},
            "map": {"entrypoint": "calculate", "args": {"offset": 3}},
        }
    )


def _runtime() -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(sort_buffer_bytes=1),
    )


def test_aligned_stream_opens_inputs_in_configured_order(monkeypatch) -> None:
    streams = {
        "stream.a": [_record(2, 2), _record(1, 1)],
        "stream.b": [_record(1, 10), _record(2, 20)],
    }

    def open_stream(_context, stream_id):
        yield from streams[stream_id]

    monkeypatch.setattr(
        "datapipeline.services.streams.aligned.open_record_stream",
        open_stream,
    )

    rows = list(
        AlignedStream(
            _runtime(),
            _config().input_streams(),
            "id_",
        ).stream()
    )

    assert [[record.value for record in row] for row in rows] == [
        [1, 10],
        [2, 20],
    ]


def test_aligned_stream_closes_upstream_iterators(monkeypatch) -> None:
    streams = {
        "stream.a": _ClosableIterator([_record(1, 1), _record(2, 2)]),
        "stream.b": _ClosableIterator([_record(1, 10), _record(2, 20)]),
    }
    monkeypatch.setattr(
        "datapipeline.services.streams.aligned.open_record_stream",
        lambda _context, stream_id: streams[stream_id],
    )
    records = AlignedStream(
        _runtime(),
        _config().input_streams(),
        "id_",
    ).stream()

    next(records)
    records.close()

    assert streams["stream.a"].closed == 1
    assert streams["stream.b"].closed == 1


def test_aligned_mapper_calls_function_with_positional_records(monkeypatch) -> None:
    def calculate(left, right, offset):
        return _record(1, left.value + right.value + offset)

    monkeypatch.setattr(
        "datapipeline.services.streams.aligned.load_ep",
        lambda _group, _entrypoint: calculate,
    )
    mapper = build_aligned_mapper(_config())

    records = list(mapper(iter([(_record(1, 1), _record(1, 10))])))

    assert [record.value for record in records] == [14]


def test_aligned_mapper_drops_none(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.services.streams.aligned.load_ep",
        lambda _group, _entrypoint: lambda _left, _right, offset: None,
    )
    mapper = build_aligned_mapper(_config())

    assert list(mapper(iter([(_record(1, 1), _record(1, 10))]))) == []
