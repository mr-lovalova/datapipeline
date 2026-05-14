from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from datapipeline.config.catalog import StreamConfig, EPArgs
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    set_current_execution_event_sink,
)
from datapipeline.domain.record import TemporalRecord
from datapipeline.joined import JoinedRow
from datapipeline.services.streams.ingest import build_mapper_from_spec
from datapipeline.services.streams.joined import JoinedStream
from datapipeline.services.streams.manual import ManualStream


@dataclass
class _Rec(TemporalRecord):
    value: float


@dataclass
class _TickerRec(TemporalRecord):
    ticker: str
    value: float


@dataclass
class _TickerHorizonRec(TemporalRecord):
    ticker: str
    horizon: int
    value: float


class _Registry:
    def __init__(self, values) -> None:
        self._values = dict(values)

    def keys(self):
        return tuple(self._values.keys())

    def get(self, key):
        return self._values[key]


class _ClosableIterator:
    def __init__(self, values):
        self._values = iter(values)
        self.closed = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._values)

    def close(self):
        self.closed += 1


class _CaptureSink:
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


def _t(day: int) -> datetime:
    return datetime(2021, 1, day, tzinfo=timezone.utc)


def _runtime(*, streams: dict[str, list[TemporalRecord]], partitions: dict[str, object]):
    return SimpleNamespace(
        _streams=streams,
        registries=SimpleNamespace(
            stream_sources=_Registry({key: object() for key in streams}),
            partition_by=_Registry(partitions),
        ),
    )


def _install_streams(monkeypatch, runtime) -> None:
    def _cached_record_stream(_context, ref):
        return iter(runtime._streams[ref])

    monkeypatch.setattr(
        "datapipeline.pipelines.record.inputs.cached_record_stream",
        _cached_record_stream,
    )


def _joined_spec(*, join: dict) -> StreamConfig:
    from_cfg = {
        "join": {"a": "stream.a", "b": "stream.b"},
        "primary": join.get("primary"),
        "on": join.get("on", "time"),
        "mode": join.get("mode", "inner"),
    }
    return StreamConfig.model_validate(
        {
            "id": "joined.out",
            "from": from_cfg,
            "map": {"entrypoint": "join.mapper", "args": {}},
        }
    )


def test_manual_stream_closes_upstream_iterators_when_closed(monkeypatch) -> None:
    upstream = _ClosableIterator([1, 2, 3])

    def _cached_record_stream(_context, _ref):
        return upstream

    def _mapper(inputs, *, context, driver, **params):
        del context
        del params
        yield from inputs[driver]

    monkeypatch.setattr(
        "datapipeline.pipelines.record.inputs.cached_record_stream",
        _cached_record_stream,
    )
    monkeypatch.setattr(
        "datapipeline.services.streams.manual.load_ep",
        lambda _namespace, _entrypoint: _mapper,
    )

    runtime = SimpleNamespace(
        registries=SimpleNamespace(
            stream_sources=_Registry({"equity.aapl": object()}),
        )
    )
    spec = StreamConfig(
        id="equity.pair.aapl",
        from_={"streams": {"aapl": "equity.aapl"}},
        map=EPArgs(entrypoint="test.mapper", args={}),
    )
    stream = ManualStream(runtime=runtime, stream_id=spec.id, spec=spec)

    iterator = stream.stream()
    assert next(iterator) == 1

    iterator.close()

    assert upstream.closed == 1


def test_row_mapper_adapter_maps_joined_rows(monkeypatch) -> None:
    def _mapper(row, *, context, **params):
        del context
        return _Rec(time=row.time, value=row.values["a"].value + params["offset"])

    monkeypatch.setattr(
        "datapipeline.services.streams.ingest.load_ep",
        lambda _namespace, _entrypoint: _mapper,
    )

    mapper = build_mapper_from_spec(
        EPArgs(entrypoint="join.mapper", args={"offset": 3}),
        runtime=SimpleNamespace(),
        row_mapper=True,
    )
    rows = [
        JoinedRow(time=_t(1), values={"a": _Rec(time=_t(1), value=2)}),
    ]

    assert [record.value for record in mapper(rows)] == [5]


def test_joined_stream_matches_same_partition_and_time(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [
                _TickerRec(time=_t(1), ticker="AAPL", value=1),
                _TickerRec(time=_t(1), ticker="MSFT", value=2),
            ],
            "stream.b": [
                _TickerRec(time=_t(1), ticker="MSFT", value=20),
            ],
        },
        partitions={"stream.a": "ticker", "stream.b": "ticker"},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime,
            "joined.out",
            _joined_spec(join={"primary": "a", "on": ["ticker", "time"]}),
        ).stream()
    )

    assert len(rows) == 1
    assert rows[0].values["a"].ticker == "MSFT"
    assert rows[0].values["b"].value == 20


def test_joined_stream_joins_partitioned_to_unpartitioned_by_time(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [
                _TickerRec(time=_t(1), ticker="AAPL", value=1),
                _TickerRec(time=_t(1), ticker="MSFT", value=2),
            ],
            "stream.b": [_Rec(time=_t(1), value=20)],
        },
        partitions={"stream.a": "ticker", "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime,
            "joined.out",
            _joined_spec(join={"primary": "a"}),
        ).stream()
    )

    assert [row.values["a"].value for row in rows] == [1, 2]
    assert [row.values["b"].value for row in rows] == [20, 20]


def test_joined_stream_reuses_unpartitioned_matches_across_partitions(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [
                _TickerRec(time=_t(1), ticker="AAPL", value=1),
                _TickerRec(time=_t(2), ticker="AAPL", value=2),
                _TickerRec(time=_t(1), ticker="MSFT", value=3),
            ],
            "stream.b": [_Rec(time=_t(1), value=10), _Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": "ticker", "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime,
            "joined.out",
            _joined_spec(join={"primary": "a"}),
        ).stream()
    )

    assert [row.values["a"].value for row in rows] == [1, 2, 3]
    assert [row.values["b"].value for row in rows] == [10, 20, 10]


def test_joined_stream_matches_unpartitioned_inputs_by_time(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [_Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime, "joined.out", _joined_spec(join={"primary": "a"})
        ).stream()
    )

    assert len(rows) == 1
    assert rows[0].values["a"].value == 2
    assert rows[0].values["b"].value == 20


def test_joined_stream_emits_join_diagnostics(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [_Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        rows = list(
            JoinedStream(
                runtime, "joined.out", _joined_spec(join={"primary": "a"})
            ).stream()
        )
    finally:
        reset_current_execution_event_sink(token)

    assert len(rows) == 1
    assert rows[0].values["a"].value == 2
    assert rows[0].values["b"].value == 20
    messages = [event.message for event in capture.events]
    assert (
        "[joined.out] join: primary=a on=time mode=inner "
        "primary_rows=2 output_rows=1"
    ) in messages
    assert (
        "[joined.out] join.input: b=stream.b rows=1 matched=1 missed=1 match_rate=50.0%"
    ) in messages
    assert {event.message_kind for event in capture.events} == {"source_info"}


def test_joined_stream_can_join_on_subset_of_partition_fields(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [
                _TickerHorizonRec(time=_t(1), ticker="AAPL", horizon=63, value=1),
                _TickerHorizonRec(time=_t(1), ticker="AAPL", horizon=126, value=2),
            ],
            "stream.b": [_TickerRec(time=_t(1), ticker="AAPL", value=20)],
        },
        partitions={"stream.a": ["ticker", "horizon"], "stream.b": "ticker"},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime,
            "joined.out",
            _joined_spec(join={"primary": "a", "on": ["ticker", "time"]}),
        ).stream()
    )

    assert [row.values["a"].value for row in rows] == [1, 2]
    assert [row.values["b"].value for row in rows] == [20, 20]


def test_joined_stream_emits_many_to_many_matches(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(1), value=2)],
            "stream.b": [_Rec(time=_t(1), value=10), _Rec(time=_t(1), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime, "joined.out", _joined_spec(join={"primary": "a"})
        ).stream()
    )

    assert [(row.values["a"].value, row.values["b"].value) for row in rows] == [
        (1, 10),
        (1, 20),
        (2, 10),
        (2, 20),
    ]


def test_joined_stream_allows_duplicate_primary_join_key(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(1), value=2)],
            "stream.b": [_Rec(time=_t(1), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(
        JoinedStream(
            runtime, "joined.out", _joined_spec(join={"primary": "a", "mode": "left"})
        ).stream()
    )

    assert [row.values["a"].value for row in rows] == [1, 2]
    assert [row.values["b"].value for row in rows] == [20, 20]
