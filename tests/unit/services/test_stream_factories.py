from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

from datapipeline.config.catalog import ContractConfig, EPArgs
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    set_current_execution_event_sink,
)
from datapipeline.domain.record import TemporalRecord
from datapipeline.services.streams.joined import JoinedLoader
from datapipeline.services.streams.manual import ManualLoader


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
        "datapipeline.services.streams.common.cached_record_stream",
        _cached_record_stream,
    )


def _install_join_mapper(monkeypatch):
    def _mapper(row, *, context, **params):
        del context
        del params
        primary = row.values["a"]
        other = row.values.get("b")
        if primary is None:
            return None
        value = primary.value + (0 if other is None else other.value)
        return _Rec(time=primary.time, value=value)

    monkeypatch.setattr(
        "datapipeline.services.streams.joined.load_ep",
        lambda _namespace, _entrypoint: _mapper,
    )


def _joined_spec(*, join: dict) -> ContractConfig:
    return ContractConfig.model_validate(
        {
            "kind": "joined",
            "id": "joined.out",
            "inputs": ["a=stream.a", "b=stream.b"],
            "join": join,
            "mapper": {"entrypoint": "join.mapper", "args": {}},
        }
    )


def test_manual_loader_closes_upstream_iterators_when_closed(monkeypatch) -> None:
    upstream = _ClosableIterator([1, 2, 3])

    def _cached_record_stream(_context, _ref):
        return upstream

    def _mapper(inputs, *, context, driver, **params):
        del context
        del params
        yield from inputs[driver]

    monkeypatch.setattr(
        "datapipeline.services.streams.common.cached_record_stream",
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
    spec = ContractConfig(
        kind="manual",
        id="equity.pair.aapl",
        inputs=["aapl=equity.aapl"],
        mapper=EPArgs(entrypoint="test.mapper", args={}),
    )
    loader = ManualLoader(runtime=runtime, stream_id=spec.id, spec=spec)

    iterator = loader.load()
    assert next(iterator) == 1

    iterator.close()

    assert upstream.closed == 1


def test_joined_loader_matches_same_partition_and_time(monkeypatch) -> None:
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
    _install_join_mapper(monkeypatch)

    rows = list(JoinedLoader(runtime, "joined.out", _joined_spec(join={"primary": "a"})).load())

    assert [row.value for row in rows] == [22]


def test_joined_loader_broadcasts_unpartitioned_input_by_time(monkeypatch) -> None:
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
    _install_join_mapper(monkeypatch)

    rows = list(
        JoinedLoader(
            runtime,
            "joined.out",
            _joined_spec(join={"primary": "a", "broadcast": ["b"]}),
        ).load()
    )

    assert [row.value for row in rows] == [21, 22]


def test_joined_loader_matches_unpartitioned_inputs_by_time(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [_Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)
    _install_join_mapper(monkeypatch)

    rows = list(JoinedLoader(runtime, "joined.out", _joined_spec(join={"primary": "a"})).load())

    assert [row.value for row in rows] == [22]


def test_joined_loader_emits_join_diagnostics(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [_Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)
    _install_join_mapper(monkeypatch)
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        rows = list(
            JoinedLoader(runtime, "joined.out", _joined_spec(join={"primary": "a"})).load()
        )
    finally:
        reset_current_execution_event_sink(token)

    assert [row.value for row in rows] == [22]
    messages = [event.message for event in capture.events]
    assert (
        "[joined.out] join: primary=a on=time mode=inner "
        "primary_rows=2 output_rows=1"
    ) in messages
    assert (
        "[joined.out] join.input: b=stream.b rows=1 matched=1 missed=1 "
        "match_rate=50.0% broadcast=false"
    ) in messages
    assert {event.message_kind for event in capture.events} == {"source_info"}


def test_joined_loader_broadcasts_subset_partition(monkeypatch) -> None:
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
    _install_join_mapper(monkeypatch)

    rows = list(
        JoinedLoader(
            runtime,
            "joined.out",
            _joined_spec(join={"primary": "a", "broadcast": ["b"]}),
        ).load()
    )

    assert [row.value for row in rows] == [21, 22]
