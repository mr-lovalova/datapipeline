import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Literal

import pytest

from datapipeline.config.catalog import EPArgs, StreamConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.joined import JoinedRow
from datapipeline.services.streams.ingest import build_mapper_from_spec
from datapipeline.services.streams.joined import build_joined_stream
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


@dataclass
class _NullableTickerRec(TemporalRecord):
    ticker: str | None
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
    def _open_record_stream(_context, ref):
        return iter(runtime._streams[ref])

    monkeypatch.setattr(
        "datapipeline.pipelines.record.inputs.open_record_stream",
        _open_record_stream,
    )


def _joined_stream(
    runtime,
    *,
    on: str | list[str] = "time",
    mode: Literal["inner", "left"] = "inner",
):
    config = StreamConfig.model_validate(
        {
            "id": "joined.out",
            "from": {
                "join": {"a": "stream.a", "b": "stream.b"},
                "primary": "a",
                "on": on,
                "mode": mode,
            },
            "map": {"entrypoint": "join.mapper", "args": {}},
        }
    )
    return build_joined_stream(
        "joined.out",
        config,
        runtime,
    )


def test_manual_stream_closes_upstream_iterators_when_closed(monkeypatch) -> None:
    upstream = _ClosableIterator([1, 2, 3])

    def _open_record_stream(_context, _ref):
        return upstream

    def _mapper(inputs, *, context, driver, **params):
        del context
        del params
        yield from inputs[driver]

    monkeypatch.setattr(
        "datapipeline.pipelines.record.inputs.open_record_stream",
        _open_record_stream,
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

    rows = list(_joined_stream(runtime, on=["ticker", "time"]).stream())

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

    rows = list(_joined_stream(runtime).stream())

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

    rows = list(_joined_stream(runtime).stream())

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

    rows = list(_joined_stream(runtime).stream())

    assert len(rows) == 1
    assert rows[0].values["a"].value == 2
    assert rows[0].values["b"].value == 20


def test_joined_stream_logs_join_diagnostics(monkeypatch, caplog) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [_Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)
    caplog.set_level(logging.INFO, logger="datapipeline.services.streams.joined")

    rows = list(_joined_stream(runtime).stream())

    assert len(rows) == 1
    assert rows[0].values["a"].value == 2
    assert rows[0].values["b"].value == 20
    assert (
        "[joined.out] join: primary=a on=time mode=inner "
        "primary_rows=2 output_rows=1"
    ) in caplog.messages
    assert (
        "[joined.out] join.input: b=stream.b rows=1 "
        "matched_primary_rows=1 missed_primary_rows=1 match_rate=50.0%"
    ) in caplog.messages


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

    rows = list(_joined_stream(runtime, on=["ticker", "time"]).stream())

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

    rows = list(_joined_stream(runtime).stream())

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

    rows = list(_joined_stream(runtime, mode="left").stream())

    assert [row.values["a"].value for row in rows] == [1, 2]
    assert [row.values["b"].value for row in rows] == [20, 20]


def test_joined_stream_rejects_missing_join_field(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1)],
            "stream.b": [_Rec(time=_t(1), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    with pytest.raises(
        ValueError,
        match=(
            "Joined stream 'joined.out' input 'b' from 'stream.b' row 1 "
            "is missing join field 'ticker'"
        ),
    ):
        list(_joined_stream(runtime, on="ticker").stream())


def test_joined_stream_preserves_explicit_null_join_key(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [
                _NullableTickerRec(time=_t(1), ticker=None, value=1),
            ],
            "stream.b": [
                _NullableTickerRec(time=_t(1), ticker=None, value=20),
            ],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(_joined_stream(runtime, on=["ticker", "time"]).stream())

    assert len(rows) == 1
    assert rows[0].values["b"].value == 20


def test_joined_stream_left_join_keeps_unmatched_primary(monkeypatch) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [_Rec(time=_t(2), value=20)],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)

    rows = list(_joined_stream(runtime, mode="left").stream())

    assert [row.values["a"].value for row in rows] == [1, 2]
    assert [row.values["b"] for row in rows] == [
        None,
        runtime._streams["stream.b"][0],
    ]


def test_joined_stream_diagnostics_count_primary_matches(monkeypatch, caplog) -> None:
    runtime = _runtime(
        streams={
            "stream.a": [_Rec(time=_t(1), value=1), _Rec(time=_t(2), value=2)],
            "stream.b": [
                _Rec(time=_t(1), value=10),
                _Rec(time=_t(1), value=20),
            ],
        },
        partitions={"stream.a": None, "stream.b": None},
    )
    _install_streams(monkeypatch, runtime)
    caplog.set_level(logging.INFO, logger="datapipeline.services.streams.joined")

    rows = list(_joined_stream(runtime).stream())

    assert len(rows) == 2
    assert (
        "[joined.out] join.input: b=stream.b rows=2 "
        "matched_primary_rows=1 missed_primary_rows=1 match_rate=50.0%"
    ) in caplog.messages
