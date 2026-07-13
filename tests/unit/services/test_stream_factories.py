from dataclasses import dataclass
from datetime import UTC, datetime

from datapipeline.config.catalog import StreamConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.services.streams.aligned import build_aligned_mapper


@dataclass
class _Record(TemporalRecord):
    id_: str
    value: float


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
