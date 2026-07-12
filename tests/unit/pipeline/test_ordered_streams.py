from dataclasses import dataclass
from datetime import datetime, timezone

import datapipeline.pipelines.shared.record_nodes as record_nodes
import pytest
from datapipeline.config.execution import ExecutionConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.shared.record_nodes import order_records
from datapipeline.runtime import Runtime


@dataclass
class _Record:
    time: datetime
    security_id: str


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _context(tmp_path):
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    return PipelineContext(
        Runtime(
            project_yaml=project_yaml,
            artifacts_root=tmp_path / "artifacts",
            execution=ExecutionConfig(),
        )
    )


def test_order_records_skips_sort_when_ordered_by_matches_required_order(
    monkeypatch,
    tmp_path,
) -> None:
    called = False

    def _batch_sort(*args, **kwargs):
        nonlocal called
        called = True
        return ()

    monkeypatch.setattr(record_nodes, "batch_sort", _batch_sort)
    records = [
        _Record(time=_ts(1), security_id="AAPL"),
        _Record(time=_ts(1), security_id="AAPL"),
        _Record(time=_ts(2), security_id="MSFT"),
    ]

    ordered = list(
        order_records(
            _context(tmp_path),
            ["security_id"],
            ["security_id", "time"],
            records,
        )
    )

    assert ordered == records
    assert all(actual is expected for actual, expected in zip(ordered, records))
    assert called is False


@pytest.mark.parametrize(
    "rows",
    [
        pytest.param([(2, "AAPL"), (1, "AAPL")], id="time-regresses"),
        pytest.param([(1, "AAPL"), (1, "MSFT"), (2, "AAPL")], id="partition-reappears"),
        pytest.param(
            [(1, float("nan")), (2, float("nan"))], id="unordered-partition-key"
        ),
    ],
)
def test_order_records_rejects_false_declaration(tmp_path, rows) -> None:
    records = [
        _Record(time=_ts(day), security_id=security_id) for day, security_id in rows
    ]
    with pytest.raises(ValueError, match="violates declared ordered_by"):
        list(
            order_records(
                _context(tmp_path),
                ["security_id"],
                ["security_id", "time"],
                records,
            )
        )


def test_order_records_sorts_when_ordered_by_does_not_match(tmp_path) -> None:
    records = [
        _Record(time=_ts(2), security_id="MSFT"),
        _Record(time=_ts(1), security_id="AAPL"),
    ]

    ordered = list(
        order_records(
            _context(tmp_path),
            ["security_id"],
            ["time"],
            records,
        )
    )

    assert [(rec.security_id, rec.time.day) for rec in ordered] == [
        ("AAPL", 1),
        ("MSFT", 2),
    ]
