import json
from datetime import datetime, timezone

import pytest

from datapipeline.config.tasks import TicksTask
from datapipeline.domain.record import TemporalRecord
from datapipeline.operations.artifacts.ticks import materialize_ticks
from datapipeline.runtime import Runtime, StreamRuntimeSpec
from datapipeline.transforms.spec import TransformSpec


class _Source:
    def __init__(self, rows):
        self.rows = rows

    def stream(self):
        return iter(self.rows)


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


def _record(
    hour: int,
    security_id: str | None = None,
    *,
    minute: int = 0,
) -> TemporalRecord:
    record = TemporalRecord(time=_ts(hour, minute))
    if security_id is not None:
        record.security_id = security_id
    return record


def _identity(records):
    yield from records


def _runtime(tmp_path) -> Runtime:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    regs = runtime.registries
    regs.stream_specs.register("source.stream", StreamRuntimeSpec(pipeline="ingest"))
    regs.stream_sources.register(
        "source.stream",
        _Source([_record(2), _record(0), _record(2)]),
    )
    regs.mappers.register("source.stream", _identity)
    regs.record_operations.register("source.stream", [])
    regs.stream_operations.register("source.stream", [])
    regs.debug_operations.register("source.stream", [])
    regs.partition_by.register("source.stream", None)
    regs.feature_id_by.register("source.stream", None)
    regs.ordered_by.register("source.stream", None)
    regs.sort_batch_size.register("source.stream", 1)
    return runtime


def _stream_runtime(tmp_path) -> Runtime:
    runtime = _runtime(tmp_path)
    regs = runtime.registries
    regs.stream_specs.register("derived.stream", StreamRuntimeSpec(pipeline="stream"))
    regs.stream_sources.register(
        "derived.stream",
        _Source([_record(0), _record(1)]),
    )
    regs.mappers.register("derived.stream", _identity)
    regs.record_operations.register("derived.stream", [])
    regs.stream_operations.register(
        "derived.stream",
        [TransformSpec(name="floor_time", params={"cadence": "1h"})],
    )
    regs.debug_operations.register(
        "derived.stream",
        [TransformSpec(name="missing_debug", params={})],
    )
    regs.partition_by.register("derived.stream", None)
    regs.feature_id_by.register("derived.stream", None)
    regs.ordered_by.register("derived.stream", None)
    regs.sort_batch_size.register("derived.stream", 1)
    return runtime


def test_materialize_ticks_writes_sorted_unique_tick_rows(tmp_path) -> None:
    runtime = _runtime(tmp_path)

    result = materialize_ticks(
        runtime,
        TicksTask(
            id="dataset_ticks",
            entrypoint="core.artifact.ticks",
            stream="source.stream",
            output="build/dataset_ticks.jsonl",
        ),
    )

    path = runtime.artifacts_root / result.relative_path
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [
        {"time": "2024-01-01T00:00:00Z"},
        {"time": "2024-01-01T02:00:00Z"},
    ]
    assert result.meta == {"rows": 2, "stream": "source.stream", "grid_by": []}


def test_materialize_ticks_writes_keyed_grid_rows(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    runtime.registries.stream_sources.register(
        "source.stream",
        _Source(
            [
                _record(1, "MSFT"),
                _record(0, "AAPL"),
                _record(1, "AAPL"),
            ]
        ),
    )

    result = materialize_ticks(
        runtime,
        TicksTask(
            id="model_grid",
            entrypoint="core.artifact.ticks",
            stream="source.stream",
            grid_by=["security_id"],
            output="build/model_grid.jsonl",
        ),
    )

    path = runtime.artifacts_root / result.relative_path
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [
        {"time": "2024-01-01T00:00:00Z", "security_id": "AAPL"},
        {"time": "2024-01-01T01:00:00Z", "security_id": "AAPL"},
        {"time": "2024-01-01T01:00:00Z", "security_id": "MSFT"},
    ]
    assert result.meta == {
        "rows": 3,
        "stream": "source.stream",
        "grid_by": ["security_id"],
    }


def test_materialize_ticks_rejects_missing_key_field(tmp_path) -> None:
    runtime = _runtime(tmp_path)
    runtime.registries.stream_sources.register(
        "source.stream",
        _Source([_record(0)]),
    )

    with pytest.raises(ValueError, match="missing grid_by field 'security_id'"):
        materialize_ticks(
            runtime,
            TicksTask(
                id="model_grid",
                entrypoint="core.artifact.ticks",
                stream="source.stream",
                grid_by=["security_id"],
                output="build/model_grid.jsonl",
            ),
        )


def test_materialize_ticks_uses_stream_transforms_but_excludes_debug(
    tmp_path,
) -> None:
    runtime = _stream_runtime(tmp_path)
    runtime.registries.stream_sources.register(
        "derived.stream",
        _Source([_record(2, minute=30), _record(0, minute=30)]),
    )

    result = materialize_ticks(
        runtime,
        TicksTask(
            id="derived_ticks",
            entrypoint="core.artifact.ticks",
            stream="derived.stream",
            output="build/derived_ticks.jsonl",
        ),
    )

    path = runtime.artifacts_root / result.relative_path
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert rows == [
        {"time": "2024-01-01T00:00:00Z"},
        {"time": "2024-01-01T02:00:00Z"},
    ]
