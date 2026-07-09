import json
from datetime import datetime, timezone

import pytest

from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime
from datapipeline.transforms.stream.ensure_ticks import EnsureCadenceTransform
from tests.unit.transforms.helpers import make_time_record


def _ts(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc)


def _context(tmp_path) -> PipelineContext:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    return PipelineContext(Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root))


def _write_ticks(context: PipelineContext, artifact_id: str, hours: list[int]) -> None:
    path = context.runtime.artifacts_root / f"{artifact_id}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for hour in hours:
            handle.write(json.dumps({"time": _ts(hour).isoformat()}))
            handle.write("\n")
    context.runtime.artifacts.register(artifact_id, path.name, meta={"grid_by": []})


def _write_keyed_ticks(
    context: PipelineContext,
    artifact_id: str,
    rows: list[tuple[int, str]],
) -> None:
    path = context.runtime.artifacts_root / f"{artifact_id}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for hour, security_id in rows:
            handle.write(
                json.dumps(
                    {
                        "time": _ts(hour).isoformat(),
                        "security_id": security_id,
                    }
                )
            )
            handle.write("\n")
    context.runtime.artifacts.register(
        artifact_id,
        path.name,
        meta={"grid_by": ["security_id"]},
    )


def _write_tick_rows(
    context: PipelineContext,
    artifact_id: str,
    rows: list[dict],
    *,
    meta: dict | None = None,
) -> None:
    path = context.runtime.artifacts_root / f"{artifact_id}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")
    context.runtime.artifacts.register(
        artifact_id,
        path.name,
        meta={"grid_by": []} if meta is None else meta,
    )


def _security_record(value: float | None, hour: int, security_id: str):
    record = make_time_record(value, hour)
    record.security_id = security_id
    return record


def _security_venue_record(
    value: float | None,
    hour: int,
    security_id: str,
    venue: str,
):
    record = _security_record(value, hour, security_id)
    record.venue = venue
    return record


def test_ensure_cadence_uses_duration_cadence_unchanged() -> None:
    stream = iter([make_time_record(1.0, 0), make_time_record(2.0, 2)])

    records = list(EnsureCadenceTransform(cadence="1h", field="value").apply(stream))

    assert [(record.time.hour, record.value) for record in records] == [
        (0, 1.0),
        (1, None),
        (2, 2.0),
    ]


@pytest.mark.parametrize("cadence", ["0m", "-1h"])
def test_ensure_cadence_rejects_nonpositive_duration(cadence: str) -> None:
    stream = iter([make_time_record(1.0, 0)])

    with pytest.raises(ValueError, match="cadence duration must be positive"):
        list(EnsureCadenceTransform(cadence=cadence, field="value").apply(stream))


def test_ensure_cadence_uses_tick_artifact_and_extends_after_last_record(tmp_path) -> None:
    context = _context(tmp_path)
    _write_ticks(context, "dataset_ticks", [0, 1, 2, 3])
    stream = iter([make_time_record(1.0, 0), make_time_record(2.0, 2)])

    transform = EnsureCadenceTransform(
        cadence="dataset_ticks",
        field="value",
        context=context,
    )
    records = list(transform.apply(stream))

    assert [(record.time.hour, record.value) for record in records] == [
        (0, 1.0),
        (1, None),
        (2, 2.0),
        (3, None),
    ]


def test_ensure_cadence_uses_keyed_tick_artifact_per_partition(tmp_path) -> None:
    context = _context(tmp_path)
    _write_keyed_ticks(
        context,
        "model_grid",
        [(0, "AAPL"), (1, "AAPL"), (2, "AAPL"), (3, "AAPL"), (1, "MSFT")],
    )
    stream = iter([_security_record(2.0, 2, "AAPL")])

    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    records = list(transform.apply(stream))

    assert [(record.security_id, record.time.hour, record.value) for record in records] == [
        ("AAPL", 0, None),
        ("AAPL", 1, None),
        ("AAPL", 2, 2.0),
        ("AAPL", 3, None),
    ]


def test_ensure_cadence_handles_multiple_partitions_from_one_grid(tmp_path) -> None:
    context = _context(tmp_path)
    _write_keyed_ticks(
        context,
        "model_grid",
        [
            (0, "AAPL"),
            (1, "AAPL"),
            (2, "AAPL"),
            (0, "MSFT"),
            (1, "MSFT"),
            (2, "MSFT"),
        ],
    )
    stream = iter(
        [
            _security_record(10.0, 1, "AAPL"),
            _security_record(20.0, 1, "MSFT"),
        ]
    )

    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    records = list(transform.apply(stream))

    assert [(record.security_id, record.time.hour, record.value) for record in records] == [
        ("AAPL", 0, None),
        ("AAPL", 1, 10.0),
        ("AAPL", 2, None),
        ("MSFT", 0, None),
        ("MSFT", 1, 20.0),
        ("MSFT", 2, None),
    ]


def test_ensure_cadence_sorts_and_deduplicates_tick_rows(tmp_path) -> None:
    context = _context(tmp_path)
    _write_tick_rows(
        context,
        "dataset_ticks",
        [
            {"time": _ts(2).isoformat()},
            {"time": _ts(0).isoformat()},
            {"time": _ts(1).isoformat()},
            {"time": _ts(1).isoformat()},
        ],
    )
    stream = iter([make_time_record(1.0, 0), make_time_record(2.0, 2)])

    records = list(
        EnsureCadenceTransform(
            cadence="dataset_ticks",
            field="value",
            context=context,
        ).apply(stream)
    )

    assert [(record.time.hour, record.value) for record in records] == [
        (0, 1.0),
        (1, None),
        (2, 2.0),
    ]


def test_ensure_cadence_accepts_tick_json_key_order_when_metadata_defines_grid_by(
    tmp_path,
) -> None:
    context = _context(tmp_path)
    _write_tick_rows(
        context,
        "model_grid",
        [
            {"venue": "XNYS", "time": _ts(0).isoformat(), "security_id": "AAPL"},
            {"time": _ts(1).isoformat(), "security_id": "AAPL", "venue": "XNYS"},
        ],
        meta={"grid_by": ["security_id", "venue"]},
    )
    stream = iter([_security_venue_record(2.0, 1, "AAPL", "XNYS")])

    records = list(
        EnsureCadenceTransform(
            cadence="model_grid",
            field="value",
            stream_partition_by=["security_id", "venue"],
            context=context,
        ).apply(stream)
    )

    assert [
        (record.security_id, record.venue, record.time.hour, record.value)
        for record in records
    ] == [
        ("AAPL", "XNYS", 0, None),
        ("AAPL", "XNYS", 1, 2.0),
    ]


def test_ensure_cadence_rejects_tick_artifact_extra_grid_fields(tmp_path) -> None:
    context = _context(tmp_path)
    _write_tick_rows(
        context,
        "model_grid",
        [{"time": _ts(0).isoformat(), "security_id": "AAPL", "venue": "XNYS"}],
        meta={"grid_by": ["security_id"]},
    )
    stream = iter([_security_record(1.0, 0, "AAPL")])

    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    with pytest.raises(ValueError, match="row grid fields"):
        list(transform.apply(stream))


def test_ensure_cadence_rejects_tick_artifact_row_without_time(tmp_path) -> None:
    context = _context(tmp_path)
    _write_tick_rows(
        context,
        "model_grid",
        [{"security_id": "AAPL"}],
        meta={"grid_by": ["security_id"]},
    )
    stream = iter([_security_record(1.0, 0, "AAPL")])

    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    with pytest.raises(ValueError, match="without time"):
        list(transform.apply(stream))


def test_ensure_cadence_rejects_bad_tick_grid_by_metadata(tmp_path) -> None:
    context = _context(tmp_path)
    _write_tick_rows(
        context,
        "model_grid",
        [{"time": _ts(0).isoformat(), "security_id": "AAPL"}],
        meta={"grid_by": "security_id"},
    )
    stream = iter([_security_record(1.0, 0, "AAPL")])

    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    with pytest.raises(RuntimeError, match="metadata field 'grid_by'"):
        list(transform.apply(stream))


def test_ensure_cadence_requires_tick_grid_by_metadata(tmp_path) -> None:
    context = _context(tmp_path)
    path = context.runtime.artifacts_root / "model_grid.jsonl"
    path.write_text(
        json.dumps({"time": _ts(0).isoformat(), "security_id": "AAPL"}) + "\n",
        encoding="utf-8",
    )
    context.runtime.artifacts.register("model_grid", path.name)
    stream = iter([_security_record(1.0, 0, "AAPL")])

    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    with pytest.raises(RuntimeError, match="metadata field 'grid_by' is required"):
        list(transform.apply(stream))


def test_ensure_cadence_rejects_global_artifact_for_partitioned_stream(tmp_path) -> None:
    context = _context(tmp_path)
    _write_ticks(context, "dataset_ticks", [0])
    stream = iter([_security_record(1.0, 0, "AAPL")])

    transform = EnsureCadenceTransform(
        cadence="dataset_ticks",
        field="value",
        stream_partition_by="security_id",
        context=context,
    )
    with pytest.raises(RuntimeError, match="must match ensure_cadence partition_by"):
        list(transform.apply(stream))


def test_ensure_cadence_keeps_records_outside_tick_grid(tmp_path) -> None:
    context = _context(tmp_path)
    _write_ticks(context, "dataset_ticks", [0, 1])
    stream = iter([make_time_record(3.0, 3)])

    records = list(
        EnsureCadenceTransform(
            cadence="dataset_ticks",
            field="value",
            context=context,
        ).apply(stream)
    )

    assert [(record.time.hour, record.value) for record in records] == [
        (0, None),
        (1, None),
        (3, 3.0),
    ]


def test_ensure_cadence_does_not_create_rows_without_seed_record(tmp_path) -> None:
    context = _context(tmp_path)
    _write_keyed_ticks(context, "model_grid", [(0, "AAPL"), (1, "AAPL")])

    records = list(
        EnsureCadenceTransform(
            cadence="model_grid",
            field="value",
            stream_partition_by="security_id",
            context=context,
        ).apply(iter([]))
    )

    assert records == []


def test_ensure_cadence_rejects_keyed_artifact_without_matching_partition(tmp_path) -> None:
    context = _context(tmp_path)
    _write_keyed_ticks(context, "model_grid", [(0, "AAPL")])
    stream = iter([_security_record(1.0, 0, "AAPL")])
    transform = EnsureCadenceTransform(
        cadence="model_grid",
        field="value",
        context=context,
    )

    with pytest.raises(RuntimeError, match="must match ensure_cadence partition_by"):
        list(transform.apply(stream))


def test_ensure_cadence_rejects_transform_partition_that_differs_from_stream() -> None:
    with pytest.raises(ValueError, match="must match the stream partition_by"):
        EnsureCadenceTransform(
            cadence="1h",
            field="value",
            partition_by="venue",
            stream_partition_by="security_id",
        )


def test_ensure_cadence_allows_matching_transform_and_stream_partition() -> None:
    transform = EnsureCadenceTransform(
        cadence="1h",
        field="value",
        partition_by="security_id",
        stream_partition_by="security_id",
    )

    assert transform.partition_fields() == ("security_id",)


def test_ensure_cadence_errors_when_tick_artifact_is_missing(tmp_path) -> None:
    context = _context(tmp_path)
    stream = iter([make_time_record(1.0, 0)])
    transform = EnsureCadenceTransform(
        cadence="missing_ticks",
        field="value",
        context=context,
    )

    with pytest.raises(RuntimeError, match="no tick artifact"):
        list(transform.apply(stream))
