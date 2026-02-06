from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.feature import FeatureRecordSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import (
    build_feature_pipeline,
    build_record_pipeline,
    build_vector_pipeline,
)
from datapipeline.pipeline.stages import post_process
from datapipeline.runtime import Runtime
from datapipeline.services.constants import POSTPROCESS_TRANSFORMS, VECTOR_SCHEMA


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


class _StubSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def stream(self):
        return iter(self._rows)


def _mapper(rows):
    for row in rows:
        rec = TemporalRecord(time=row["time"])
        for key, value in row.items():
            if key == "time":
                continue
            setattr(rec, key, value)
        yield rec


def _runtime_with_rows(
    tmp_path: Path,
    rows: list[dict],
    *,
    stream_id: str = "stream",
    record_ops: list[dict] | None = None,
    stream_ops: list[dict] | None = None,
    partition_by: str | None = None,
) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)

    regs = runtime.registries
    regs.stream_sources.register(stream_id, _StubSource(rows))
    regs.mappers.register(stream_id, _mapper)
    regs.record_operations.register(stream_id, record_ops or [])
    regs.stream_operations.register(stream_id, stream_ops or [])
    regs.debug_operations.register(stream_id, [])
    regs.partition_by.register(stream_id, partition_by)
    regs.sort_batch_size.register(stream_id, 128)
    return runtime


def test_stage_0_to_2_record_pipeline(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0, 30), "value": 1.0},
        {"time": _ts(0, 10), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        record_ops=[{"floor_time": {"cadence": "1h"}}],
    )
    ctx = PipelineContext(runtime)

    stage0 = list(build_record_pipeline(ctx, "stream", stage=0))
    assert stage0 == rows

    stage1 = list(build_record_pipeline(ctx, "stream", stage=1))
    assert all(isinstance(rec, TemporalRecord) for rec in stage1)
    assert [rec.time for rec in stage1] == [rows[0]["time"], rows[1]["time"]]

    stage2 = list(build_record_pipeline(ctx, "stream", stage=2))
    assert all(rec.time.minute == 0 for rec in stage2)


def test_stage_3_orders_by_partition_and_time(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(1), "value": 10.0, "symbol": "B"},
        {"time": _ts(0), "value": 5.0, "symbol": "A"},
        {"time": _ts(2), "value": 6.0, "symbol": "A"},
    ]
    runtime = _runtime_with_rows(tmp_path, rows, partition_by="symbol")
    ctx = PipelineContext(runtime)

    ordered = list(build_record_pipeline(ctx, "stream", stage=3))
    assert [(rec.symbol, rec.time.hour) for rec in ordered] == [
        ("A", 0),
        ("A", 2),
        ("B", 1),
    ]


def test_stage_4_applies_stream_transforms(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0},
        {"time": _ts(2), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        stream_ops=[{"ensure_cadence": {"cadence": "1h", "field": "value"}}],
    )
    ctx = PipelineContext(runtime)

    transformed = list(build_record_pipeline(ctx, "stream", stage=4))
    assert [(rec.time.hour, rec.value) for rec in transformed] == [
        (0, 1.0),
        (1, None),
        (2, 2.0),
    ]


def test_stage_5_wraps_feature_values(tmp_path: Path) -> None:
    rows = [{"time": _ts(0), "value": 3.0, "symbol": "X"}]
    runtime = _runtime_with_rows(tmp_path, rows, partition_by="symbol")
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        record_stream="stream",
        id="price",
        field="value",
    )

    features = list(build_feature_pipeline(ctx, cfg, stage=5))
    assert len(features) == 1
    feature = features[0]
    assert feature.value == 3.0
    assert feature.id == "price__@symbol:X"


def test_stage_6_applies_feature_transforms(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0},
        {"time": _ts(1), "value": 2.0},
        {"time": _ts(2), "value": 3.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        record_stream="stream",
        id="price",
        field="value",
        sequence={"size": 2, "stride": 1},
    )

    sequences = list(build_feature_pipeline(ctx, cfg, stage=6))
    assert len(sequences) == 2
    assert isinstance(sequences[0], FeatureRecordSequence)
    assert sequences[0].values == [1.0, 2.0]
    assert sequences[1].values == [2.0, 3.0]


def test_stage_7_vs_8_postprocess(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": None},
        {"time": _ts(1), "value": 2.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    schema_path = runtime.artifacts_root / "schema.json"
    schema_doc = {"features": [{"id": "price"}], "targets": []}
    schema_path.write_text(json.dumps(schema_doc, indent=2), encoding="utf-8")
    runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema_path.relative_to(runtime.artifacts_root).as_posix(),
    )
    runtime.registries.postprocesses.register(
        POSTPROCESS_TRANSFORMS,
        [{"replace": {"value": 0}}],
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    raw = list(build_vector_pipeline(ctx, [cfg], "1h", rectangular=False))
    assert raw[0].features.values["price"] is None

    processed = list(post_process(ctx, iter(raw)))
    assert processed[0].features.values["price"] == 0
