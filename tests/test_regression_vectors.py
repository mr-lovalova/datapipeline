from datetime import datetime, timezone
from pathlib import Path

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.constants import PARTIONED_IDS, SCALER_STATISTICS
from datapipeline.transforms.feature.scaler import StandardScaler
from datapipeline.transforms.vector import (
    VectorDropMissingTransform,
    VectorFillAcrossPartitionsTransform,
    VectorFillHistoryTransform,
)


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


def _identity(iterable):
    return iterable


class _StubSource:
    def __init__(self, rows):
        self._rows = rows

    def stream(self):
        return iter(self._rows)


def _runtime_with_streams(
    tmp_path: Path,
    streams: dict[str, list[TemporalRecord]],
    stream_transforms: dict[str, list[dict[str, object]]] | None = None,
) -> Runtime:
    project_yaml = tmp_path / "project.yaml"
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)

    regs = runtime.registries
    stream_transforms = stream_transforms or {}
    for alias, rows in streams.items():
        regs.stream_sources.register(alias, _StubSource(rows))
        regs.mappers.register(alias, _identity)
        regs.record_operations.register(alias, [])
        regs.stream_operations.register(alias, stream_transforms.get(alias, []))
        regs.debug_operations.register(alias, [])
        regs.partition_by.register(alias, None)
        regs.sort_batch_size.register(alias, 1024)

    return runtime


def _register_scaler(runtime: Runtime, configs: list[FeatureRecordConfig], group_by: str) -> None:
    sanitized = [cfg.model_copy(update={"scale": False}) for cfg in configs]
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(context, sanitized, group_by, stage=None)

    scaler = StandardScaler()
    total = scaler.fit(((group_key, vector) for group_key, vector in vectors))
    if not total:
        raise RuntimeError(
            "Unable to compute scaler statistics for test runtime.")

    destination = runtime.artifacts_root / "scaler.pkl"
    scaler.save(destination)
    runtime.artifacts.register(
        SCALER_STATISTICS,
        relative_path=destination.relative_to(
            runtime.artifacts_root).as_posix(),
    )


def _register_partitioned_ids(runtime: Runtime, ids: list[str]) -> None:
    path = runtime.artifacts_root / "partitioned_ids.txt"
    path.write_text("\n".join(ids) + "\n", encoding="utf-8")
    runtime.artifacts.register(
        PARTIONED_IDS,
        relative_path=path.relative_to(runtime.artifacts_root).as_posix(),
    )


def test_regression_scaled_shapes_airpressure_high_freq_and_windspeed_hourly(tmp_path) -> None:
    # Fake raw streams
    # high-frequency mock series (multiple samples per hour)
    high_freq_raw = [
        TemporalRecord(time=_ts(0, 10), value=1010.0),
        TemporalRecord(time=_ts(0, 20), value=1020.0),
        TemporalRecord(time=_ts(0, 55), value=1000.0),
        TemporalRecord(time=_ts(1, 5), value=1005.0),
        TemporalRecord(time=_ts(1, 15), value=1007.0),
    ]
    # hourly mock series (single sample per hour)
    hourly_raw = [
        TemporalRecord(time=_ts(0, 0), value=5.0),
        TemporalRecord(time=_ts(1, 0), value=7.0),
    ]

    streams = {
        "air_pressure": high_freq_raw,
        "wind_speed": hourly_raw,
    }
    runtime = _runtime_with_streams(tmp_path, streams)
    group_by = "1h"

    configs = [
        FeatureRecordConfig(record_stream="air_pressure",
                            id="air_pressure", scale=True),
        FeatureRecordConfig(record_stream="wind_speed", id="wind_speed", scale=True),
    ]

    _register_scaler(runtime, configs, group_by)
    context = PipelineContext(runtime)

    out = list(build_vector_pipeline(context, configs, group_by, stage=None))

    # Two hourly groups expected: 00:00 and 01:00
    assert len(out) == 2

    # Shapes: air_pressure -> lists (multiple per hour); wind_speed -> scalars (single per hour)
    v0 = out[0][1].values
    v1 = out[1][1].values

    assert isinstance(v0["air_pressure"], list) and len(
        v0["air_pressure"]) == 3
    assert isinstance(v1["air_pressure"], list) and len(
        v1["air_pressure"]) == 2
    assert not isinstance(v0["wind_speed"], list)
    assert not isinstance(v1["wind_speed"], list)

    # Scaled values should have ~zero mean per feature across the whole stream
    ap_all = v0["air_pressure"] + v1["air_pressure"]
    ap_mean = sum(ap_all) / len(ap_all)
    assert abs(ap_mean) < 1e-6

    ws_all = [v0["wind_speed"], v1["wind_speed"]]
    ws_mean = sum(ws_all) / len(ws_all)
    assert abs(ws_mean) < 1e-6


def test_regression_fill_then_scale_with_missing_values(tmp_path) -> None:
    # air_pressure has a missing value in between two valid ones within the same hour
    series_high = [
        TemporalRecord(time=_ts(0, 10), value=1000.0),
        TemporalRecord(time=_ts(0, 20), value=None),  # missing
        TemporalRecord(time=_ts(0, 40), value=1100.0),
    ]
    # wind_speed hourly with a missing at hour 1
    series_hourly = [
        TemporalRecord(time=_ts(0, 0), value=5.0),
        TemporalRecord(time=_ts(1, 0), value=None),  # missing
    ]

    streams = {"ap": series_high, "ws": series_hourly}
    runtime = _runtime_with_streams(tmp_path, streams)
    group_by = "1h"

    # Fill then scale for both features
    stream_transforms = {
        "ap": [
            {"fill": {"statistic": "median", "window": 10, "min_samples": 1}},
        ],
        "ws": [
            {"fill": {"statistic": "mean", "window": 10, "min_samples": 1}},
        ],
    }

    runtime = _runtime_with_streams(tmp_path, streams,
                                    stream_transforms=stream_transforms)

    configs = [
        FeatureRecordConfig(record_stream="ap",
                            id="air_pressure", scale=True),
        FeatureRecordConfig(record_stream="ws", id="wind_speed", scale=True),
    ]

    _register_scaler(runtime, configs, group_by)
    context = PipelineContext(runtime)
    out = list(build_vector_pipeline(context, configs, group_by, stage=None))

    # One hour group
    assert len(out) == 2  # hours 00 and 01 due to wind_speed hour 1

    v0 = out[0][1].values
    # air_pressure list length = 3 with middle filled (not None)
    assert isinstance(v0["air_pressure"], list) and len(
        v0["air_pressure"]) == 3
    assert all(isinstance(x, float)
               for x in v0["air_pressure"])  # filled and scaled

    # wind_speed hour 0 present and scaled; hour 1 present due to fill then scale
    v1 = out[1][1].values
    assert not isinstance(v0["wind_speed"], list)
    assert not isinstance(v1["wind_speed"], list)


def test_regression_vector_transforms_fill_horizontal_history_and_drop(tmp_path) -> None:
    # Two partitioned features using explicit ids to avoid partition_by on records
    wind_a = [
        TemporalRecord(time=_ts(0, 0), value=10.0),
        TemporalRecord(time=_ts(1, 0), value=12.0),
    ]
    # B missing at both hours
    wind_b = [
        TemporalRecord(time=_ts(0, 0), value=None),
        TemporalRecord(time=_ts(1, 0), value=None),
    ]

    streams = {
        "wind_A": wind_a,
        "wind_B": wind_b,
    }
    runtime = _runtime_with_streams(tmp_path, streams)
    context = PipelineContext(runtime)
    group_by = "1h"

    configs = [
        FeatureRecordConfig(record_stream="wind_A", id="wind_speed__A"),
        FeatureRecordConfig(record_stream="wind_B", id="wind_speed__B"),
    ]

    _register_partitioned_ids(
        runtime, ["wind_speed__A", "wind_speed__B"])

    vectors = build_vector_pipeline(context, configs, group_by, stage=None)

    transforms = [
        VectorFillAcrossPartitionsTransform(statistic="mean", min_samples=1),
        VectorFillHistoryTransform(statistic="mean", window=2, min_samples=1),
        VectorDropMissingTransform(min_coverage=1.0),
    ]

    stream = vectors
    for transform in transforms:
        transform.bind_context(context)
        stream = transform.apply(stream)

    out = list(stream)

    # Two hourly vectors should be present after horizontal fill + drop
    assert len(out) == 2
    v0 = out[0][1].values
    v1 = out[1][1].values
    assert v0["wind_speed__A"] == 10.0 and v0["wind_speed__B"] == 10.0
    assert v1["wind_speed__A"] == 12.0 and v1["wind_speed__B"] == 12.0
