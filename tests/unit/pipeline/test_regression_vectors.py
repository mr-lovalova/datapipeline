from datetime import datetime, timezone
import math
import json
from pathlib import Path

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.constants import SCALER_STATISTICS, VECTOR_SCHEMA
from datapipeline.transforms.feature.scaler import StandardScaler
from datapipeline.transforms.vector import (
    VectorDropTransform,
    VectorFillTransform,
)


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


def _record(ts: datetime, value: float | None) -> TemporalRecord:
    rec = TemporalRecord(time=ts)
    setattr(rec, "value", value)
    return rec


def _air_density(pressure_hpa: float, temp_c: float, rh_percent: float | None) -> float:
    pressure_pa = pressure_hpa * 100.0
    temp_k = temp_c + 273.15
    density = pressure_pa / (287.05 * temp_k)
    if rh_percent is not None:
        rh = rh_percent / 100.0
        saturation = 6.112 * math.exp((17.67 * temp_c) / (temp_c + 243.5))
        vapor_pressure = rh * saturation * 100.0
        density = (pressure_pa - 0.378 * vapor_pressure) / (287.05 * temp_k)
    return density


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
    start_time: datetime | None = None
    end_time: datetime | None = None
    for alias, rows in streams.items():
        regs.stream_sources.register(alias, _StubSource(rows))
        regs.mappers.register(alias, _identity)
        regs.record_operations.register(alias, [])
        regs.stream_operations.register(
            alias, stream_transforms.get(alias, []))
        regs.debug_operations.register(alias, [])
        regs.partition_by.register(alias, None)
        regs.sort_batch_size.register(alias, 1024)
        for rec in rows:
            ts = getattr(rec, "time", None)
            if isinstance(ts, datetime):
                start_time = ts if start_time is None else min(start_time, ts)
                end_time = ts if end_time is None else max(end_time, ts)

    runtime.window_bounds = (start_time, end_time)
    return runtime


def _register_scaler(runtime: Runtime, configs: list[FeatureRecordConfig], group_by: str) -> None:
    sanitized = [cfg.model_copy(update={"scale": False}) for cfg in configs]
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(context, sanitized, group_by)

    scaler = StandardScaler()
    total = scaler.fit(vectors)
    if not total:
        raise RuntimeError(
            "Unable to compute scaler statistics for test runtime.")

    destination = runtime.artifacts_root / "scaler.json"
    scaler.save(destination)
    runtime.artifacts.register(
        SCALER_STATISTICS,
        relative_path=destination.relative_to(
            runtime.artifacts_root).as_posix(),
    )


def _register_partitioned_ids(runtime: Runtime, ids: list[str]) -> None:
    schema_path = runtime.artifacts_root / "schema.json"
    schema_doc = {"features": [{"id": fid} for fid in ids], "targets": []}
    schema_path.write_text(json.dumps(schema_doc, indent=2), encoding="utf-8")
    runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema_path.relative_to(runtime.artifacts_root).as_posix(),
    )


def test_vector_targets_respect_partitioned_ids(tmp_path) -> None:
    def _partitioned_record(value: float, code: str) -> TemporalRecord:
        rec = _record(_ts(0), value)
        setattr(rec, "municipality", code)
        return rec

    streams = {
        "wind_speed_stream": [
            _partitioned_record(2.5, "06019"),
            _partitioned_record(3.1, "06030"),
        ],
        "wind_production_stream": [
            _partitioned_record(0.3, "06019"),
            _partitioned_record(0.5, "06030"),
        ],
    }
    runtime = _runtime_with_streams(tmp_path, streams)
    runtime.registries.partition_by.register(
        "wind_speed_stream", "municipality")
    runtime.registries.partition_by.register(
        "wind_production_stream", "municipality")

    context = PipelineContext(runtime)
    feature_cfgs = [
        FeatureRecordConfig(
            record_stream="wind_speed_stream",
            id="wind_speed",
            field="value",
        ),
    ]
    target_cfgs = [
        FeatureRecordConfig(
            record_stream="wind_production_stream",
            id="wind_production",
            field="value",
        ),
    ]

    samples = list(
        build_vector_pipeline(
            context, feature_cfgs, "1h", target_configs=target_cfgs
        )
    )

    assert len(samples) == 1
    sample = samples[0]
    assert sample.targets is not None
    assert set(sample.targets.keys()) == {
        "wind_production__@municipality:06019",
        "wind_production__@municipality:06030",
    }
    assert all(
        not key.startswith("wind_production")
        for key in sample.features.keys()
    )


def test_regression_scaled_shapes_airpressure_high_freq_and_windspeed_hourly(tmp_path) -> None:
    # Fake raw streams
    # high-frequency mock series (multiple samples per hour)
    high_freq_raw = [
        _record(_ts(0, 10), 1010.0),
        _record(_ts(0, 20), 1020.0),
        _record(_ts(0, 55), 1000.0),
        _record(_ts(1, 5), 1005.0),
        _record(_ts(1, 15), 1007.0),
    ]
    # hourly mock series (single sample per hour)
    hourly_raw = [
        _record(_ts(0, 0), 5.0),
        _record(_ts(1, 0), 7.0),
    ]

    streams = {
        "air_pressure": high_freq_raw,
        "wind_speed": hourly_raw,
    }
    runtime = _runtime_with_streams(tmp_path, streams)
    group_by = "1h"

    configs = [
        FeatureRecordConfig(
            record_stream="air_pressure",
            id="air_pressure",
            field="value",
            scale=True,
        ),
        FeatureRecordConfig(
            record_stream="wind_speed",
            id="wind_speed",
            field="value",
            scale=True,
        ),
    ]

    _register_scaler(runtime, configs, group_by)
    context = PipelineContext(runtime)

    out = list(build_vector_pipeline(context, configs, group_by))

    # Two hourly groups expected: 00:00 and 01:00
    assert len(out) == 2

    # Shapes: air_pressure -> lists (multiple per hour); wind_speed -> scalars (single per hour)
    v0 = out[0].features.values
    v1 = out[1].features.values

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
        _record(_ts(0, 10), 1000.0),
        _record(_ts(0, 20), None),  # missing
        _record(_ts(0, 40), 1100.0),
    ]
    # wind_speed hourly with a missing at hour 1
    series_hourly = [
        _record(_ts(0, 0), 5.0),
        _record(_ts(1, 0), None),  # missing
    ]

    streams = {"ap": series_high, "ws": series_hourly}
    runtime = _runtime_with_streams(tmp_path, streams)
    group_by = "1h"

    # Fill then scale for both features
    stream_transforms = {
        "ap": [
            {
                "fill": {
                    "field": "value",
                    "statistic": "median",
                    "window": 10,
                    "min_samples": 1,
                }
            },
        ],
        "ws": [
            {
                "fill": {
                    "field": "value",
                    "statistic": "mean",
                    "window": 10,
                    "min_samples": 1,
                }
            },
        ],
    }

    runtime = _runtime_with_streams(tmp_path, streams,
                                    stream_transforms=stream_transforms)

    configs = [
        FeatureRecordConfig(
            record_stream="ap",
            id="air_pressure",
            field="value",
            scale=True,
        ),
        FeatureRecordConfig(
            record_stream="ws",
            id="wind_speed",
            field="value",
            scale=True,
        ),
    ]

    _register_scaler(runtime, configs, group_by)
    context = PipelineContext(runtime)
    out = list(build_vector_pipeline(context, configs, group_by))

    # Two hour groups because fill made the second hour visible
    assert len(out) == 2  # hours 00 and 01 due to wind_speed hour 1

    v0 = out[0].features.values
    # air_pressure list length = 3 with middle filled (not None)
    assert isinstance(v0["air_pressure"], list) and len(
        v0["air_pressure"]) == 3
    assert all(isinstance(x, float)
               for x in v0["air_pressure"])  # filled and scaled

    # wind_speed hour 0 present and scaled; hour 1 present due to fill then scale
    v1 = out[1].features.values
    assert not isinstance(v0["wind_speed"], list)
    assert not isinstance(v1["wind_speed"], list)
