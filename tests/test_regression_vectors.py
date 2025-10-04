from __future__ import annotations

from datetime import datetime, timezone, timedelta
import json

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.group_by import GroupBy, TimeKey
from datapipeline.domain.record import TimeSeriesRecord
from datapipeline.pipeline.pipelines import build_vector_pipeline


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


def test_regression_scaled_shapes_airpressure_high_freq_and_windspeed_hourly() -> None:
    # Fake raw streams
    # high-frequency mock series (multiple samples per hour)
    high_freq_raw = [
        TimeSeriesRecord(time=_ts(0, 10), value=1010.0),
        TimeSeriesRecord(time=_ts(0, 20), value=1020.0),
        TimeSeriesRecord(time=_ts(0, 55), value=1000.0),
        TimeSeriesRecord(time=_ts(1, 5), value=1005.0),
        TimeSeriesRecord(time=_ts(1, 15), value=1007.0),
    ]
    # hourly mock series (single sample per hour)
    hourly_raw = [
        TimeSeriesRecord(time=_ts(0, 0), value=5.0),
        TimeSeriesRecord(time=_ts(1, 0), value=7.0),
    ]

    streams = {
        "air_pressure": high_freq_raw,
        "wind_speed": hourly_raw,
    }

    def open_stream(alias: str):
        return iter(streams[alias])

    group_by = GroupBy(keys=[TimeKey(field="time", resolution="1h")])

    configs = [
        FeatureRecordConfig(stream="air_pressure", id="air_pressure", scale=True),
        FeatureRecordConfig(stream="wind_speed", id="wind_speed", scale=True),
    ]

    out = list(build_vector_pipeline(configs, group_by, open_stream, vector_transforms=None))

    # Two hourly groups expected: 00:00 and 01:00
    assert len(out) == 2

    # Shapes: air_pressure -> lists (multiple per hour); wind_speed -> scalars (single per hour)
    v0 = out[0][1].values
    v1 = out[1][1].values

    assert isinstance(v0["air_pressure"], list) and len(v0["air_pressure"]) == 3
    assert isinstance(v1["air_pressure"], list) and len(v1["air_pressure"]) == 2
    assert not isinstance(v0["wind_speed"], list)
    assert not isinstance(v1["wind_speed"], list)

    # Scaled values should have ~zero mean per feature across the whole stream
    ap_all = v0["air_pressure"] + v1["air_pressure"]
    ap_mean = sum(ap_all) / len(ap_all)
    assert abs(ap_mean) < 1e-6

    ws_all = [v0["wind_speed"], v1["wind_speed"]]
    ws_mean = sum(ws_all) / len(ws_all)
    assert abs(ws_mean) < 1e-6


def test_regression_fill_then_scale_with_missing_values() -> None:
    # air_pressure has a missing value in between two valid ones within the same hour
    series_high = [
        TimeSeriesRecord(time=_ts(0, 10), value=1000.0),
        TimeSeriesRecord(time=_ts(0, 20), value=None),  # missing
        TimeSeriesRecord(time=_ts(0, 40), value=1100.0),
    ]
    # wind_speed hourly with a missing at hour 1
    series_hourly = [
        TimeSeriesRecord(time=_ts(0, 0), value=5.0),
        TimeSeriesRecord(time=_ts(1, 0), value=None),  # missing
    ]

    streams = {"ap": series_high, "ws": series_hourly}

    def open_stream(alias: str):
        return iter(streams[alias])

    group_by = GroupBy(keys=[TimeKey(field="time", resolution="1h")])

    # Fill then scale for both features
    configs = [
        FeatureRecordConfig(stream="ap", id="air_pressure", fill={"statistic": "median", "window": 10, "min_samples": 1}, scale=True),
        FeatureRecordConfig(stream="ws", id="wind_speed", fill={"statistic": "mean", "window": 10, "min_samples": 1}, scale=True),
    ]

    out = list(build_vector_pipeline(configs, group_by, open_stream, vector_transforms=None))

    # One hour group
    assert len(out) == 2  # hours 00 and 01 due to wind_speed hour 1

    v0 = out[0][1].values
    # air_pressure list length = 3 with middle filled (not None)
    assert isinstance(v0["air_pressure"], list) and len(v0["air_pressure"]) == 3
    assert all(isinstance(x, float) for x in v0["air_pressure"])  # filled and scaled

    # wind_speed hour 0 present and scaled; hour 1 present due to fill then scale
    v1 = out[1][1].values
    assert not isinstance(v0["wind_speed"], list)
    assert not isinstance(v1["wind_speed"], list)


def test_regression_vector_transforms_fill_horizontal_history_and_drop(tmp_path) -> None:
    # Two partitioned features using explicit ids to avoid partition_by on records
    wind_a = [
        TimeSeriesRecord(time=_ts(0, 0), value=10.0),
        TimeSeriesRecord(time=_ts(1, 0), value=12.0),
    ]
    # B missing at both hours
    wind_b = [
        TimeSeriesRecord(time=_ts(0, 0), value=None),
        TimeSeriesRecord(time=_ts(1, 0), value=None),
    ]

    streams = {
        "wind_A": wind_a,
        "wind_B": wind_b,
    }

    def open_stream(alias: str):
        return iter(streams[alias])

    group_by = GroupBy(keys=[TimeKey(field="time", resolution="1h")])

    configs = [
        FeatureRecordConfig(stream="wind_A", id="wind_speed__A"),
        FeatureRecordConfig(stream="wind_B", id="wind_speed__B"),
    ]

    # Minimal manifest for partitions
    manifest_path = tmp_path / "parts.json"
    manifest_data = {
        "features": ["wind_speed"],
        "partitions": ["wind_speed__A", "wind_speed__B"],
        "by_feature": {"wind_speed": ["A", "B"]},
    }
    manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

    vt = [
        {"fill_horizontal": {"manifest": str(manifest_path), "statistic": "mean", "min_samples": 1}},
        {"fill_history": {"manifest": str(manifest_path), "match_partition": "full", "statistic": "mean", "window": 2, "min_samples": 1}},
        {"drop_missing": {"manifest": str(manifest_path), "match_partition": "full", "min_coverage": 1.0}},
    ]

    out = list(build_vector_pipeline(configs, group_by, open_stream, vector_transforms=vt))

    # Two hourly vectors should be present after horizontal fill + drop
    assert len(out) == 2
    v0 = out[0][1].values
    v1 = out[1][1].values
    assert v0["wind_speed__A"] == 10.0 and v0["wind_speed__B"] == 10.0
    assert v1["wind_speed__A"] == 12.0 and v1["wind_speed__B"] == 12.0
