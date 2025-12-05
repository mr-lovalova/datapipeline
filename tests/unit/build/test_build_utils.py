from __future__ import annotations

import math
from datetime import datetime, timezone

from datapipeline.build.tasks.metadata import _window_bounds_from_stats, _window_size
from datapipeline.build.tasks.utils import (
    collect_schema_entries,
    metadata_entries_from_stats,
    schema_entries_from_stats,
)
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.runtime import Runtime


def test_collect_schema_entries_counts_nan(monkeypatch, tmp_path):
    """Ensure metadata collection treats NaN values as nulls."""
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    cfg = FeatureRecordConfig(id="wind_speed", record_stream="met.obs")
    sample = Sample(key=(0,), features=Vector(values={"wind_speed": math.nan}))

    def fake_pipeline(context, configs, group_by_cadence, target_configs=None, *, rectangular=True):
        assert not rectangular
        return iter([sample])

    monkeypatch.setattr(
        "datapipeline.build.tasks.utils.build_vector_pipeline",
        fake_pipeline,
    )

    stats, vector_count, _, _ = collect_schema_entries(
        runtime,
        [cfg],
        group_by="1h",
        cadence_strategy="max",
        collect_metadata=True,
    )

    assert vector_count == 1
    entry = next(item for item in stats if item["id"] == "wind_speed")
    assert entry["present_count"] == 1
    assert entry["null_count"] == 1


def test_metadata_entries_include_observation_bounds():
    stats = [
        {
            "id": "temp",
            "base_id": "temp",
            "kind": "scalar",
            "present_count": 4,
            "null_count": 0,
            "first_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "last_ts": datetime(2024, 1, 2, tzinfo=timezone.utc),
        }
    ]

    entries = metadata_entries_from_stats(stats, "max")

    assert entries[0]["first_observed"] == "2024-01-01T00:00:00Z"
    assert entries[0]["last_observed"] == "2024-01-02T00:00:00Z"


def test_window_bounds_modes():
    ts = lambda hour: datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc)
    feature_stats = [
        {"id": "wind__@A", "base_id": "wind", "first_ts": ts(0), "last_ts": ts(6)},
        {"id": "wind__@B", "base_id": "wind", "first_ts": ts(2), "last_ts": ts(5)},
        {"id": "temp", "base_id": "temp", "first_ts": ts(1), "last_ts": ts(7)},
    ]
    target_stats: list[dict] = []

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="union")
    assert start == ts(0)
    assert end == ts(7)

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="intersection")
    assert start == ts(1)
    assert end == ts(6)

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="strict")
    assert start == ts(2)
    assert end == ts(5)

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="relaxed")
    assert start == ts(0)
    assert end == ts(7)


def test_window_size_counts_cadence_buckets():
    ts = lambda hour: datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc)
    start = ts(4)
    end = ts(10)

    assert _window_size(start, end, "1h") == 7  # hours 4..10 inclusive
    assert _window_size(start, end, "2h") == 4  # 4,6,8,10
    assert _window_size(start, end, "15m") == 25  # 6 hours -> 360 minutes / 15 + 1
    assert _window_size(start, end, None) is None
