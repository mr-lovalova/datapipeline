from __future__ import annotations

import math

from datapipeline.build.tasks.utils import collect_schema_entries
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
