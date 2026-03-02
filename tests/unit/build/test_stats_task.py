from datetime import datetime, timezone
import json

from datapipeline.operations.artifacts.stats import materialize_vector_stats
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import StatsTask
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_SCHEMA_METADATA


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def test_materialize_vector_stats_reads_metadata_and_omits_schema_meta(monkeypatch, tmp_path):
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)

    dataset = FeatureDatasetConfig(
        group_by="1h",
        features=[FeatureRecordConfig(id="speed", record_stream="stream", field="value")],
        targets=[],
    )
    samples = [
        Sample(key=(_ts(1),), features=Vector(values={"speed": [1.0, None]})),
    ]

    class _Ctx:
        def __init__(self, _runtime):
            self.runtime = _runtime

        def require_artifact(self, spec):
            assert spec.key == VECTOR_SCHEMA_METADATA
            return {
                "features": [{"id": "speed", "cadence": {"strategy": "max", "target": 2}}],
                "targets": [],
            }

        def window_bounds(self, rectangular_required: bool):
            assert rectangular_required is True

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.stats.PipelineContext",
        _Ctx,
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.stats.load_dataset",
        lambda *_args, **_kwargs: dataset,
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.stats.build_vector_pipeline",
        lambda *_args, **_kwargs: iter(samples),
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.stats.post_process",
        lambda _context, vectors: vectors,
    )

    result = materialize_vector_stats(
        runtime,
        StatsTask(id="stats", mode="final", output="build/stats.json"),
    )

    assert result is not None
    rel_path, _meta = result
    payload = json.loads((artifacts_root / rel_path).read_text(encoding="utf-8"))
    assert "schema_meta" not in payload
    assert "expected_features" not in payload
    assert "discovered_features" not in payload
    assert "discovered_partitions" not in payload
    assert "seen_counts" not in payload
    assert "null_counts_features" not in payload
    assert "seen_counts_partitions" not in payload
    assert "null_counts_partitions" not in payload
    assert "cadence_null_counts" not in payload
    assert "cadence_opportunities" not in payload
    assert "cadence_null_counts_partitions" not in payload
    assert "cadence_opportunities_partitions" not in payload
    assert "missing_samples" not in payload
    assert "missing_partition_samples" not in payload
    assert payload["group_feature_status"]["2024-01-01 00:00:00+00:00"]["speed"] == 1
    assert payload["group_partition_status"]["2024-01-01 00:00:00+00:00"]["speed"] == 1
