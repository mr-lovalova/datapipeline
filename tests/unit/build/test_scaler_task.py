from datetime import datetime, timezone
import json

import pytest

from datapipeline.operations.artifacts.scaler import materialize_scaler_statistics
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.split import TimeSplitConfig
from datapipeline.config.tasks import ScalerTask
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.runtime import Runtime


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def test_materialize_scaler_statistics_split_all_ignores_label_filter(monkeypatch, tmp_path):
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    runtime.split = TimeSplitConfig(
        boundaries=["2024-01-02T00:00:00Z"],
        labels=["train", "test"],
    )

    dataset = FeatureDatasetConfig(
        group_by="1h",
        features=[
            FeatureRecordConfig(
                id="x",
                record_stream="stream",
                field="value",
                scale=True,
            )
        ],
        targets=[],
    )

    samples = [
        Sample(key=(_ts(1),), features=Vector(values={"x": 1.0})),
        Sample(key=(_ts(3),), features=Vector(values={"x": 3.0})),
    ]

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args, **_kwargs: dataset,
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.build_vector_pipeline",
        lambda *_args, **_kwargs: iter(samples),
    )

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(id="scaler", split_label="all", output="scaler.json"),
    )

    assert result is not None
    assert result.relative_path == "scaler.json"
    assert (artifacts_root / result.relative_path).exists()
    assert result.meta["split"] == "all"
    assert result.meta["observations"] == 2
    payload = json.loads(
        (artifacts_root / result.relative_path).read_text(encoding="utf-8")
    )
    assert payload["kind"] == "standard_scaler"
    assert payload["split"] == "all"
    assert payload["observations"] == 2
    assert "x" in payload["statistics"]


def test_materialize_scaler_statistics_skips_when_no_scaled_features(
    monkeypatch,
    tmp_path,
) -> None:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    dataset = FeatureDatasetConfig(
        group_by="1h",
        features=[FeatureRecordConfig(id="x", record_stream="stream", field="value")],
        targets=[],
    )

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.load_dataset",
        lambda *_args, **_kwargs: dataset,
    )
    monkeypatch.setattr(
        "datapipeline.operations.artifacts.scaler.build_vector_pipeline",
        lambda *_args, **_kwargs: pytest.fail("vector pipeline should not run"),
    )

    result = materialize_scaler_statistics(
        runtime,
        ScalerTask(id="scaler", split_label="train", output="scaler.json"),
    )

    assert result is None
