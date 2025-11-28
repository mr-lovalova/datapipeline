from __future__ import annotations

from pathlib import Path

import pytest

from datapipeline.config.context import load_dataset_context
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process


@pytest.mark.parametrize(
    "project_path",
    [Path("tests/fixtures/regression_project/project.yaml")],
)
def test_full_regression_project_vectors(project_path: Path) -> None:
    ctx = load_dataset_context(project_path)
    context = ctx.pipeline_context

    base_vectors = build_vector_pipeline(
        context,
        ctx.features,
        ctx.dataset.group_by,
        target_configs=ctx.targets,
    )
    samples = list(post_process(context, base_vectors))

    assert len(samples) == 6

    expected = [
        (0, {
            "linear_scaled": -1.6666666666666667,
            "sine_window": [0.5, 1.0],
            "humidity_partitioned__@location:north": 40.0,
            "humidity_partitioned__@location:south": 38.5,
        }, 100.0),
        (1, {
            "linear_scaled": -1.0,
            "sine_window": [0.5, 0.0],
            "humidity_partitioned__@location:north": 40.0,
            "humidity_partitioned__@location:south": 37.0,
        }, 102.0),
        (2, {
            "linear_scaled": -0.3333333333333333,
            "sine_window": [None, -0.5],
            "humidity_partitioned__@location:north": 41.0,
            "humidity_partitioned__@location:south": 37.75,
        }, 101.0),
        (3, {
            "linear_scaled": 0.3333333333333333,
            "sine_window": [-1.0, -0.5],
            "humidity_partitioned__@location:north": 42.0,
            "humidity_partitioned__@location:south": 37.75,
        }, 105.0),
        (4, {
            "linear_scaled": 1.0,
            "sine_window": [0.0, 0.5],
            "humidity_partitioned__@location:north": 41.5,
            "humidity_partitioned__@location:south": 39.0,
        }, 103.0),
        (5, {
            "linear_scaled": 1.6666666666666667,
            "sine_window": [None, None],
            "humidity_partitioned__@location:north": 43.0,
            "humidity_partitioned__@location:south": 40.0,
        }, 107.0),
    ]

    for sample, (hour, expected_features, target_value) in zip(samples, expected):
        ts = sample.key[0]
        assert ts.hour == hour
        assert ts.tzinfo is not None

        values = sample.features.values
        for feature_id, expected_value in expected_features.items():
            assert feature_id in values
            observed = values[feature_id]
            if isinstance(expected_value, list):
                assert observed == expected_value
            elif expected_value is None:
                assert observed is None
            else:
                assert observed == pytest.approx(expected_value)

        assert sample.targets is not None
        assert sample.targets.values["power_target"] == pytest.approx(target_value)
