import pytest

from datapipeline.config.context import load_dataset_context
from datapipeline.config.tasks import SchemaTask
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.services.constants import SCALER_STATISTICS, VECTOR_SCHEMA
from datapipeline.utils.json_artifact import write_json_artifact
from tests.vector_input_helpers import register_vector_inputs


def test_full_regression_project_vectors(copy_fixture) -> None:
    project_root = copy_fixture("regression_project")
    project_path = project_root / "project.yaml"
    ctx = load_dataset_context(project_path)
    context = ctx.pipeline_context
    scaler_path = ctx.runtime.artifacts_root / "scaler.json"
    write_json_artifact(
        scaler_path,
        {
            "version": 1,
            "kind": "standard_scaler",
            "with_mean": True,
            "with_std": True,
            "epsilon": 1e-12,
            "statistics": {
                "linear_scaled": {
                    "count": 6,
                    "mean": 15.0,
                    "std": 3.0,
                }
            },
        },
    )
    ctx.runtime.artifacts.register(
        SCALER_STATISTICS,
        relative_path="scaler.json",
    )
    register_vector_inputs(
        ctx.runtime,
        ctx.features,
        ctx.dataset.group_by,
        targets=ctx.targets,
    )
    schema = materialize_vector_schema(
        ctx.runtime,
        SchemaTask(id="schema", output="schema.json"),
    )
    ctx.runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema.relative_path,
    )

    base_vectors = build_vector_pipeline(
        context,
        ctx.features,
        ctx.dataset.group_by,
        target_configs=ctx.targets,
        rectangular=False,
    )
    samples = list(post_process(context, base_vectors))

    assert len(samples) == 6

    expected = [
        (
            0,
            {
                "linear_scaled": -1.6666666666666667,
                "sine_window": [None, None],
                "humidity_partitioned__@location:north": 40.0,
                "humidity_partitioned__@location:south": 38.5,
            },
            100.0,
        ),
        (
            1,
            {
                "linear_scaled": -1.0,
                "sine_window": [0.5, 1.0],
                "humidity_partitioned__@location:north": 40.0,
                "humidity_partitioned__@location:south": 37.0,
            },
            102.0,
        ),
        (
            2,
            {
                "linear_scaled": -0.3333333333333333,
                "sine_window": [0.5, 0.0],
                "humidity_partitioned__@location:north": 41.0,
                "humidity_partitioned__@location:south": 37.75,
            },
            101.0,
        ),
        (
            3,
            {
                "linear_scaled": 0.3333333333333333,
                "sine_window": [None, -0.5],
                "humidity_partitioned__@location:north": 42.0,
                "humidity_partitioned__@location:south": 37.75,
            },
            105.0,
        ),
        (
            4,
            {
                "linear_scaled": 1.0,
                "sine_window": [-1.0, -0.5],
                "humidity_partitioned__@location:north": 41.5,
                "humidity_partitioned__@location:south": 39.0,
            },
            103.0,
        ),
        (
            5,
            {
                "linear_scaled": 1.6666666666666667,
                "sine_window": [0.0, 0.5],
                "humidity_partitioned__@location:north": 43.0,
                "humidity_partitioned__@location:south": 40.0,
            },
            107.0,
        ),
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
