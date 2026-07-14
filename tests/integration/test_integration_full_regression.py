import pytest

from datapipeline.artifacts.scaler import (
    ScalerStatistics,
    StandardScalerArtifact,
    save_scaler_artifact,
)
from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.config.tasks import MetadataTask, SchemaTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.pipelines.full.nodes import apply_postprocess
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
)
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime
from tests.vector_input_helpers import register_vector_inputs


def test_full_regression_project_vectors(copy_fixture) -> None:
    project_root = copy_fixture("regression_project")
    project_path = project_root / "project.yaml"
    definition = load_pipeline(project_path)
    runtime = compile_runtime(definition)
    hydrate_runtime_artifacts_for_pipeline(runtime, definition)
    dataset = definition.dataset
    context = PipelineContext(runtime)
    scaler_path = runtime.artifacts_root / "scaler.json"
    save_scaler_artifact(
        scaler_path,
        StandardScalerArtifact(
            with_mean=True,
            with_std=True,
            epsilon=1e-12,
            observations=6,
            statistics={
                "linear_scaled": ScalerStatistics(
                    count=6,
                    mean=15.0,
                    std=3.0,
                )
            },
        ),
    )
    runtime.artifacts.register(
        SCALER_STATISTICS,
        relative_path="scaler.json",
    )
    register_vector_inputs(
        runtime,
        dataset.features,
        dataset.sample.cadence,
        targets=dataset.targets,
    )
    metadata = materialize_metadata(
        runtime,
        MetadataTask(id="metadata", output="metadata.json"),
    )
    runtime.artifacts.register(
        VECTOR_METADATA,
        relative_path=metadata.relative_path,
    )
    schema = materialize_vector_schema(
        runtime,
        SchemaTask(id="schema", output="schema.json"),
    )
    runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema.relative_path,
    )

    base_vectors = build_vector_pipeline(
        context,
        dataset.features,
        dataset.sample.cadence,
        target_configs=dataset.targets,
        rectangular=False,
    )
    samples = list(apply_postprocess(context, base_vectors))

    assert len(samples) == 6

    expected = [
        (
            0,
            {
                "linear_scaled": -1.6666666666666667,
                "sine_window": [0.0, 0.5],
                "humidity_partitioned__@location:north": 40.0,
                "humidity_partitioned__@location:south": 38.5,
            },
            100.0,
        ),
        (
            1,
            {
                "linear_scaled": -1.0,
                "sine_window": [1.0, 0.5],
                "humidity_partitioned__@location:north": 40.0,
                "humidity_partitioned__@location:south": 37.0,
            },
            102.0,
        ),
        (
            2,
            {
                "linear_scaled": -0.3333333333333333,
                "sine_window": [0.0, None],
                "humidity_partitioned__@location:north": 41.0,
                "humidity_partitioned__@location:south": 37.75,
            },
            101.0,
        ),
        (
            3,
            {
                "linear_scaled": 0.3333333333333333,
                "sine_window": [-0.5, -1.0],
                "humidity_partitioned__@location:north": 42.0,
                "humidity_partitioned__@location:south": 37.75,
            },
            105.0,
        ),
        (
            4,
            {
                "linear_scaled": 1.0,
                "sine_window": [-0.5, 0.0],
                "humidity_partitioned__@location:north": 41.5,
                "humidity_partitioned__@location:south": 39.0,
            },
            105.0,
        ),
        (
            5,
            {
                "linear_scaled": 1.6666666666666667,
                "sine_window": [None, None],
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
