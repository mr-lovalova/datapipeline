from datetime import datetime, timezone

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.config.tasks import MetadataTask, SchemaTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.pipelines.full.nodes import apply_postprocess
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.services.constants import VECTOR_METADATA, VECTOR_SCHEMA
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime
from tests.vector_input_helpers import register_vector_inputs


def test_drop_with_schema_and_partitioned_streams(copy_fixture):
    project_root = copy_fixture("drop_null_project")
    project = project_root / "project.yaml"
    definition = load_pipeline(project)
    runtime = compile_runtime(definition)
    hydrate_runtime_artifacts_for_pipeline(runtime, definition)
    dataset = definition.dataset
    context = PipelineContext(runtime)
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

    vectors = build_vector_pipeline(
        context,
        dataset.features,
        dataset.sample.cadence,
        target_configs=dataset.targets,
        rectangular=False,
    )
    processed = apply_postprocess(context, vectors)
    samples = list(processed)

    # Source emits ticks every 2h; ensure_cadence fills 1h gaps with None.
    # drop with axis=horizontal, threshold=1.0 removes the filled (None) buckets,
    # keeping only the original ticks.
    expected_hours = [0, 2, 4]
    assert len(samples) == len(expected_hours)
    for sample, hour in zip(samples, expected_hours):
        ts = sample.key[0]
        assert isinstance(ts, datetime)
        assert ts.tzinfo == timezone.utc
        assert ts.hour == hour
        assert sample.features.values["time_linear"] is not None
