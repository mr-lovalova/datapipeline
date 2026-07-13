from datetime import datetime, timezone

from datapipeline.config.context import load_dataset_context
from datapipeline.config.tasks import MetadataTask, SchemaTask
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.pipelines.full.nodes import apply_postprocess
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.services.constants import VECTOR_METADATA, VECTOR_SCHEMA
from tests.vector_input_helpers import register_vector_inputs


def test_drop_with_schema_and_partitioned_streams(copy_fixture):
    project_root = copy_fixture("drop_null_project")
    project = project_root / "project.yaml"
    dataset_ctx = load_dataset_context(project)
    context = dataset_ctx.pipeline_context
    register_vector_inputs(
        dataset_ctx.runtime,
        dataset_ctx.features,
        dataset_ctx.dataset.sample.cadence,
        targets=dataset_ctx.targets,
    )
    metadata = materialize_metadata(
        dataset_ctx.runtime,
        MetadataTask(id="metadata", output="metadata.json"),
    )
    dataset_ctx.runtime.artifacts.register(
        VECTOR_METADATA,
        relative_path=metadata.relative_path,
    )
    schema = materialize_vector_schema(
        dataset_ctx.runtime,
        SchemaTask(id="schema", output="schema.json"),
    )
    dataset_ctx.runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema.relative_path,
    )

    vectors = build_vector_pipeline(
        context,
        dataset_ctx.features,
        dataset_ctx.dataset.sample.cadence,
        target_configs=dataset_ctx.targets,
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
