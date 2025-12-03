from pathlib import Path
from datetime import datetime, timezone

from datapipeline.config.context import load_dataset_context
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process


def test_drop_missing_with_schema_and_partitioned_streams():
    project = Path("tests/fixtures/drop_null_project/project.yaml")
    dataset_ctx = load_dataset_context(project)
    context = dataset_ctx.pipeline_context

    vectors = build_vector_pipeline(
        context,
        dataset_ctx.features,
        dataset_ctx.dataset.group_by,
        target_configs=dataset_ctx.targets,
        rectangular=False,
    )
    processed = post_process(context, vectors)
    samples = list(processed)

    # Source emits ticks every 2h; ensure_cadence fills 1h gaps with None.
    # drop_missing with min_coverage=1.0 removes the filled (None) buckets,
    # keeping only the original ticks.
    expected_hours = [0, 2, 4]
    assert len(samples) == len(expected_hours)
    for sample, hour in zip(samples, expected_hours):
        ts = sample.key[0]
        assert isinstance(ts, datetime)
        assert ts.tzinfo == timezone.utc
        assert ts.hour == hour
        assert sample.features.values["time_linear"] is not None
