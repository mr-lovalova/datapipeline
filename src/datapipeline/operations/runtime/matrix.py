from itertools import islice
from pathlib import Path

from datapipeline.analysis.vector.matrix import MatrixBuilder, write_matrix_html
from datapipeline.config.tasks import MatrixTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.pipelines.dataset.nodes import build_postprocess_plan
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import VECTOR_METADATA_SPEC


def run_matrix_operation(
    runtime: Runtime,
    task: MatrixTask,
    limit: int | None = None,
) -> RuntimeOutput:
    options = task.options
    dataset = runtime.dataset
    context = PipelineContext(runtime)
    metadata = context.require_artifact(VECTOR_METADATA_SPEC)
    context.window_bounds(rectangular_required=True)

    samples = build_vector_pipeline(
        context,
        dataset.features,
        dataset.sample.cadence,
        target_configs=dataset.targets,
        rectangular=True,
        sample_keys=dataset.sample.keys,
    )
    feature_entries = metadata.features
    target_entries = metadata.targets
    if options.stage == "postprocessed":
        plan = build_postprocess_plan(context)
        feature_entries, target_entries = plan.select_metadata(metadata)
        samples = plan.apply(samples)

    builder = MatrixBuilder(feature_entries, target_entries, options.max_cells)
    limited_samples = islice(samples, limit) if limit is not None else samples
    try:
        for sample in limited_samples:
            targets = sample.targets.values if sample.targets is not None else {}
            builder.add(sample.key, sample.features.values, targets)
    finally:
        close = getattr(samples, "close", None)
        if callable(close):
            close()
    matrix = builder.finish()

    def render_html(destination: Path) -> Path:
        return write_matrix_html(matrix, destination)

    return RuntimeOutput(
        rows=matrix.output_rows(),
        html_renderer=render_html,
    )
