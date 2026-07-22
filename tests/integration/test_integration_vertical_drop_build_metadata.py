import shutil

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.artifacts.specs import (
    SCALER_STATISTICS,
    SERIES,
    VECTOR_METADATA,
)
from datapipeline.config.dataset.postprocess import PostprocessConfig
from datapipeline.config.tasks import (
    MetadataTask,
    ScalerTask,
    SeriesTask,
)
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.scaler import materialize_scaler_statistics
from datapipeline.operations.artifacts.series import (
    materialize_series,
)
from datapipeline.pipelines.dataset.nodes import apply_postprocess
from datapipeline.pipelines.sample.source import open_samples
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime


def test_column_selection_counts_absent_sequence_opportunities(copy_fixture):
    project_root = copy_fixture("incomplete_prices_project")
    project = project_root / "project.yaml"

    # Ensure a clean artifact folder so builds reflect current code each run.
    build_dir = project.parent / "build"
    if build_dir.exists():
        shutil.rmtree(build_dir)

    # Build metadata artifact for the fixture project.
    definition = load_pipeline(project)
    runtime = compile_runtime(definition)
    hydrate_runtime_artifacts_for_pipeline(runtime, definition)
    scaler_rel = materialize_scaler_statistics(
        runtime,
        ScalerTask(id="scaler", output="scaler.json"),
    )
    if scaler_rel:
        runtime.artifacts.register(
            SCALER_STATISTICS, relative_path=scaler_rel.relative_path
        )
    series_rel = materialize_series(
        runtime,
        SeriesTask(id="series", output="series/manifest.json"),
    )
    runtime.artifacts.register(
        SERIES,
        relative_path=series_rel.relative_path,
    )
    meta_rel = materialize_metadata(
        runtime,
        MetadataTask(id="metadata", output="metadata.json"),
    )
    assert meta_rel is not None
    runtime.artifacts.register(
        VECTOR_METADATA,
        relative_path=meta_rel.relative_path,
    )
    runtime.dataset = runtime.dataset.model_copy(
        update={
            "postprocess": PostprocessConfig.model_validate(
                {"columns": {"features": {"threshold": 1.0}}}
            )
        }
    )

    dataset = runtime.dataset
    ctx = PipelineContext(runtime)

    assembled_samples = open_samples(
        ctx,
        dataset.features,
        dataset.sample.cadence,
        target_configs=dataset.targets,
        rectangular=False,
    )
    samples = list(apply_postprocess(ctx, assembled_samples))

    assert all(
        "spot_eur_sequence__@area:DK1" not in sample.features.values
        for sample in samples
    )
    assert any(
        "spot_eur_scaled__@area:DK1" in sample.features.values for sample in samples
    )
