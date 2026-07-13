import shutil

from datapipeline.config.postprocess import PostprocessConfig
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.vector_inputs import materialize_vector_inputs
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.operations.artifacts.scaler import materialize_scaler_statistics
from datapipeline.config.context import load_dataset
from datapipeline.config.tasks import (
    MetadataTask,
    SchemaTask,
    ScalerTask,
    VectorInputsTask,
)
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.pipelines.full.nodes import apply_postprocess
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.constants import (
    VECTOR_METADATA,
    VECTOR_SCHEMA,
    SCALER_STATISTICS,
    VECTOR_INPUTS,
)


def test_column_selection_counts_absent_sequence_opportunities(copy_fixture):
    project_root = copy_fixture("incomplete_prices_project")
    project = project_root / "project.yaml"

    # Ensure a clean artifact folder so builds reflect current code each run.
    build_dir = project.parent / "build"
    if build_dir.exists():
        shutil.rmtree(build_dir)

    # Build metadata artifact for the fixture project.
    runtime = bootstrap(project)
    scaler_rel = materialize_scaler_statistics(
        runtime,
        ScalerTask(id="scaler", split_label="all", output="scaler.json"),
    )
    if scaler_rel:
        runtime.artifacts.register(
            SCALER_STATISTICS, relative_path=scaler_rel.relative_path
        )
    vector_inputs_rel = materialize_vector_inputs(
        runtime,
        VectorInputsTask(id="vector_inputs", output="vector_inputs/manifest.json"),
    )
    runtime.artifacts.register(
        VECTOR_INPUTS,
        relative_path=vector_inputs_rel.relative_path,
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
    schema_rel = materialize_vector_schema(
        runtime,
        SchemaTask(id="schema", output="schema.json"),
    )
    runtime.artifacts.register(VECTOR_SCHEMA, relative_path=schema_rel.relative_path)
    runtime.postprocess = PostprocessConfig.model_validate(
        {"columns": {"features": {"threshold": 1.0}}}
    )

    dataset = load_dataset(project)
    ctx = PipelineContext(runtime)

    vectors = build_vector_pipeline(
        ctx,
        dataset.features,
        dataset.sample.cadence,
        target_configs=dataset.targets,
        rectangular=False,
    )
    samples = list(apply_postprocess(ctx, vectors))

    assert all(
        "spot_eur_sequence__@area:DK1" not in sample.features.values
        for sample in samples
    )
    assert any(
        "spot_eur_scaled__@area:DK1" in sample.features.values for sample in samples
    )
