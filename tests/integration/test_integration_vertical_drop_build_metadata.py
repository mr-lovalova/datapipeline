import shutil

from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.operations.artifacts.scaler import materialize_scaler_statistics
from datapipeline.config.context import load_dataset
from datapipeline.config.tasks import MetadataTask, SchemaTask, ScalerTask
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.constants import (
    VECTOR_SCHEMA_METADATA,
    VECTOR_SCHEMA,
    SCALER_STATISTICS,
)
from datapipeline.transforms.vector import VectorDropTransform


def test_vertical_drop_respects_element_coverage_after_metadata_build(copy_fixture):
    project_root = copy_fixture("incomplete_prices_project")
    project = project_root / "project.yaml"

    # Ensure a clean artifact folder so builds reflect current code each run.
    build_dir = project.parent / "build"
    if build_dir.exists():
        shutil.rmtree(build_dir)

    # Build metadata artifact for the fixture project.
    runtime = bootstrap(project)
    schema_rel = materialize_vector_schema(
        runtime,
        SchemaTask(id="schema", output="schema.json"),
    )
    if schema_rel:
        runtime.artifacts.register(VECTOR_SCHEMA, relative_path=schema_rel.relative_path)
    scaler_rel = materialize_scaler_statistics(
        runtime,
        ScalerTask(id="scaler", split_label="all", output="scaler.json"),
    )
    if scaler_rel:
        runtime.artifacts.register(
            SCALER_STATISTICS, relative_path=scaler_rel.relative_path
        )
    meta_rel = materialize_metadata(
        runtime,
        MetadataTask(id="metadata", output="metadata.json"),
    )
    assert meta_rel is not None
    runtime.artifacts.register(
        VECTOR_SCHEMA_METADATA,
        relative_path=meta_rel.relative_path,
    )

    dataset = load_dataset(project, "vectors")
    ctx = PipelineContext(runtime)

    # Build raw vectors (no postprocess) and apply vertical drop with threshold=1.0.
    vectors = build_vector_pipeline(
        ctx,
        dataset.features,
        dataset.group_by,
        target_configs=dataset.targets,
        rectangular=False,
    )
    drop = VectorDropTransform(axis="vertical", threshold=1.0, payload="features")
    drop.bind_context(ctx)
    samples = list(drop.apply(vectors))

    # Sequences are fully populated (present buckets * cadence == observed_elements), so they should be kept.
    assert any(
        "spot_eur_sequence__@area:DK1" in sample.features.values for sample in samples
    )
