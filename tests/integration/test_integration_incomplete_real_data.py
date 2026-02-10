import pytest

from datapipeline.config.context import load_dataset_context
from datapipeline.build.tasks.schema import materialize_vector_schema
from datapipeline.build.tasks.scaler import materialize_scaler_statistics
from datapipeline.config.tasks import SchemaTask, ScalerTask
from datapipeline.services.constants import VECTOR_SCHEMA, SCALER_STATISTICS
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process


def _vector_samples(project_yaml):
    ctx = load_dataset_context(project_yaml)
    context = ctx.pipeline_context
    runtime = ctx.runtime

    # Ensure artifacts are materialized for the test run.
    schema_rel = materialize_vector_schema(
        runtime, SchemaTask(kind="schema", output="schema.json")
    )
    if schema_rel:
        runtime.artifacts.register(VECTOR_SCHEMA, relative_path=schema_rel[0])
    scaler_rel = materialize_scaler_statistics(
        runtime, ScalerTask(kind="scaler", split_label="all", output="scaler.json")
    )
    if scaler_rel:
        runtime.artifacts.register(SCALER_STATISTICS, relative_path=scaler_rel[0])

    vectors = build_vector_pipeline(
        context,
        ctx.features,
        ctx.dataset.group_by,
        target_configs=ctx.targets,
        rectangular=False,
    )
    return list(post_process(context, vectors))


def test_incomplete_prices_project_vectors(copy_fixture):
    project_root = copy_fixture("incomplete_prices_project")
    project = project_root / "project.yaml"
    samples = _vector_samples(project)

    assert len(samples) == 8

    first = samples[0]
    assert first.key[0].hour == 3
    assert len(first.features.values) == 14  # 7 areas x 2 feature ids
    values = first.features.values
    assert values["spot_eur_scaled__@area:DK1"] == pytest.approx(-1.0020365384, rel=1e-6)
    assert values["spot_eur_scaled__@area:SYSTEM"] == pytest.approx(-1.3841396412, rel=1e-6)
    assert values["spot_eur_sequence__@area:DK1"] == pytest.approx(
        [37.669998, 39.700001, 40.59], rel=1e-6
    )

    # Window stride keeps only a subset of buckets populated; others stay empty after postprocess.
    empty_window = samples[1].features.values["spot_eur_sequence__@area:DK1"]
    assert all(v is None for v in empty_window)

    populated = samples[2].features.values["spot_eur_sequence__@area:DK1"]
    assert populated == pytest.approx([40.59, 43.259998, 49.66], rel=1e-6)


def test_incomplete_generation_project_alignment(copy_fixture):
    project_root = copy_fixture("incomplete_generation_project")
    project = project_root / "project.yaml"
    samples = _vector_samples(project)

    assert len(samples) == 9

    expected_features = {
        "onshore_mwh_scaled__@municipality:400",
        "onshore_mwh_scaled__@municipality:550",
        "onshore_mwh_scaled__@municipality:849",
        "onshore_mwh_window__@municipality:400",
        "onshore_mwh_window__@municipality:550",
        "onshore_mwh_window__@municipality:849",
    }

    first = samples[0]
    assert set(first.features.keys()) == expected_features
    assert first.targets is not None
    assert first.targets.values["dk1_price"] == pytest.approx(39.700001, rel=1e-6)
    assert first.features.values["onshore_mwh_scaled__@municipality:849"] == pytest.approx(
        0.2560143735, rel=1e-6
    )
    assert first.features.values["onshore_mwh_window__@municipality:849"] == [None, None]

    window = samples[3].features.values["onshore_mwh_window__@municipality:849"]
    assert window == pytest.approx([2.351027, 1.049725], rel=1e-6)
