import json

import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.postprocess import PostprocessConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.dataset.nodes import (
    apply_postprocess,
    build_postprocess_plan,
)
from datapipeline.pipelines.dataset.pipeline import build_dataset_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_METADATA, VECTOR_SCHEMA


def _runtime(
    tmp_path,
    schema: dict,
    metadata: dict | None = None,
    postprocess: PostprocessConfig | None = None,
) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project = tmp_path / "project.yaml"
    project.write_text("version: 1\nartifact_revision: 1\n", encoding="utf-8")
    if postprocess is None:
        postprocess = PostprocessConfig()
    runtime = Runtime(
        project_yaml=project,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(
            sample=SampleConfig(cadence="1h"),
            postprocess=postprocess,
        ),
    )

    schema_path = artifacts_root / "schema.json"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")
    runtime.artifacts.register(VECTOR_SCHEMA, schema_path.name)
    if metadata is not None:
        metadata_path = artifacts_root / "metadata.json"
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        runtime.artifacts.register(VECTOR_METADATA, metadata_path.name)
    return runtime


def test_dataset_pipeline_assembles_before_postprocess(tmp_path) -> None:
    runtime = _runtime(
        tmp_path,
        schema={
            "schema_version": 2,
            "features": [{"id": "feature", "kind": "scalar"}],
            "targets": [],
        },
    )

    pipeline = build_dataset_pipeline(PipelineContext(runtime), [], "1h")

    assert pipeline.name == "dataset"
    assert [node.name for node in pipeline.nodes] == [
        "vector_assemble",
        "normalize_features",
        "reject_undeclared_targets",
    ]


def test_postprocess_has_one_explicit_execution_order(tmp_path) -> None:
    runtime = _runtime(
        tmp_path,
        schema={
            "schema_version": 2,
            "features": [
                {"id": "sparse", "kind": "scalar"},
                {"id": "value", "kind": "scalar"},
            ],
            "targets": [],
        },
        metadata={
            "schema_version": 1,
            "counts": {"feature_vectors": 2, "target_vectors": 0},
            "features": [
                {
                    "id": "sparse",
                    "base_id": "sparse",
                    "kind": "scalar",
                    "present_count": 0,
                    "null_count": 0,
                }
            ],
            "targets": [],
        },
        postprocess=PostprocessConfig.model_validate(
            {
                "columns": {
                    "features": {"threshold": 1.0, "ids": ["sparse"]},
                },
                "samples": {"features": {"threshold": 1.0, "ids": ["value"]}},
            }
        ),
    )
    samples = [
        Sample(key=(0,), features=Vector(values={"sparse": None})),
        Sample(
            key=(1,),
            features=Vector(values={"sparse": None, "value": 2.0}),
        ),
    ]

    context = PipelineContext(runtime)
    plan = build_postprocess_plan(context)
    output = list(apply_postprocess(context, iter(samples)))

    assert plan.feature_ids == ("value",)
    assert plan.target_ids == ()
    assert [node.name for node in plan.nodes] == [
        "select_features",
        "normalize_features",
        "reject_undeclared_targets",
        "filter_samples_by_features",
    ]
    assert [sample.features.values for sample in output] == [{"value": 2.0}]


def test_postprocess_applies_explicit_target_policies(tmp_path) -> None:
    runtime = _runtime(
        tmp_path,
        schema={
            "schema_version": 2,
            "features": [{"id": "feature", "kind": "scalar"}],
            "targets": [
                {"id": "sparse", "kind": "scalar"},
                {"id": "target", "kind": "scalar"},
            ],
        },
        metadata={
            "schema_version": 1,
            "counts": {"feature_vectors": 1, "target_vectors": 1},
            "features": [],
            "targets": [
                {
                    "id": "sparse",
                    "base_id": "sparse",
                    "kind": "scalar",
                    "present_count": 0,
                    "null_count": 0,
                }
            ],
        },
        postprocess=PostprocessConfig.model_validate(
            {
                "columns": {
                    "targets": {"threshold": 1.0, "ids": ["sparse"]},
                },
                "samples": {"targets": {"threshold": 1.0, "ids": ["target"]}},
            }
        ),
    )
    sample = Sample(
        key=(0,),
        features=Vector(values={"feature": 2.0}),
        targets=Vector(values={"sparse": None, "target": 1.0}),
    )

    output = list(apply_postprocess(PipelineContext(runtime), iter([sample])))

    assert output[0].features.values == {"feature": 2.0}
    assert output[0].targets is not None
    assert output[0].targets.values == {"target": 1.0}


def test_postprocess_rejects_schema_metadata_kind_drift(tmp_path) -> None:
    runtime = _runtime(
        tmp_path,
        schema={
            "schema_version": 2,
            "features": [{"id": "value", "kind": "scalar"}],
            "targets": [],
        },
        metadata={
            "schema_version": 1,
            "counts": {"feature_vectors": 1, "target_vectors": 0},
            "features": [
                {
                    "id": "value",
                    "base_id": "value",
                    "kind": "list",
                    "present_count": 1,
                    "null_count": 0,
                    "lengths": {"1": 1},
                    "cadence": {"target": 1},
                    "observed_elements": 1,
                }
            ],
            "targets": [],
        },
        postprocess=PostprocessConfig.model_validate(
            {"columns": {"features": {"threshold": 1.0}}}
        ),
    )

    with pytest.raises(ValueError, match="does not match the schema"):
        apply_postprocess(PipelineContext(runtime), iter(()))
