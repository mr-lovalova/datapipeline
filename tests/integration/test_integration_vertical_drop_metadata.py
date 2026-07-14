import json

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.postprocess import PostprocessConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.full.nodes import apply_postprocess
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_METADATA, VECTOR_SCHEMA


def test_column_selection_uses_metadata_vector_count_without_mutating_schema(
    tmp_path,
) -> None:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project = tmp_path / "project.yaml"
    project.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(
        project_yaml=project,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(
            sample=SampleConfig(cadence="1h"),
            postprocess=PostprocessConfig.model_validate(
                {"columns": {"features": {"threshold": 0.5}}}
            ),
        ),
    )

    schema_path = artifacts_root / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "features": [
                    {"id": "sparse", "kind": "scalar"},
                    {"id": "complete", "kind": "scalar"},
                ],
                "targets": [],
            }
        ),
        encoding="utf-8",
    )
    metadata_path = artifacts_root / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "counts": {"feature_vectors": 100, "target_vectors": 0},
                "window": {
                    "start": "2024-01-01T00:00:00Z",
                    "end": "2024-01-05T00:00:00Z",
                    "mode": "union",
                    "size": 5,
                },
                "features": [
                    {
                        "id": "sparse",
                        "base_id": "sparse",
                        "kind": "scalar",
                        "present_count": 3,
                        "null_count": 0,
                    },
                    {
                        "id": "complete",
                        "base_id": "complete",
                        "kind": "scalar",
                        "present_count": 100,
                        "null_count": 0,
                    },
                ],
                "targets": [],
            }
        ),
        encoding="utf-8",
    )
    runtime.artifacts.register(VECTOR_SCHEMA, schema_path.name)
    runtime.artifacts.register(VECTOR_METADATA, metadata_path.name)
    context = PipelineContext(runtime)
    sample = Sample(
        key=(0,),
        features=Vector(values={"sparse": 1.0, "complete": 2.0}),
    )

    output = list(apply_postprocess(context, iter([sample])))

    assert output[0].features.values == {"complete": 2.0}
    assert [entry.id for entry in context.load_schema().features] == [
        "sparse",
        "complete",
    ]
