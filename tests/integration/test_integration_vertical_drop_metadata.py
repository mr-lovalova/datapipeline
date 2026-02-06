import json
import shutil

from datapipeline.config.context import load_dataset_context
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.context import PipelineContext
from datapipeline.transforms.vector import VectorDropTransform
from datapipeline.services.constants import VECTOR_SCHEMA_METADATA


def test_vertical_drop_uses_window_size_metadata(tmp_path, copy_fixture):
    project_root = copy_fixture("incomplete_generation_project")
    project = project_root / "project.yaml"
    build_dir = project.parent / "build"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    ctx = load_dataset_context(project)
    runtime = ctx.runtime

    # Craft metadata where global counts would force a drop, but window.size keeps coverage above threshold.
    metadata_doc = {
        "schema_version": 1,
        "counts": {"feature_vectors": 100},
        "window": {
            "start": "2022-01-01T04:00:00Z",
            "end": "2022-01-01T08:00:00Z",
            "mode": "intersection",
            "size": 5,
        },
        "features": [
            {
                "id": "onshore_mwh_scaled__@municipality:849",
                "present_count": 3,
                "null_count": 0,
            }
        ],
        "targets": [],
    }

    # Write metadata into the fixture artifacts root and register it for the runtime.
    metadata_path = runtime.artifacts_root / "metadata.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata_doc, indent=2), encoding="utf-8")
    runtime.artifacts.register(
        VECTOR_SCHEMA_METADATA,
        relative_path=metadata_path.relative_to(runtime.artifacts_root).as_posix(),
    )

    pipeline_ctx = PipelineContext(runtime)
    drop = VectorDropTransform(axis="vertical", threshold=0.5)
    drop.bind_context(pipeline_ctx)

    sample = Sample(
        key=(0,),
        features=Vector(values={"onshore_mwh_scaled__@municipality:849": 1.0}),
    )

    out = list(drop.apply(iter([sample])))
    assert out and out[0].features is not None
    assert "onshore_mwh_scaled__@municipality:849" in out[0].features.values
