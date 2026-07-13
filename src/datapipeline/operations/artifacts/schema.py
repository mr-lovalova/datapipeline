import json
from pathlib import Path
from typing import Dict

from datapipeline.artifacts.models import VectorSchemaArtifact
from datapipeline.config.tasks import SchemaTask
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent

from .utils import (
    collect_schema_entries,
    configured_vectors_are_empty,
    schema_entries_from_stats,
)


def materialize_vector_schema(
    runtime: Runtime,
    task_cfg: SchemaTask,
) -> ArtifactOutput:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    validate_dataset_feature_identity(runtime, dataset)
    features_cfgs = list(dataset.features or [])
    feature_stats, feature_vectors = collect_schema_entries(
        runtime,
        features_cfgs,
        dataset.group_by,
        sample_keys=dataset.sample_keys,
        collect_metadata=False,
        progress_step="scan_features",
    )
    if configured_vectors_are_empty(features_cfgs, feature_vectors):
        raise RuntimeError(
            "Cannot materialize vector schema: "
            f"{len(features_cfgs)} configured features produced zero vectors. "
            "Check upstream source data and credentials."
        )
    target_stats: list[dict] = []
    target_cfgs = list(dataset.targets or [])
    if target_cfgs:
        target_stats, target_vectors = collect_schema_entries(
            runtime,
            target_cfgs,
            dataset.group_by,
            sample_keys=dataset.sample_keys,
            collect_metadata=False,
            progress_step="scan_targets",
        )
        if configured_vectors_are_empty(target_cfgs, target_vectors):
            raise RuntimeError(
                "Cannot materialize vector schema: "
                f"{len(target_cfgs)} configured targets produced zero vectors. "
                "Check upstream source data and credentials."
            )
    target_entries = schema_entries_from_stats(target_stats)
    feature_entries = schema_entries_from_stats(feature_stats)

    doc = VectorSchemaArtifact(
        schema_version=2,
        features=feature_entries,
        targets=target_entries,
    )

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)
    with destination.open("w", encoding="utf-8") as fh:
        json.dump(doc.model_dump(mode="json", exclude_none=True), fh, indent=2)

    meta: Dict[str, object] = {
        "features": len(feature_entries),
        "targets": len(target_entries),
    }
    return ArtifactOutput(relative_path=str(relative_path), meta=meta)
