import json
from pathlib import Path

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    VectorMetadataEntry,
    VectorSchemaArtifact,
    VectorSchemaEntry,
)
from datapipeline.config.tasks import SchemaTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import VECTOR_METADATA_SPEC
from datapipeline.utils.paths import ensure_parent


def _schema_entries(
    metadata_entries: tuple[VectorMetadataEntry, ...],
) -> tuple[VectorSchemaEntry, ...]:
    return tuple(
        VectorSchemaEntry(
            id=entry.id,
            kind=entry.kind,
            cadence=(
                entry.cadence if isinstance(entry, ListVectorMetadataEntry) else None
            ),
        )
        for entry in metadata_entries
    )


def materialize_vector_schema(
    runtime: Runtime,
    task_cfg: SchemaTask,
) -> ArtifactOutput:
    metadata = PipelineContext(runtime).require_artifact(VECTOR_METADATA_SPEC)
    feature_entries = _schema_entries(metadata.features)
    target_entries = _schema_entries(metadata.targets)

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

    meta: dict[str, object] = {
        "features": len(feature_entries),
        "targets": len(target_entries),
    }
    return ArtifactOutput(relative_path=str(relative_path), meta=meta)
