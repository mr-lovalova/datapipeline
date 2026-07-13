from typing import Any

from datapipeline.artifacts.models import VectorMetadata


def build_vector_metadata_lookup(
    metadata: VectorMetadata,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    expected_feature_ids = [entry.id for entry in metadata.features]
    schema_meta_by_id = {
        entry.id: entry.model_dump(mode="python")
        for entry in (*metadata.features, *metadata.targets)
    }
    return expected_feature_ids, schema_meta_by_id
