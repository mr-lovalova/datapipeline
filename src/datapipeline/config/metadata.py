from typing import Any

from datapipeline.artifacts.models import VectorMetadata, Window

# Shared keys for vector metadata counts
FEATURE_VECTORS_COUNT_KEY = "feature_vectors"
TARGET_VECTORS_COUNT_KEY = "target_vectors"


def build_vector_metadata_lookup(
    payload: Any,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    doc = VectorMetadata.model_validate(payload)
    expected_feature_ids = [
        entry["id"]
        for entry in doc.features
        if isinstance(entry, dict) and isinstance(entry.get("id"), str)
    ]
    schema_meta_by_id = {
        entry["id"]: entry
        for entry in [*doc.features, *doc.targets]
        if isinstance(entry, dict) and isinstance(entry.get("id"), str)
    }
    return expected_feature_ids, schema_meta_by_id
