from datetime import datetime

from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    VECTOR_METADATA_SPEC,
    VECTOR_SCHEMA_SPEC,
)
from datapipeline.artifacts.models import VectorMetadata, VectorSchemaArtifact
from datapipeline.utils.time import parse_datetime
from datapipeline.runtime import Runtime


WindowBounds = tuple[datetime | None, datetime | None]


def resolve_window_bounds(
    runtime: Runtime,
    rectangular_required: bool,
) -> WindowBounds:
    existing = getattr(runtime, "window_bounds", None)
    cached_bounds = _usable_cached_bounds(existing, rectangular_required)
    if cached_bounds is not None:
        return cached_bounds

    start, end = _metadata_window_bounds(runtime)
    if start is None or end is None:
        schema_start, schema_end = _schema_window_bounds(runtime)
        start = start or schema_start
        end = end or schema_end

    if rectangular_required and (start is None or end is None):
        raise RuntimeError(
            "Window bounds unavailable (rebuild metadata to materialize build/metadata.json with a window); rectangular output required."
        )
    return start, end


def _usable_cached_bounds(
    existing: object,
    rectangular_required: bool,
) -> WindowBounds | None:
    if not isinstance(existing, tuple) or len(existing) != 2:
        return None
    cached_start, cached_end = existing
    if not rectangular_required and (
        cached_start is not None or cached_end is not None
    ):
        return cached_start, cached_end
    if cached_start is not None and cached_end is not None:
        return cached_start, cached_end
    return None


def _optional_artifact_doc(runtime: Runtime, spec) -> dict | None:
    try:
        doc = runtime.artifacts.load(spec)
    except (ArtifactNotRegisteredError, RuntimeError, ValueError):
        return None
    if isinstance(doc, dict):
        return doc
    return None


def _metadata_window_bounds(runtime: Runtime) -> WindowBounds:
    doc = _optional_artifact_doc(runtime, VECTOR_METADATA_SPEC)
    if doc is None:
        return None, None
    try:
        meta = VectorMetadata.model_validate(doc)
    except ValueError:
        return None, None
    if meta.window is None:
        return None, None
    return meta.window.start, meta.window.end


def _schema_window_bounds(runtime: Runtime) -> WindowBounds:
    doc = _optional_artifact_doc(runtime, VECTOR_SCHEMA_SPEC)
    if doc is None:
        return None, None
    try:
        schema = VectorSchemaArtifact.model_validate(doc)
    except ValueError:
        return None, None
    if schema.window is not None:
        return schema.window.start, schema.window.end
    return None, None


def _parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return parse_datetime(str(value))
    except ValueError:
        return None
