from __future__ import annotations

from datetime import datetime

from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    VECTOR_METADATA_SPEC,
    VECTOR_SCHEMA_SPEC,
)
from datapipeline.config.metadata import VectorMetadata
from datapipeline.utils.time import parse_datetime
from datapipeline.runtime import Runtime


def resolve_window_bounds(runtime: Runtime, rectangular_required: bool) -> tuple[datetime | None, datetime | None]:
    existing = getattr(runtime, "window_bounds", None)
    if isinstance(existing, tuple) and len(existing) == 2:
        cached_start, cached_end = existing
        if not rectangular_required and (cached_start is not None or cached_end is not None):
            return cached_start, cached_end
        if cached_start is not None and cached_end is not None:
            return cached_start, cached_end

    start = end = None

    # Window bounds are derived from artifacts (metadata/schema) only.
    doc = None
    try:
        doc = runtime.artifacts.load(VECTOR_METADATA_SPEC)
    except ArtifactNotRegisteredError:
        doc = None
    except Exception:
        doc = None
    if isinstance(doc, dict):
        try:
            meta = VectorMetadata.model_validate(doc)
            window = meta.window
            if window is not None:
                start = window.start
                end = window.end
        except Exception:
            start = end = None

    # Fallback: try schema/spec window if metadata missing or invalid.
    if start is None or end is None:
        try:
            doc = runtime.artifacts.load(VECTOR_SCHEMA_SPEC)
        except ArtifactNotRegisteredError:
            doc = None
        except Exception:
            doc = None
        try:
            if isinstance(doc, dict):
                window = doc.get("window") or doc.get("meta", {}).get("window")
                if isinstance(window, dict):
                    start = _parse_dt(window.get("start")) or start
                    end = _parse_dt(window.get("end")) or end
        except Exception:
            pass

    if rectangular_required and (start is None or end is None):
        raise RuntimeError(
            "Window bounds unavailable (rebuild metadata to materialize metadata.json with a window); rectangular output required."
        )
    return start, end


def _parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return parse_datetime(str(value))
    except Exception:
        return None
