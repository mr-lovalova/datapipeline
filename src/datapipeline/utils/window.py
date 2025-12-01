from __future__ import annotations

from datetime import datetime

from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    VECTOR_METADATA_SPEC,
    VECTOR_SCHEMA_SPEC,
)
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
    try:
        from datapipeline.services.bootstrap.config import _project
    except Exception:  # pragma: no cover
        _project = None  # type: ignore[assignment]

    if _project is not None:
        try:
            proj = _project(runtime.project_yaml)
            globals_cfg = getattr(proj, "globals", None)
            globals_start = getattr(globals_cfg, "start_time", "auto")
            globals_end = getattr(globals_cfg, "end_time", "auto")
            if isinstance(globals_start, datetime):
                start = globals_start
            elif isinstance(globals_start, str) and globals_start.lower() != "auto":
                start = _parse_dt(globals_start)
            if isinstance(globals_end, datetime):
                end = globals_end
            elif isinstance(globals_end, str) and globals_end.lower() != "auto":
                end = _parse_dt(globals_end)
        except Exception:
            start = end = None

    if start is None or end is None:
        doc = None
        try:
            doc = runtime.artifacts.load(VECTOR_METADATA_SPEC)
        except ArtifactNotRegisteredError:
            doc = None
        except Exception:
            doc = None
        if not isinstance(doc, dict):
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
                    start = start or _parse_dt(window.get("start") or window.get("start_time"))
                    end = end or _parse_dt(window.get("end") or window.get("end_time"))
        except Exception:
            pass

    if rectangular_required and (start is None or end is None):
        raise RuntimeError("Window bounds unavailable (configure globals.start/end or rebuild metadata); rectangular output required.")
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
