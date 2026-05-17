from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    VECTOR_METADATA_SPEC,
    VECTOR_SCHEMA_SPEC,
)
from datapipeline.utils.window import resolve_window_bounds


START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 2, tzinfo=timezone.utc)


class _Artifacts:
    def __init__(self, docs):
        self._docs = docs

    def load(self, spec):
        try:
            return self._docs[spec.key]
        except KeyError as exc:
            raise ArtifactNotRegisteredError(spec.key) from exc


def _runtime(docs=None, window_bounds=None):
    return SimpleNamespace(
        artifacts=_Artifacts(docs or {}),
        window_bounds=window_bounds,
    )


def test_resolve_window_bounds_uses_complete_cached_bounds_when_required() -> None:
    runtime = _runtime(window_bounds=(START, END))

    assert resolve_window_bounds(runtime, rectangular_required=True) == (START, END)


def test_resolve_window_bounds_prefers_metadata_window() -> None:
    runtime = _runtime(
        {
            VECTOR_METADATA_SPEC.key: {
                "window": {
                    "start": START.isoformat(),
                    "end": END.isoformat(),
                }
            },
            VECTOR_SCHEMA_SPEC.key: {
                "window": {
                    "start": "2020-01-01T00:00:00Z",
                    "end": "2020-01-02T00:00:00Z",
                }
            },
        }
    )

    assert resolve_window_bounds(runtime, rectangular_required=True) == (START, END)


def test_resolve_window_bounds_fills_missing_metadata_bound_from_schema() -> None:
    runtime = _runtime(
        {
            VECTOR_METADATA_SPEC.key: {
                "window": {
                    "start": START.isoformat(),
                }
            },
            VECTOR_SCHEMA_SPEC.key: {
                "window": {
                    "start": "2020-01-01T00:00:00Z",
                    "end": END.isoformat(),
                }
            },
        }
    )

    assert resolve_window_bounds(runtime, rectangular_required=True) == (START, END)


def test_resolve_window_bounds_ignores_legacy_schema_meta_window() -> None:
    runtime = _runtime(
        {
            VECTOR_SCHEMA_SPEC.key: {
                "meta": {
                    "window": {
                        "start": START.isoformat(),
                        "end": END.isoformat(),
                    }
                }
            }
        }
    )

    with pytest.raises(RuntimeError, match="Window bounds unavailable"):
        resolve_window_bounds(runtime, rectangular_required=True)


def test_resolve_window_bounds_requires_complete_rectangular_window() -> None:
    runtime = _runtime(
        {
            VECTOR_SCHEMA_SPEC.key: {
                "window": {
                    "start": START.isoformat(),
                }
            }
        }
    )

    with pytest.raises(RuntimeError, match="Window bounds unavailable"):
        resolve_window_bounds(runtime, rectangular_required=True)
