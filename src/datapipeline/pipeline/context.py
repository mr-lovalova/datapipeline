from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Iterator, Mapping, Any

from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import (
    ArtifactManager,
    ArtifactSpec,
    ArtifactValue,
    PARTITIONED_IDS_SPEC,
)


_current_context: ContextVar[PipelineContext | None] = ContextVar(
    "datapipeline_pipeline_context", default=None
)


@dataclass
class PipelineContext:
    """Lightweight runtime context shared across pipeline stages."""

    runtime: Runtime
    _cache: dict[str, Any] = field(default_factory=dict)

    @property
    def artifacts(self) -> ArtifactManager:
        return self.runtime.artifacts

    def has_artifact(self, key: str) -> bool:
        return self.artifacts.has(key)

    def artifact_metadata(self, key: str) -> Mapping[str, Any]:
        return self.artifacts.metadata(key)

    def resolve_artifact_path(self, key: str):
        return self.artifacts.resolve_path(key)

    def require_artifact(self, spec: ArtifactSpec[ArtifactValue]) -> ArtifactValue:
        return self.artifacts.load(spec)

    def load_expected_ids(self) -> list[str]:
        ids = self._cache.get("expected_ids")
        if ids is None:
            ids = list(self.artifacts.load(PARTITIONED_IDS_SPEC))
            self._cache["expected_ids"] = ids
        return list(ids)

    @contextmanager
    def activate(self) -> Iterator[PipelineContext]:
        token = _current_context.set(self)
        try:
            yield self
        finally:
            _current_context.reset(token)


def current_context() -> PipelineContext:
    ctx = _current_context.get()
    if ctx is None:
        raise RuntimeError("No active pipeline context.")
    return ctx


def try_get_current_context() -> PipelineContext | None:
    return _current_context.get()
