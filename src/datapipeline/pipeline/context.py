from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Iterator, Mapping, Any

from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    ArtifactManager,
    ArtifactSpec,
    ArtifactValue,
    PARTITIONED_IDS_SPEC,
    PARTITIONED_TARGET_IDS_SPEC,
)

logger = logging.getLogger(__name__)

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

    def load_expected_ids(self, *, payload: str = "features") -> list[str]:
        key = f"expected_ids:{payload}"
        ids = self._cache.get(key)
        if ids is None:
            spec = PARTITIONED_IDS_SPEC if payload != "targets" else PARTITIONED_TARGET_IDS_SPEC
            try:
                ids = list(self.artifacts.load(spec))
            except ArtifactNotRegisteredError:
                if payload == "targets":
                    ids = []
                    logger.debug(
                        "Target expected-id artifact ('%s') not registered; proceeding without a baseline.",
                        spec.key,
                    )
                else:
                    raise
            self._cache[key] = ids
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
