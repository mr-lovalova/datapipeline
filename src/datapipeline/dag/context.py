import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Iterator, Mapping, Any, Callable, Optional
from datetime import datetime

from datapipeline.artifacts.models import VectorSchemaArtifact
from datapipeline.runtime import Runtime
from datapipeline.dag.transform_observability import ObserverRegistry
from datapipeline.services.artifacts import (
    ArtifactNotRegisteredError,
    ArtifactManager,
    ArtifactSpec,
    ArtifactValue,
    VECTOR_SCHEMA_SPEC,
)
from datapipeline.utils.window import resolve_window_bounds

logger = logging.getLogger(__name__)

_current_context: ContextVar["PipelineContext | None"] = ContextVar(
    "datapipeline_pipeline_context", default=None
)


@dataclass
class PipelineContext:
    """Lightweight runtime context shared across pipeline stages."""

    runtime: Runtime
    transform_observer: Callable[..., None] | None = None
    observer_registry: Optional[ObserverRegistry] = None
    execution_observer: object | None = None
    heartbeat_interval_seconds: float | None = None
    _cache: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.execution_observer is None:
            self.execution_observer = getattr(
                self.runtime,
                "execution_observer",
                None,
            )
        if self.heartbeat_interval_seconds is None:
            self.heartbeat_interval_seconds = getattr(
                self.runtime,
                "heartbeat_interval_seconds",
                None,
            )

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
        cached = self._cache.get(key)
        if cached is not None:
            return list(cached)
        entries = self.load_schema(payload=payload)
        if not entries:
            if payload == "targets":
                logger.debug("Target schema entries missing; proceeding without target baseline.")
                self._cache[key] = []
                return []
            raise RuntimeError("Vector schema artifact missing; run `jerry build` to materialize build/schema.json.")
        ids = [entry["id"] for entry in entries if isinstance(entry.get("id"), str)]
        self._cache[key] = ids
        return list(ids)

    def load_schema(self, *, payload: str = "features") -> list[dict[str, Any]]:
        key = f"schema:{payload}"
        cached = self._cache.get(key)
        if cached is None:
            try:
                doc = self.artifacts.load(VECTOR_SCHEMA_SPEC)
            except ArtifactNotRegisteredError:
                cached = []
            else:
                try:
                    schema = VectorSchemaArtifact.model_validate(doc)
                except ValueError:
                    cached = []
                else:
                    cached = schema.entries_for_payload(payload)
            self._cache[key] = cached
        return [dict(entry) for entry in cached] if cached else []

    @property
    def schema_required(self) -> bool:
        return bool(getattr(self.runtime, "schema_required", True))

    def window_bounds(self, *, rectangular_required: bool = False) -> tuple[datetime | None, datetime | None]:
        key = "window_bounds:required" if rectangular_required else "window_bounds:optional"
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        bounds = resolve_window_bounds(self.runtime, rectangular_required)
        if rectangular_required:
            self.runtime.window_bounds = bounds
        self._cache[key] = bounds
        return bounds

    @property
    def start_time(self) -> datetime | None:
        start, _ = self.window_bounds()
        return start

    @property
    def end_time(self) -> datetime | None:
        _, end = self.window_bounds()
        return end

    @contextmanager
    def activate(self) -> Iterator["PipelineContext"]:
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
