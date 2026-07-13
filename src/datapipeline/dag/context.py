from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterator, Mapping, Optional

from datapipeline.artifacts.models import (
    SchemaPayload,
    VectorSchemaArtifact,
)
from datapipeline.runtime import Runtime
from datapipeline.dag.transform_observability import ObserverRegistry
from datapipeline.services.artifacts import (
    ArtifactManager,
    ArtifactSpec,
    ArtifactValue,
    VECTOR_SCHEMA_SPEC,
)
from datapipeline.utils.window import resolve_window_bounds

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
    _schema: VectorSchemaArtifact | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _window_bounds_cache: dict[
        bool,
        tuple[datetime | None, datetime | None],
    ] = field(default_factory=dict, init=False, repr=False)

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

    def load_schema(self) -> VectorSchemaArtifact:
        if self._schema is None:
            self._schema = self.artifacts.load(VECTOR_SCHEMA_SPEC)
        return self._schema

    def remove_schema_ids(
        self,
        payload: SchemaPayload,
        identifiers: set[str],
    ) -> None:
        schema = self.load_schema()
        if payload == "features":
            entries = schema.features
        elif payload == "targets":
            entries = schema.targets
        else:
            raise ValueError("schema payload must be 'features' or 'targets'")
        self._schema = schema.model_copy(
            update={
                payload: tuple(
                    entry for entry in entries if entry.id not in identifiers
                )
            }
        )

    def window_bounds(self, *, rectangular_required: bool = False) -> tuple[datetime | None, datetime | None]:
        if rectangular_required in self._window_bounds_cache:
            return self._window_bounds_cache[rectangular_required]
        bounds = resolve_window_bounds(self.runtime, rectangular_required)
        if rectangular_required:
            self.runtime.window_bounds = bounds
        self._window_bounds_cache[rectangular_required] = bounds
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
