from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.split import SplitConfig
from datapipeline.domain.stream import RecordStream

from datapipeline.registries.registry import Registry
from datapipeline.sources.models.source import Source
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.transforms.spec import TransformSpec

StreamPipelineKind = Literal["ingest", "stream"]


@dataclass(frozen=True)
class StreamRuntimeSpec:
    pipeline: StreamPipelineKind


@dataclass
class Registries:
    """Container for all runtime registries.

    Replaces global registries with an instance-scoped collection so multiple
    projects can run in the same process without clobbering shared state.
    """

    sources: Registry[str, Source] = field(default_factory=Registry)
    mappers: Registry[str, Any] = field(default_factory=Registry)
    stream_sources: Registry[str, RecordStream[Any]] = field(default_factory=Registry)
    stream_specs: Registry[str, StreamRuntimeSpec] = field(default_factory=Registry)
    record_operations: Registry[str, Sequence[TransformSpec] | None] = field(
        default_factory=Registry
    )
    postprocesses: Registry[str, Sequence[TransformSpec] | None] = field(
        default_factory=Registry
    )

    # Per-stream policies
    stream_operations: Registry[str, Sequence[TransformSpec] | None] = field(
        default_factory=Registry
    )
    debug_operations: Registry[str, Sequence[TransformSpec] | None] = field(
        default_factory=Registry
    )
    partition_by: Registry[str, str | list[str] | None] = field(
        default_factory=Registry
    )
    feature_id_by: Registry[str, str | list[str] | None] = field(
        default_factory=Registry
    )
    presorted: Registry[str, bool] = field(default_factory=Registry)

    def clear_all(self) -> None:
        for reg in (
            self.stream_operations,
            self.debug_operations,
            self.partition_by,
            self.feature_id_by,
            self.presorted,
            self.record_operations,
            self.postprocesses,
            self.sources,
            self.mappers,
            self.stream_sources,
            self.stream_specs,
        ):
            reg.clear()


@dataclass
class Runtime:
    """Holds the active project context and runtime registries."""

    project_yaml: Path
    artifacts_root: Path
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    registries: Registries = field(default_factory=Registries)
    split: SplitConfig | None = None
    split_labels: tuple[str, ...] = ()
    sample_keys: list[str] = field(default_factory=list)
    window_bounds: tuple[datetime | None, datetime | None] | None = None
    heartbeat_interval_seconds: float | None = None
    execution_observer: object | None = None
    artifacts: ArtifactManager = field(init=False)

    def __post_init__(self) -> None:
        self.artifacts = ArtifactManager(self.artifacts_root)
