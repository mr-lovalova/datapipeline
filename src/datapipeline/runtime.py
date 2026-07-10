from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Literal, Mapping, Optional, Sequence, Union
from datetime import datetime

from datapipeline.config.profiles import ServeProfile
from datapipeline.config.split import SplitConfig
from datapipeline.domain.stream import RecordStream

from datapipeline.registries.registry import Registry
from datapipeline.sources.models.source import Source
from datapipeline.services.artifacts import ArtifactManager

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
    record_operations: Registry[str, Sequence[Mapping[str, object]]] = field(
        default_factory=Registry
    )
    feature_transforms: Registry[str, Sequence[Mapping[str, object]]] = field(
        default_factory=Registry
    )
    postprocesses: Registry[str, Any] = field(default_factory=Registry)

    # Per-stream policies
    stream_operations: Registry[str, Sequence[Mapping[str, object]]] = field(
        default_factory=Registry
    )
    debug_operations: Registry[str, Sequence[Mapping[str, object]]] = field(
        default_factory=Registry
    )
    partition_by: Registry[str, Optional[Union[str, List[str]]]] = field(
        default_factory=Registry
    )
    feature_id_by: Registry[str, Optional[Union[str, List[str]]]] = field(
        default_factory=Registry
    )
    ordered_by: Registry[str, Optional[List[str]]] = field(default_factory=Registry)
    sort_batch_size: Registry[str, int] = field(default_factory=Registry)

    def clear_all(self) -> None:
        for reg in (
            self.stream_operations,
            self.debug_operations,
            self.partition_by,
            self.feature_id_by,
            self.ordered_by,
            self.sort_batch_size,
            self.record_operations,
            self.feature_transforms,
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
    registries: Registries = field(default_factory=Registries)
    split: Optional[SplitConfig] = None
    split_keep: Optional[str] = None
    run: Optional[ServeProfile] = None
    schema_required: bool = True
    sample_keys: list[str] = field(default_factory=list)
    window_bounds: tuple[datetime | None, datetime | None] | None = None
    heartbeat_interval_seconds: float | None = None
    execution_observer: object | None = None
    artifacts: ArtifactManager = field(init=False)

    def __post_init__(self) -> None:
        self.artifacts = ArtifactManager(self.artifacts_root)
