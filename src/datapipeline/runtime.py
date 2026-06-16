from dataclasses import dataclass, field
import shutil
import tempfile
from pathlib import Path
from typing import Any, List, Literal, Mapping, Optional, Sequence, Union
from datetime import datetime

from datapipeline.cache import RecordStreamCache
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
    sort_batch_size: Registry[str, int] = field(default_factory=Registry)

    def clear_all(self) -> None:
        for reg in (
            self.stream_operations,
            self.debug_operations,
            self.partition_by,
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
    cache_root: Path | None = None
    cache_enabled: bool = True
    registries: Registries = field(default_factory=Registries)
    split: Optional[SplitConfig] = None
    split_keep: Optional[str] = None
    run: Optional[ServeProfile] = None
    schema_required: bool = True
    sample_keys: list[str] = field(default_factory=list)
    window_bounds: tuple[datetime | None, datetime | None] | None = None
    artifacts: ArtifactManager = field(init=False)
    record_stream_cache: RecordStreamCache = field(init=False)
    _owns_cache_root: bool = field(init=False, repr=False, default=False)

    def __post_init__(self) -> None:
        self.artifacts = ArtifactManager(self.artifacts_root)
        self.cache_root = self._resolve_cache_root(self.cache_root)
        self.record_stream_cache = RecordStreamCache(
            project_yaml=self.project_yaml,
            root=self.cache_root,
        )

    def cleanup_cache(self) -> None:
        cache_root = self.cache_root
        if not self._owns_cache_root or cache_root is None:
            return
        shutil.rmtree(cache_root, ignore_errors=True)
        self.cache_root = None
        self._owns_cache_root = False

    def disable_cache(self) -> None:
        self.cache_enabled = False
        self.cleanup_cache()

    def ensure_cache_root(self) -> Path:
        if self.cache_root is not None:
            return self.cache_root
        root = self._new_owned_cache_root()
        self.cache_root = root
        self._owns_cache_root = True
        self.record_stream_cache = RecordStreamCache(
            project_yaml=self.project_yaml,
            root=root,
        )
        return root

    def sort_spill_dir(self) -> Path:
        return self.ensure_cache_root() / "_sort"

    def set_cache_root(self, root: Path, *, owned: bool = False) -> None:
        current = self.cache_root
        if self._owns_cache_root and current is not None and current != root:
            shutil.rmtree(current, ignore_errors=True)
        resolved = Path(root).resolve()
        resolved.mkdir(parents=True, exist_ok=True)
        self.cache_root = resolved
        self._owns_cache_root = bool(owned)
        self.record_stream_cache = RecordStreamCache(
            project_yaml=self.project_yaml,
            root=resolved,
        )

    def _resolve_cache_root(self, configured: Path | None) -> Path:
        if configured is not None:
            root = Path(configured).resolve()
            root.mkdir(parents=True, exist_ok=True)
            self._owns_cache_root = False
            return root

        root = self._new_owned_cache_root()
        self._owns_cache_root = True
        return root

    def _new_owned_cache_root(self) -> Path:
        project_name = self.project_yaml.stem or "project"
        return Path(
            tempfile.mkdtemp(prefix=f"datapipeline-cache-{project_name}-")
        ).resolve()
