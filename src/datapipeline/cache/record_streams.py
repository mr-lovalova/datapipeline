from pathlib import Path
from typing import Any, Iterator

from datapipeline.cache.models import (
    MaterializationRef,
    MaterializationStore,
    SourceObserver,
)
from datapipeline.cache.pickle_store import PickleMaterializationStore
from datapipeline.cache.signatures import resolve_record_stream_cache_ref

STREAM_CACHE_BOUNDARY_NODE = "stream_transforms"


class RecordStreamCache:
    def __init__(
        self,
        *,
        project_yaml: Path,
        root: Path,
        store: MaterializationStore | None = None,
        source_observer: SourceObserver | None = None,
    ) -> None:
        self._project_yaml = Path(project_yaml).resolve()
        self._store = store or PickleMaterializationStore(root)
        self._signature_cache: dict[str, tuple[str, str, str] | None] = {}
        self._source_observer = source_observer or _default_source_observer

    def load(self, stream_id: str) -> Iterator[Any] | None:
        ref = self._materialization_ref(stream_id)
        if ref is None:
            return None
        stream_source = self._store.load(ref)
        if stream_source is None:
            return None
        return self._source_observer(stream_source, stream_id).stream()

    def materialize(self, stream_id: str, items: Iterator[Any]) -> Iterator[Any]:
        ref = self._materialization_ref(stream_id)
        if ref is None:
            yield from items
            return
        yield from self._store.materialize(ref, items)

    def _materialization_ref(self, stream_id: str) -> MaterializationRef | None:
        cache_ref = self._cache_ref(stream_id)
        if cache_ref is None:
            return None
        kind, stage, signature = cache_ref
        return MaterializationRef(
            kind=kind,
            name=stream_id,
            stage=stage,
            signature_hash=signature,
        )

    def _cache_ref(self, stream_id: str) -> tuple[str, str, str] | None:
        if stream_id in self._signature_cache:
            return self._signature_cache[stream_id]
        cache_ref = resolve_record_stream_cache_ref(self._project_yaml, stream_id)
        self._signature_cache[stream_id] = cache_ref
        return cache_ref

    def _signature(self, stream_id: str) -> str | None:
        cache_ref = self._cache_ref(stream_id)
        if cache_ref is None:
            return None
        return cache_ref[2]


def cached_record_stream(context, record_stream_id: str) -> Iterator[Any]:
    from datapipeline.pipelines.ingest import build_ingest_pipeline
    from datapipeline.pipelines.shared.record_nodes import apply_debug_operations
    from datapipeline.pipelines.stream import build_stream_dag, build_stream_pipeline

    runtime = context.runtime
    stream_spec = runtime.registries.stream_specs.get(record_stream_id)
    is_ingest = stream_spec.pipeline == "ingest"
    if not getattr(runtime, "cache_enabled", True):
        if is_ingest:
            return build_ingest_pipeline(context, record_stream_id)
        return build_stream_pipeline(context, record_stream_id)

    partition_by = runtime.registries.partition_by.get(record_stream_id)
    debug_operations = runtime.registries.debug_operations.get(record_stream_id)

    stream = runtime.record_stream_cache.load(record_stream_id)
    if stream is None:
        if is_ingest:
            base_stream = build_ingest_pipeline(context, record_stream_id)
        else:
            from datapipeline.dag.runner import run_dag

            stream_dag = build_stream_dag(context, record_stream_id)
            base_stream = run_dag(
                context,
                stream_dag.upto_node_named(STREAM_CACHE_BOUNDARY_NODE),
            )
        stream = runtime.record_stream_cache.materialize(record_stream_id, base_stream)

    if is_ingest:
        return stream
    return apply_debug_operations(
        context,
        debug_operations,
        partition_by,
        stream,
    )


def _default_source_observer(stream_source, stream_id: str):
    try:
        from datapipeline.cli.visuals.streams import observe_source
    except Exception:
        return stream_source
    return observe_source(stream_source, stream_id)
