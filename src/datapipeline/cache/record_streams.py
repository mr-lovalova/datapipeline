from pathlib import Path
from typing import Any, Iterator

from datapipeline.cache.contracts import (
    MaterializationRef,
    MaterializationStore,
    SourceObserver,
)
from datapipeline.cache.pickle_store import PickleMaterializationStore
from datapipeline.cache.signatures import resolve_record_stream_signature

_RECORD_STREAM_STAGE = "stream_transforms"


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
        self._signature_cache: dict[str, str | None] = {}
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
        signature = self._signature(stream_id)
        if signature is None:
            return None
        return MaterializationRef(
            kind="record_stream",
            name=stream_id,
            stage=_RECORD_STREAM_STAGE,
            signature_hash=signature,
        )

    def _signature(self, stream_id: str) -> str | None:
        if stream_id in self._signature_cache:
            return self._signature_cache[stream_id]
        signature = resolve_record_stream_signature(self._project_yaml, stream_id)
        self._signature_cache[stream_id] = signature
        return signature


def cached_record_stream(context, record_stream_id: str) -> Iterator[Any]:
    from datapipeline.pipelines.record.dag import build_record_pipeline
    from datapipeline.pipelines.record.nodes import (
        RECORD_NODE_COUNT,
        apply_debug_operations,
    )

    runtime = context.runtime
    if not getattr(runtime, "cache_enabled", True):
        return build_record_pipeline(
            context,
            record_stream_id,
            step=RECORD_NODE_COUNT - 1,
        )

    partition_by = runtime.registries.partition_by.get(record_stream_id)
    debug_operations = runtime.registries.debug_operations.get(record_stream_id)

    stream = runtime.record_stream_cache.load(record_stream_id)
    if stream is None:
        base_stream = build_record_pipeline(
            context,
            record_stream_id,
            step=RECORD_NODE_COUNT - 2,
        )
        stream = runtime.record_stream_cache.materialize(record_stream_id, base_stream)

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
