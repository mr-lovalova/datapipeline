import logging
import time
from collections import Counter, OrderedDict
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Literal

from datapipeline.artifacts.models import VectorSchemaCadence, VectorSchemaEntry
from datapipeline.dag.context import PipelineContext
from datapipeline.execution.observability import emit_operation_progress
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.transforms.vector_utils import base_id as _base_feature_id
from datapipeline.transforms.utils import is_missing

logger = logging.getLogger(__name__)
_COLLECTION_PROGRESS_INTERVAL_SECONDS = 60.0
_COLLECTION_PROGRESS_ITEM_INTERVAL = 10_000


def _type_name(value: object) -> str:
    if value is None:
        return "null"
    return type(value).__name__


def collect_schema_entries(
    runtime: Runtime,
    configs,
    group_by: str,
    *,
    sample_keys: Sequence[str] = (),
    collect_metadata: bool,
    progress_label: str = "schema entries",
) -> tuple[list[dict], int, datetime | None, datetime | None]:
    entries, vector_count, min_time, max_time, _ = _collect_schema_entries(
        runtime,
        configs,
        group_by,
        sample_keys=sample_keys,
        collect_metadata=collect_metadata,
        collect_sample_domain=False,
        progress_label=progress_label,
    )
    return entries, vector_count, min_time, max_time


def collect_schema_entries_and_sample_domain(
    runtime: Runtime,
    configs,
    group_by: str,
    *,
    sample_keys: Sequence[str],
    collect_metadata: bool,
    progress_label: str = "metadata entries",
) -> tuple[
    list[dict],
    int,
    datetime | None,
    datetime | None,
    dict[tuple, tuple[datetime, datetime]],
]:
    return _collect_schema_entries(
        runtime,
        configs,
        group_by,
        sample_keys=sample_keys,
        collect_metadata=collect_metadata,
        collect_sample_domain=True,
        progress_label=progress_label,
    )


def _collect_schema_entries(
    runtime: Runtime,
    configs,
    group_by: str,
    *,
    sample_keys: Sequence[str],
    collect_metadata: bool,
    collect_sample_domain: bool,
    progress_label: str,
) -> tuple[
    list[dict],
    int,
    datetime | None,
    datetime | None,
    dict[tuple, tuple[datetime, datetime]],
]:
    configs = list(configs or [])
    if not configs:
        return [], 0, None, None, {}
    sanitized = [cfg.model_copy(update={"scale": False}) for cfg in configs]
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        sanitized,
        group_by,
        rectangular=False,
        sample_keys=sample_keys,
    )

    stats: OrderedDict[str, dict] = OrderedDict()
    sample_domain: dict[tuple, tuple[datetime, datetime]] = {}
    vector_count = 0
    min_time: datetime | None = None
    max_time: datetime | None = None
    started_at = time.perf_counter()
    next_progress_at = started_at + _COLLECTION_PROGRESS_INTERVAL_SECONDS
    for sample in vectors:
        vector_count += 1
        ts = sample.key[0] if isinstance(sample.key, tuple) and sample.key else None
        if isinstance(ts, datetime):
            min_time = ts if min_time is None else min(min_time, ts)
            max_time = ts if max_time is None else max(max_time, ts)
            if collect_sample_domain and sample_keys:
                _update_sample_domain(sample_domain, sample.key, ts)
        payload = sample.features
        for fid, value in payload.values.items():
            entry = stats.get(fid)
            if not entry:
                entry = stats[fid] = {
                    "id": fid,
                    "base_id": _base_feature_id(fid),
                    "kind": None,
                    "max_length": None,
                    "present_count": 0,
                    "null_count": 0,
                    "scalar_types": set(),
                    "element_types": set(),
                    "min_length": None,
                    "lengths": Counter(),
                    "first_ts": None,
                    "last_ts": None,
                }
            if isinstance(ts, datetime):
                prev_start = entry.get("first_ts")
                entry["first_ts"] = ts if prev_start is None else min(prev_start, ts)
                prev_end = entry.get("last_ts")
                entry["last_ts"] = ts if prev_end is None else max(prev_end, ts)
            if collect_metadata:
                entry["present_count"] += 1
            if is_missing(value):
                if collect_metadata:
                    entry["null_count"] += 1
                continue
            if isinstance(value, list):
                entry["kind"] = "list"
                length = len(value)
                entry["min_length"] = length if entry["min_length"] is None else min(
                    entry["min_length"], length
                )
                entry["max_length"] = length if entry["max_length"] is None else max(
                    entry["max_length"], length
                )
                if collect_metadata:
                    entry["lengths"][length] += 1
                    entry["observed_elements"] = entry.get("observed_elements", 0) + sum(
                        1 for v in value if not is_missing(v)
                    )
                    if not value:
                        entry["element_types"].add("empty")
                    else:
                        entry["element_types"].update(_type_name(v) for v in value)
            else:
                if entry["kind"] != "list":
                    entry["kind"] = "scalar"
                if collect_metadata:
                    entry["scalar_types"].add(_type_name(value))
        if vector_count % _COLLECTION_PROGRESS_ITEM_INTERVAL == 0:
            now = time.perf_counter()
            if now >= next_progress_at:
                _emit_collection_progress(
                    progress_label=progress_label,
                    vector_count=vector_count,
                    discovered_ids=len(stats),
                    elapsed_seconds=now - started_at,
                )
                next_progress_at = now + _COLLECTION_PROGRESS_INTERVAL_SECONDS

    return list(stats.values()), vector_count, min_time, max_time, sample_domain


def _emit_collection_progress(
    *,
    progress_label: str,
    vector_count: int,
    discovered_ids: int,
    elapsed_seconds: float,
) -> None:
    message = (
        f"{progress_label}: scanned vectors={vector_count} "
        f"discovered_ids={discovered_ids} elapsed={elapsed_seconds:.0f}s"
    )
    emitted = emit_operation_progress("collect_schema_entries", message)
    if not emitted:
        logger.info("%s", message)


def _update_sample_domain(
    sample_domain: dict[tuple, tuple[datetime, datetime]],
    group_key: Any,
    ts: datetime,
) -> None:
    if not isinstance(group_key, tuple) or len(group_key) < 2:
        return
    key_values = tuple(group_key[1:])
    current = sample_domain.get(key_values)
    if current is None:
        sample_domain[key_values] = (ts, ts)
        return
    start, end = current
    sample_domain[key_values] = min(start, ts), max(end, ts)


def configured_vectors_are_empty(configs, vector_count: int) -> bool:
    return bool(configs) and vector_count == 0


def _resolve_cadence_target(stats: dict) -> int | None:
    max_len = stats.get("max_length")
    if isinstance(max_len, (int, float)) and max_len > 0:
        return int(max_len)
    return None


def schema_entries_from_stats(
    entries: list[dict],
) -> tuple[VectorSchemaEntry, ...]:
    schema_entries: list[VectorSchemaEntry] = []
    for entry in entries:
        raw_kind = entry["kind"]
        if raw_kind not in {None, "scalar", "list"}:
            raise ValueError(f"Unknown vector schema kind: {raw_kind!r}")
        kind: Literal["scalar", "list"] = (
            "list" if raw_kind == "list" else "scalar"
        )
        cadence: VectorSchemaCadence | None = None
        if kind == "list":
            target = _resolve_cadence_target(entry)
            if target is not None:
                cadence = VectorSchemaCadence(target=target)
        schema_entries.append(
            VectorSchemaEntry(id=entry["id"], kind=kind, cadence=cadence)
        )
    return tuple(schema_entries)


def _to_iso(ts: datetime | None) -> str | None:
    if isinstance(ts, datetime):
        text = ts.isoformat()
        if text.endswith("+00:00"):
            return text[:-6] + "Z"
        return text
    return None


def metadata_entries_from_stats(entries: list[dict]) -> list[dict]:
    meta_entries: list[dict] = []
    for entry in entries:
        kind = entry.get("kind") or "scalar"
        item: dict[str, Any] = {
            "id": entry["id"],
            "base_id": entry["base_id"],
            "kind": kind,
            "present_count": entry.get("present_count", 0),
            "null_count": entry.get("null_count", 0),
        }
        first_ts = _to_iso(entry.get("first_ts"))
        last_ts = _to_iso(entry.get("last_ts"))
        if first_ts:
            item["first_observed"] = first_ts
        if last_ts:
            item["last_observed"] = last_ts
        if kind == "list":
            item["element_types"] = sorted(entry.get("element_types", []))
            lengths = entry.get("lengths") or {}
            item["lengths"] = {str(length): count for length, count in sorted(lengths.items())}
            target = _resolve_cadence_target(entry)
            if target is not None:
                item["cadence"] = {"target": target}
            if "observed_elements" in entry:
                item["observed_elements"] = int(entry.get("observed_elements", 0))
        else:
            item["value_types"] = sorted(entry.get("scalar_types", []))
        meta_entries.append(item)
    return meta_entries
