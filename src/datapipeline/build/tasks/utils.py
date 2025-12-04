from __future__ import annotations

from collections import Counter, OrderedDict
from datetime import datetime
from typing import Any

from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.transforms.vector_utils import base_id as _base_feature_id
from datapipeline.transforms.utils import is_missing


def _type_name(value: object) -> str:
    if value is None:
        return "null"
    return type(value).__name__


def collect_schema_entries(
    runtime: Runtime,
    configs,
    group_by: str,
    *,
    cadence_strategy: str,
    collect_metadata: bool,
) -> tuple[list[dict], int, datetime | None, datetime | None]:
    configs = list(configs or [])
    if not configs:
        return [], 0, None, None
    sanitized = [cfg.model_copy(update={"scale": False}) for cfg in configs]
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        sanitized,
        group_by,
        rectangular=False,
    )

    stats: OrderedDict[str, dict] = OrderedDict()
    vector_count = 0
    min_time: datetime | None = None
    max_time: datetime | None = None
    for sample in vectors:
        vector_count += 1
        ts = sample.key[0] if isinstance(sample.key, tuple) and sample.key else None
        if isinstance(ts, datetime):
            min_time = ts if min_time is None else min(min_time, ts)
            max_time = ts if max_time is None else max(max_time, ts)
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

    return list(stats.values()), vector_count, min_time, max_time


def _resolve_cadence_target(stats: dict, strategy: str) -> int | None:
    if strategy == "max":
        max_len = stats.get("max_length")
        if isinstance(max_len, (int, float)) and max_len > 0:
            return int(max_len)
    return None


def schema_entries_from_stats(entries: list[dict], cadence_strategy: str) -> list[dict]:
    doc: list[dict] = []
    for entry in entries:
        kind = entry.get("kind") or "scalar"
        item = {
            "id": entry["id"],
            "base_id": entry["base_id"],
            "kind": kind,
        }
        if kind == "list":
            target = _resolve_cadence_target(entry, cadence_strategy)
            if target is not None:
                item["cadence"] = {"strategy": cadence_strategy, "target": target}
        doc.append(item)
    return doc


def _to_iso(ts: datetime | None) -> str | None:
    if isinstance(ts, datetime):
        text = ts.isoformat()
        if text.endswith("+00:00"):
            return text[:-6] + "Z"
        return text
    return None


def metadata_entries_from_stats(entries: list[dict], cadence_strategy: str) -> list[dict]:
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
            target = _resolve_cadence_target(entry, cadence_strategy)
            if target is not None:
                item["cadence"] = {"strategy": cadence_strategy, "target": target}
            if "observed_elements" in entry:
                item["observed_elements"] = int(entry.get("observed_elements", 0))
        else:
            item["value_types"] = sorted(entry.get("scalar_types", []))
        meta_entries.append(item)
    return meta_entries
