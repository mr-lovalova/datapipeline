from __future__ import annotations

from collections import Counter, OrderedDict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Dict, Tuple

from datapipeline.config.build import VectorSchemaConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.utils.window import resolve_window_bounds
from datapipeline.runtime import Runtime
from datapipeline.transforms.vector_utils import base_id as _base_feature_id
from datapipeline.utils.paths import ensure_parent


def _type_name(value: object) -> str:
    if value is None:
        return "null"
    return type(value).__name__


def _collect_schema_entries(runtime: Runtime, configs, group_by: str, *, cadence_strategy: str) -> tuple[list[dict], int, datetime | None, datetime | None]:
    configs = list(configs or [])
    if not configs:
        return [], 0, None, None
    sanitized = [cfg.model_copy(update={"scale": False}) for cfg in configs]
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(context, sanitized, group_by)

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
                    "present_count": 0,
                    "null_count": 0,
                    "scalar_types": set(),
                    "element_types": set(),
                    "min_length": None,
                    "max_length": None,
                    "lengths": Counter(),
                }
            entry["present_count"] += 1
            if value is None:
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
                entry["lengths"][length] += 1
                if not value:
                    entry["element_types"].add("empty")
                else:
                    entry["element_types"].update(_type_name(v) for v in value)
            else:
                if entry["kind"] != "list":
                    entry["kind"] = "scalar"
                entry["scalar_types"].add(_type_name(value))

    entries: list[dict] = []
    for fid, entry in stats.items():
        kind = entry["kind"] or "scalar"
        result = {
            "id": fid,
            "base_id": entry["base_id"],
            "kind": kind,
            "present_count": entry["present_count"],
            "null_count": entry["null_count"],
        }
        if kind == "list":
            result["element_types"] = sorted(entry["element_types"])
        else:
            result["value_types"] = sorted(entry["scalar_types"])
        if kind == "list":
            target = _resolve_cadence_target(entry, cadence_strategy)
            if target is not None:
                result["cadence"] = {"strategy": cadence_strategy, "target": target}
        entries.append(result)
    return entries, vector_count, min_time, max_time


def _resolve_cadence_target(stats: dict, strategy: str) -> int | None:
    if strategy == "max":
        max_len = stats.get("max_length")
        if isinstance(max_len, (int, float)) and max_len > 0:
            return int(max_len)
    return None


def materialize_vector_schema(runtime: Runtime, task_cfg: VectorSchemaConfig) -> Tuple[str, Dict[str, object]] | None:
    if not task_cfg.enabled:
        return None
    dataset = load_dataset(runtime.project_yaml, "vectors")
    features_cfgs = list(dataset.features or [])
    feature_entries, feature_vectors, feature_min, feature_max = _collect_schema_entries(
        runtime, features_cfgs, dataset.group_by, cadence_strategy=task_cfg.cadence_strategy
    )
    target_entries: list[dict] = []
    target_vectors = 0
    if task_cfg.include_targets:
        target_cfgs = list(dataset.targets or [])
        target_entries, target_vectors, target_min, target_max = _collect_schema_entries(
            runtime, target_cfgs, dataset.group_by, cadence_strategy=task_cfg.cadence_strategy
        )
    else:
        target_min = target_max = None

    doc = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "features": feature_entries,
        "targets": target_entries,
        "counts": {
            "feature_vectors": feature_vectors,
            "target_vectors": target_vectors,
        },
    }
    # Prefer globals/schema when available; fall back to observed min/max when possible.
    start, end = resolve_window_bounds(runtime, False)
    obs_start_candidates = [t for t in (feature_min, target_min) if t is not None]
    obs_end_candidates = [t for t in (feature_max, target_max) if t is not None]
    obs_start = min(obs_start_candidates) if obs_start_candidates else None
    obs_end = max(obs_end_candidates) if obs_end_candidates else None
    start = start or obs_start
    end = end or obs_end
    if start is not None and end is not None:
        doc["window"] = {"start": start.isoformat(), "end": end.isoformat()}

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)
    with destination.open("w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2)

    meta: Dict[str, object] = {
        "features": len(feature_entries),
        "targets": len(target_entries),
    }
    return str(relative_path), meta
