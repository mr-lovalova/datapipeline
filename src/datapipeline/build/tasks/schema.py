from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

from datapipeline.config.tasks import SchemaTask
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent
from datapipeline.utils.window import resolve_window_bounds

from .utils import collect_schema_entries, schema_entries_from_stats


def materialize_vector_schema(runtime: Runtime, task_cfg: SchemaTask) -> Tuple[str, Dict[str, object]] | None:
    if not task_cfg.enabled:
        return None
    dataset = load_dataset(runtime.project_yaml, "vectors")
    features_cfgs = list(dataset.features or [])
    feature_stats, feature_vectors, feature_min, feature_max = collect_schema_entries(
        runtime,
        features_cfgs,
        dataset.group_by,
        cadence_strategy=task_cfg.cadence_strategy,
        collect_metadata=False,
    )
    target_entries: list[dict] = []
    target_cfgs = list(dataset.targets or [])
    target_min = target_max = None
    if target_cfgs:
        target_stats, _, target_min, target_max = collect_schema_entries(
            runtime,
            target_cfgs,
            dataset.group_by,
            cadence_strategy=task_cfg.cadence_strategy,
            collect_metadata=False,
        )
        target_entries = schema_entries_from_stats(target_stats, task_cfg.cadence_strategy)
    feature_entries = schema_entries_from_stats(feature_stats, task_cfg.cadence_strategy)

    doc = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    doc["features"] = feature_entries
    doc["targets"] = target_entries

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
