from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

from datapipeline.config.tasks import MetadataTask
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent
from datapipeline.utils.window import resolve_window_bounds

from .utils import collect_schema_entries, metadata_entries_from_stats


def materialize_metadata(runtime: Runtime, task_cfg: MetadataTask) -> Tuple[str, Dict[str, object]] | None:
    if not task_cfg.enabled:
        return None
    dataset = load_dataset(runtime.project_yaml, "vectors")
    features_cfgs = list(dataset.features or [])
    feature_stats, feature_vectors, feature_min, feature_max = collect_schema_entries(
        runtime,
        features_cfgs,
        dataset.group_by,
        cadence_strategy=task_cfg.cadence_strategy,
        collect_metadata=True,
    )
    target_meta: list[dict] = []
    target_vectors = 0
    target_cfgs = list(dataset.targets or [])
    target_min = target_max = None
    if target_cfgs:
        target_stats, target_vectors, target_min, target_max = collect_schema_entries(
            runtime,
            target_cfgs,
            dataset.group_by,
            cadence_strategy=task_cfg.cadence_strategy,
            collect_metadata=True,
        )
        target_meta = metadata_entries_from_stats(target_stats, task_cfg.cadence_strategy)
    feature_meta = metadata_entries_from_stats(feature_stats, task_cfg.cadence_strategy)

    doc = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "features": feature_meta,
        "targets": target_meta,
        "counts": {
            "feature_vectors": feature_vectors,
            "target_vectors": target_vectors,
        },
    }
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
        "features": len(feature_meta),
        "targets": len(target_meta),
    }
    return str(relative_path), meta
