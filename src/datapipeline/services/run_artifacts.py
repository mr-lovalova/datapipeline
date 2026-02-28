
import json
from pathlib import Path
from typing import Any, Mapping

from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.services.runs import RunPaths


def serve_profile_path(paths: RunPaths, task_label: str) -> Path:
    """Return the canonical per-task serve profile artifact path."""
    label = sanitize_path_segment(task_label or "run")
    return paths.run_root / "meta" / f"serve.{label}.profile.json"


def write_serve_profile(
    
    paths: RunPaths,
    task_label: str,
    payload: Mapping[str, Any],
) -> Path:
    """Persist a structured serve run-profile artifact and return its path."""
    path = serve_profile_path(paths, task_label)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, indent=2, sort_keys=True, default=str)
    return path

