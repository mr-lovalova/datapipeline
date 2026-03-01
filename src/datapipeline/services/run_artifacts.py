
import json
from pathlib import Path
from typing import Any, Mapping

from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.services.runs import RunPaths


def profile_artifact_path(
    paths: RunPaths,
    profile_kind: str,
    profile_name: str,
) -> Path:
    """Return the canonical per-profile artifact path for a run."""
    kind = sanitize_path_segment(profile_kind or "profile")
    name = sanitize_path_segment(profile_name or "run")
    return paths.run_root / "meta" / f"{kind}.{name}.profile.json"


def write_profile_artifact(
    paths: RunPaths,
    profile_kind: str,
    profile_name: str,
    payload: Mapping[str, Any],
) -> Path:
    """Persist a structured profile artifact and return its path."""
    path = profile_artifact_path(paths, profile_kind, profile_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, indent=2, sort_keys=True, default=str)
    return path
