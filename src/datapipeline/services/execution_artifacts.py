import json
from pathlib import Path
from typing import Any, Mapping

from datapipeline.services.executions import ExecutionPaths
from datapipeline.services.path_policy import sanitize_path_segment


def profile_artifact_path(
    execution: ExecutionPaths,
    profile_kind: str,
    profile_name: str,
) -> Path:
    kind = sanitize_path_segment(profile_kind or "profile")
    name = sanitize_path_segment(profile_name or "run")
    return execution.meta_dir / f"{kind}.{name}.profile.json"


def write_profile_artifact(
    execution: ExecutionPaths,
    profile_kind: str,
    profile_name: str,
    payload: Mapping[str, Any],
) -> Path:
    path = profile_artifact_path(execution, profile_kind, profile_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, indent=2, sort_keys=True, default=str)
    return path


__all__ = ["profile_artifact_path", "write_profile_artifact"]
