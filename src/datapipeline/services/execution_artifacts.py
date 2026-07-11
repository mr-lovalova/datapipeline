import json
from pathlib import Path
from typing import Any, Mapping

from datapipeline.services.path_policy import sanitize_path_segment


def write_profile_artifact(
    execution_dir: Path,
    profile_kind: str,
    profile_name: str,
    payload: Mapping[str, Any],
) -> Path:
    kind = sanitize_path_segment(profile_kind)
    name = sanitize_path_segment(profile_name)
    path = execution_dir / "meta" / f"{kind}.{name}.profile.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, indent=2, sort_keys=True, default=str)
    return path
