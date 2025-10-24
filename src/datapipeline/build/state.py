import json
from pathlib import Path
from typing import Dict, Optional, Any

from pydantic import BaseModel, Field


class ArtifactInfo(BaseModel):
    """Metadata describing a materialized artifact."""

    relative_path: str
    meta: Dict[str, Any] = Field(default_factory=dict)


class BuildState(BaseModel):
    """Minimal persisted state for caching build outputs."""

    version: int = 1
    config_hash: str
    artifacts: Dict[str, ArtifactInfo] = Field(default_factory=dict)

    def register(self, key: str, relative_path: str, *, meta: Optional[Dict[str, Any]] = None) -> None:
        self.artifacts[key] = ArtifactInfo(
            relative_path=relative_path, meta=dict(meta or {}))


def load_build_state(path: Path) -> Optional[BuildState]:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    artifacts = data.get("artifacts")
    if isinstance(artifacts, dict):
        normalized: Dict[str, ArtifactInfo] = {}
        for key, value in artifacts.items():
            if isinstance(value, dict):
                rel = value.get("relative_path")
                if isinstance(rel, str):
                    meta = value.get("meta") if isinstance(
                        value.get("meta"), dict) else {}
                    normalized[key] = ArtifactInfo(
                        relative_path=rel, meta=meta)
            elif isinstance(value, str):
                normalized[key] = ArtifactInfo(relative_path=value)
        data["artifacts"] = normalized
    return BuildState.model_validate(data)


def save_build_state(state: BuildState, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(state.model_dump(), fh, indent=2, sort_keys=True)
