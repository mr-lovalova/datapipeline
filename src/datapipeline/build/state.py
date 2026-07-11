import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

BUILD_STATE_VERSION = 3


class ArtifactInfo(BaseModel):
    """Metadata describing a materialized artifact."""

    model_config = ConfigDict(extra="forbid")

    relative_path: str
    config_hash: str
    meta: dict[str, Any] = Field(default_factory=dict)


class BuildState(BaseModel):
    """Minimal persisted state for caching build outputs."""

    model_config = ConfigDict(extra="forbid")

    version: int = BUILD_STATE_VERSION
    artifacts: dict[str, ArtifactInfo] = Field(default_factory=dict)

    def register(
        self,
        key: str,
        relative_path: str,
        config_hash: str,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        self.artifacts[key] = ArtifactInfo(
            relative_path=relative_path,
            config_hash=config_hash,
            meta=dict(meta or {}),
        )


def load_build_state(path: Path) -> BuildState | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if data.get("version") != BUILD_STATE_VERSION:
        return None
    return BuildState.model_validate(data)


def save_build_state(state: BuildState, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(state.model_dump(), fh, indent=2, sort_keys=True)
