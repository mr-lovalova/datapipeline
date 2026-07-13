import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

BUILD_STATE_VERSION = 5


class ArtifactFileFingerprint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    relative_path: str
    size: int = Field(strict=True, ge=0)
    mtime_ns: int = Field(strict=True, ge=0)

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        path = Path(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise ValueError("artifact file path must be relative")
        return str(path)

    @classmethod
    def from_path(cls, relative_path: str, path: Path) -> Self:
        stat = path.stat()
        return cls(
            relative_path=relative_path,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
        )


class ArtifactInfo(BaseModel):
    """Metadata describing a materialized artifact."""

    model_config = ConfigDict(extra="forbid")

    relative_path: str
    config_hash: str
    files: tuple[ArtifactFileFingerprint, ...]
    meta: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_files(self) -> Self:
        if not self.files or self.files[0].relative_path != self.relative_path:
            raise ValueError("artifact files must start with the primary artifact")
        paths = [file.relative_path for file in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("artifact file paths must be unique")
        return self


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
        files: tuple[ArtifactFileFingerprint, ...],
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        self.artifacts[key] = ArtifactInfo(
            relative_path=relative_path,
            config_hash=config_hash,
            files=files,
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
