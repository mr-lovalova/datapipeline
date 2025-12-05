from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from datapipeline.config.tasks import (
    VALID_PROGRESS_STYLES,
    VALID_VISUAL_PROVIDERS,
)
from datapipeline.utils.load import load_yaml


class SharedDefaults(BaseModel):
    visuals: Optional[str] = Field(
        default=None, description="AUTO | TQDM | RICH | OFF"
    )
    progress: Optional[str] = Field(
        default=None, description="AUTO | SPINNER | BARS | OFF"
    )
    log_level: Optional[str] = Field(default=None, description="DEFAULT LOG LEVEL")

    @field_validator("visuals", "progress", "log_level", mode="before")
    @classmethod
    def _normalize(cls, value: object):
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text if text else None
        return value

    @field_validator("visuals", mode="before")
    @classmethod
    def _normalize_visuals(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).upper()
        if name not in VALID_VISUAL_PROVIDERS:
            raise ValueError(
                f"visuals must be one of {', '.join(VALID_VISUAL_PROVIDERS)}, got {value!r}"
            )
        return name

    @field_validator("progress", mode="before")
    @classmethod
    def _normalize_progress(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).upper()
        if name not in VALID_PROGRESS_STYLES:
            raise ValueError(
                f"progress must be one of {', '.join(VALID_PROGRESS_STYLES)}, got {value!r}"
            )
        return name


class ServeDefaults(BaseModel):
    log_level: Optional[str] = None
    limit: Optional[int] = None
    stage: Optional[int] = None
    throttle_ms: Optional[float] = None

    class OutputDefaults(BaseModel):
        transport: str
        format: str
        payload: str = Field(default="sample")
        directory: Optional[str] = Field(
            default=None,
            description="Base directory for fs outputs (relative paths are resolved from jerry.yaml).",
        )

    output: Optional[OutputDefaults] = None


class BuildDefaults(BaseModel):
    log_level: Optional[str] = None
    mode: Optional[str] = None

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: object):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        text = str(value).strip()
        if not text:
            return None
        name = text.upper()
        valid_modes = {"AUTO", "FORCE", "OFF"}
        if name not in valid_modes:
            options = ", ".join(sorted(valid_modes))
            raise ValueError(f"build.mode must be one of {options}, got {value!r}")
        return name


class WorkspaceConfig(BaseModel):
    plugin_root: Optional[str] = None
    datasets: dict[str, str] = Field(
        default_factory=dict,
        description="Named dataset aliases mapping to project.yaml paths (relative to jerry.yaml).",
    )
    default_dataset: Optional[str] = Field(
        default=None,
        description="Optional default dataset alias when --dataset/--project are omitted.",
    )
    shared: SharedDefaults = Field(default_factory=SharedDefaults)
    serve: ServeDefaults = Field(default_factory=ServeDefaults)
    build: BuildDefaults = Field(default_factory=BuildDefaults)


@dataclass
class WorkspaceContext:
    file_path: Path
    config: WorkspaceConfig

    @property
    def root(self) -> Path:
        return self.file_path.parent

    def resolve_plugin_root(self) -> Optional[Path]:
        raw = self.config.plugin_root
        if not raw:
            return None
        candidate = Path(raw)
        return (
            candidate.resolve()
            if candidate.is_absolute()
            else (self.root / candidate).resolve()
        )


def load_workspace_context(start_dir: Optional[Path] = None) -> Optional[WorkspaceContext]:
    """Search from start_dir upward for jerry.yaml and return parsed config."""
    directory = (start_dir or Path.cwd()).resolve()
    for path in [directory, *directory.parents]:
        candidate = path / "jerry.yaml"
        if candidate.is_file():
            data = load_yaml(candidate)
            if not isinstance(data, dict):
                raise TypeError("jerry.yaml must define a mapping at the top level")
            # Allow users to set serve/build/shared to null to fall back to defaults
            for key in ("shared", "serve", "build"):
                if key in data and data[key] is None:
                    data.pop(key)
            cfg = WorkspaceConfig.model_validate(data)
            return WorkspaceContext(file_path=candidate, config=cfg)
    return None
