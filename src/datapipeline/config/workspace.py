from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from datapipeline.utils.load import load_yaml


class SharedDefaults(BaseModel):
    visual_provider: Optional[str] = Field(
        default=None, description="AUTO | TQDM | RICH | OFF"
    )
    progress_style: Optional[str] = Field(
        default=None, description="AUTO | SPINNER | BARS | OFF"
    )
    log_level: Optional[str] = Field(default=None, description="DEFAULT LOG LEVEL")

    @field_validator("visual_provider", "progress_style", "log_level", mode="before")
    @classmethod
    def _normalize(cls, value: object):
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None


class ServeDefaults(BaseModel):
    log_level: Optional[str] = None
    limit: Optional[int] = None
    stage: Optional[int] = None
    throttle_ms: Optional[float] = None

    class OutputDefaults(BaseModel):
        transport: str
        format: str
        path: Optional[str] = None

    output_defaults: Optional[OutputDefaults] = None


class BuildDefaults(BaseModel):
    log_level: Optional[str] = None
    mode: Optional[str] = None


class WorkspaceConfig(BaseModel):
    plugin_root: Optional[str] = None
    config_root: Optional[str] = None
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

    def resolve_config_root(self) -> Optional[Path]:
        raw = self.config.config_root
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
            cfg = WorkspaceConfig.model_validate(data)
            return WorkspaceContext(file_path=candidate, config=cfg)
    return None
