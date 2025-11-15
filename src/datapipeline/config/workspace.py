from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

from datapipeline.config.run import (
    VALID_PROGRESS_STYLES,
    VALID_VISUAL_PROVIDERS,
)
from datapipeline.utils.load import load_yaml


def _apply_visual_block(shared: dict[str, Any], block: dict[str, Any]) -> None:
    """Populate shared visual defaults from a legacy/nested mapping."""
    if not isinstance(block, dict):
        return
    for source, target in (("provider", "visual_provider"), ("progress_style", "progress_style")):
        if source in block:
            shared[target] = block[source]


def _normalize_shared_aliases(data: dict[str, Any]) -> None:
    """Accept legacy visuals.* aliases when populating shared defaults."""
    shared = data.get("shared")
    shared_map = shared if isinstance(shared, dict) else None

    def ensure_shared() -> dict[str, Any]:
        nonlocal shared_map
        if shared_map is None:
            shared_map = {}
            data["shared"] = shared_map
        return shared_map

    # Support top-level "visuals" blocks by promoting them into shared defaults.
    top_visuals = data.pop("visuals", None)
    if isinstance(top_visuals, dict):
        shared_map = ensure_shared()
        _apply_visual_block(shared_map, top_visuals)

    if not isinstance(shared_map, dict):
        shared_map = data.get("shared")
        if not isinstance(shared_map, dict):
            return

    # Accept nested shared.visuals blocks such as shared.visuals.provider.
    nested_visuals = shared_map.pop("visuals", None)
    if isinstance(nested_visuals, dict):
        _apply_visual_block(shared_map, nested_visuals)

    # Accept shared.visuals_provider and shared.visuals_progress_style aliases.
    alias_pairs = (
        ("visuals_provider", "visual_provider"),
        ("visuals_progress_style", "progress_style"),
    )
    for alias, target in alias_pairs:
        if alias in shared_map:
            shared_map[target] = shared_map[alias]
            del shared_map[alias]


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
        if isinstance(value, str):
            text = value.strip()
            return text if text else None
        return value

    @field_validator("visual_provider", mode="before")
    @classmethod
    def _normalize_visual_provider(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).upper()
        if name not in VALID_VISUAL_PROVIDERS:
            raise ValueError(
                f"visual_provider must be one of {', '.join(VALID_VISUAL_PROVIDERS)}, got {value!r}"
            )
        return name

    @field_validator("progress_style", mode="before")
    @classmethod
    def _normalize_progress_style(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).upper()
        if name not in VALID_PROGRESS_STYLES:
            raise ValueError(
                f"progress_style must be one of {', '.join(VALID_PROGRESS_STYLES)}, got {value!r}"
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
            # Allow users to set serve/build/shared to null to fall back to defaults
            for key in ("shared", "serve", "build"):
                if key in data and data[key] is None:
                    data.pop(key)
            _normalize_shared_aliases(data)
            cfg = WorkspaceConfig.model_validate(data)
            return WorkspaceContext(file_path=candidate, config=cfg)
    return None
