from __future__ import annotations

from pathlib import Path
from typing import List, Sequence, Tuple, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from datapipeline.config.project import ProjectConfig
from datapipeline.utils.load import load_yaml

VALID_LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
VALID_VISUAL_PROVIDERS = ("AUTO", "TQDM", "RICH", "OFF")
VALID_PROGRESS_STYLES = ("AUTO", "SPINNER", "BARS", "OFF")

Transport = Literal["fs", "stdout"]
Format = Literal["csv", "json", "json-lines", "print", "pickle"]


class OutputConfig(BaseModel):
    transport: Transport = Field(..., description="fs | stdout")
    format: Format = Field(..., description="csv | json | json-lines | print")
    path: Optional[Path] = Field(
        default=None,
        description="Required for fs. Full file path including extension."
    )

    @model_validator(mode="after")
    def _validate(self):
        if self.transport == "stdout":
            if self.path is not None:
                raise ValueError("stdout cannot have path")
            if self.format not in {"print", "json-lines", "json"}:
                raise ValueError(
                    "stdout output supports 'print', 'json-lines', or 'json' formats"
                )
        else:  # fs
            if self.path is None:
                raise ValueError("fs requires path")
            if self.format in {"print"}:
                raise ValueError("fs transport cannot use 'print' format")
        return self


class RunConfig(BaseModel):
    """Runtime overrides applied when serving vectors."""

    version: int = Field(default=1)
    keep: str | None = Field(
        default=None,
        description="Active split label to serve. Null disables filtering.",
        min_length=1,
    )
    output: OutputConfig | None = None
    limit: int | None = Field(
        default=None,
        description="Default max number of vectors to emit during serve runs.",
        ge=1,
    )
    include_targets: bool = Field(
        default=False,
        description="Serve dataset.targets alongside features by default.",
    )
    stage: int | None = Field(
        default=None,
        description="Default pipeline stage preview for serve runs (0-7).",
        ge=0,
        le=7,
    )
    throttle_ms: float | None = Field(
        default=None,
        description="Milliseconds to sleep between emitted vectors (throttle).",
        ge=0.0,
    )
    log_level: str | None = Field(
        default="INFO",
        description="Default logging level for serve runs (DEBUG, INFO, WARNING, ERROR, CRITICAL). Use null to inherit CLI.",
    )
    visual_provider: str | None = Field(
        default="AUTO",
        description="Visuals provider: AUTO (prefer rich if available), TQDM, RICH, or OFF.",
    )
    progress_style: str | None = Field(
        default="AUTO",
        description="Progress style: AUTO (spinner unless DEBUG), SPINNER, BARS, or OFF.",
    )

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str | None) -> str | None:
        if value is None:
            return None
        name = str(value).upper()
        if name not in VALID_LOG_LEVELS:
            raise ValueError(
                f"log_level must be one of {', '.join(VALID_LOG_LEVELS)}, got {value!r}"
            )
        return name

    @field_validator("visual_provider", mode="before")
    @classmethod
    def _validate_visual_provider_run(cls, value):
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
    def _validate_progress_style_run(cls, value):
        if value is None:
            return None
        name = str(value).upper()
        if name not in VALID_PROGRESS_STYLES:
            raise ValueError(
                f"progress_style must be one of {', '.join(VALID_PROGRESS_STYLES)}, got {value!r}"
            )
        return name


def _resolve_run_path(project_yaml: Path, run_path: str | Path) -> Path:
    path = Path(run_path)
    if not path.is_absolute():
        path = project_yaml.parent / path
    return path.resolve()


def _list_run_paths(project_yaml: Path) -> Sequence[Path]:
    project_data = load_yaml(project_yaml)
    project = ProjectConfig.model_validate(project_data)
    run_path_ref = getattr(project.paths, "run", None)
    if not run_path_ref:
        return []
    run_path = _resolve_run_path(project_yaml, run_path_ref)
    if not run_path.exists():
        raise FileNotFoundError(f"run config not found: {run_path}")
    if run_path.is_dir():
        entries = sorted(
            [
                p
                for p in run_path.iterdir()
                if p.is_file() and p.suffix in {".yaml", ".yml"}
            ],
            key=lambda p: p.name,
        )
        if not entries:
            raise FileNotFoundError(f"no run configs found under {run_path}")
        return entries
    return [run_path]


def _load_run_from_path(path: Path) -> RunConfig:
    doc = load_yaml(path)
    if not isinstance(doc, dict):
        raise TypeError(f"{path} must define a mapping at the top level.")
    return RunConfig.model_validate(doc)


def load_named_run_configs(project_yaml: Path) -> List[Tuple[str, RunConfig]]:
    """Return (name, config) pairs for every run file (directory-aware)."""

    paths = _list_run_paths(project_yaml)
    entries: List[Tuple[str, RunConfig]] = []
    for path in paths:
        cfg = _load_run_from_path(path)
        entries.append((path.stem, cfg))
    return entries


def load_run_config(project_yaml: Path) -> RunConfig | None:
    """Load the first run config referenced by project.paths.run, if configured."""

    paths = _list_run_paths(project_yaml)
    if not paths:
        return None
    return _load_run_from_path(paths[0])
