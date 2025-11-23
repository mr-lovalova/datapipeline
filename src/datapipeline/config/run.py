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
PayloadMode = Literal["sample", "vector"]


class OutputConfig(BaseModel):
    transport: Transport = Field(..., description="fs | stdout")
    format: Format = Field(..., description="csv | json | json-lines | print | pickle")
    payload: PayloadMode = Field(
        default="sample",
        description="sample (key + metadata) or vector payload (features [+targets]).",
    )
    directory: Optional[Path] = Field(
        default=None,
        description="Directory where run outputs should land (fs transport only).",
    )
    filename: Optional[str] = Field(
        default=None,
        description="Filename stem (extension derived from format) for fs outputs.",
    )

    @field_validator("filename", mode="before")
    @classmethod
    def _normalize_filename(cls, value):
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if any(sep in text for sep in ("/", "\\")):
            raise ValueError("filename must not contain path separators")
        if "." in Path(text).name:
            raise ValueError("filename should not include an extension; format determines the suffix")
        return text

    @model_validator(mode="after")
    def _validate(self):
        if self.transport == "stdout":
            if self.directory is not None:
                raise ValueError("stdout cannot define a directory")
            if self.filename is not None:
                raise ValueError("stdout outputs do not support filenames")
            if self.format not in {"print", "json-lines", "json"}:
                raise ValueError(
                    "stdout output supports 'print', 'json-lines', or 'json' formats"
                )
            return self

        if self.format == "print":
            raise ValueError("fs transport cannot use 'print' format")
        if self.directory is None:
            raise ValueError("fs outputs require a directory")
        return self

    @field_validator("payload", mode="before")
    @classmethod
    def _normalize_payload(cls, value):
        if value is None:
            return "sample"
        name = str(value).lower()
        if name not in {"sample", "vector"}:
            raise ValueError("payload must be 'sample' or 'vector'")
        return name


class RunConfig(BaseModel):
    """Runtime overrides applied when serving vectors."""

    version: int = Field(default=1)
    name: str | None = Field(
        default=None,
        description="Unique run name (defaults to filename stem when omitted).",
    )
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
    visuals: str | None = Field(
        default="AUTO",
        description="Visuals provider: AUTO (prefer rich if available), TQDM, RICH, or OFF.",
    )
    progress: str | None = Field(
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

    @field_validator("visuals", mode="before")
    @classmethod
    def _validate_visuals_run(cls, value):
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
    def _validate_progress_run(cls, value):
        if value is None:
            return None
        name = str(value).upper()
        if name not in VALID_PROGRESS_STYLES:
            raise ValueError(
                f"progress must be one of {', '.join(VALID_PROGRESS_STYLES)}, got {value!r}"
            )
        return name

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            raise ValueError("run name cannot be empty")
        return text


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


def load_named_run_configs(project_yaml: Path) -> List[Tuple[str, RunConfig, Path]]:
    """Return (name, config, path) triples for every run file (directory-aware)."""

    paths = _list_run_paths(project_yaml)
    entries: List[Tuple[str, RunConfig, Path]] = []
    for path in paths:
        cfg = _load_run_from_path(path)
        effective_name = cfg.name or path.stem
        if cfg.name is None:
            cfg.name = effective_name
        entries.append((effective_name, cfg, path))
    return entries


def load_run_config(project_yaml: Path) -> RunConfig | None:
    """Load the first run config referenced by project.paths.run, if configured."""

    paths = _list_run_paths(project_yaml)
    if not paths:
        return None
    cfg = _load_run_from_path(paths[0])
    if cfg.name is None:
        cfg.name = paths[0].stem
    return cfg
