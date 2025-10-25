from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from datapipeline.config.project import ProjectConfig
from datapipeline.utils.load import load_yaml


class RunConfig(BaseModel):
    """Runtime overrides applied when serving vectors."""

    version: int = Field(default=1)
    keep: str | None = Field(
        default=None,
        description="Active split label to serve. Null disables filtering.",
        min_length=1,
    )
    output: str | None = Field(
        default=None,
        description="Default output destination for jerry run serve (print|stream|<path>).",
        min_length=1,
    )
    limit: int | None = Field(
        default=None,
        description="Default max number of vectors to emit during serve runs.",
        ge=1,
    )
    include_targets: bool = Field(
        default=False,
        description="Serve dataset.targets alongside features by default.",
    )
    throttle_ms: float | None = Field(
        default=None,
        description="Milliseconds to sleep between emitted vectors (throttle).",
        ge=0.0,
    )


def load_run_config(project_yaml: Path) -> RunConfig | None:
    """Load a run.yaml referenced by project.paths.run, if configured."""

    project_data = load_yaml(project_yaml)
    project = ProjectConfig.model_validate(project_data)
    run_path = getattr(project.paths, "run", None)
    if not run_path:
        return None
    path = Path(run_path)
    if not path.is_absolute():
        path = project_yaml.parent / path
    if not path.exists():
        raise FileNotFoundError(f"run config not found: {path}")
    doc = load_yaml(path)
    if not isinstance(doc, dict):
        raise TypeError("run.yaml must define a mapping at the top level.")
    return RunConfig.model_validate(doc)
