from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from datapipeline.services.path_policy import resolve_workspace_path, workspace_cwd
from datapipeline.utils.load import load_yaml


class WorkspaceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plugin_root: Optional[str] = None
    datasets: dict[str, str] = Field(
        default_factory=dict,
        description="Named dataset aliases mapping to project.yaml paths (relative to jerry.yaml).",
    )
    default_dataset: Optional[str] = Field(
        default=None,
        description="Optional default dataset alias when --dataset/--project are omitted.",
    )


@dataclass
class WorkspaceContext:
    file_path: Path
    config: WorkspaceConfig

    @property
    def root(self) -> Path:
        return self.file_path.parent

    def resolve_path(self, raw_path: str | Path) -> Path:
        """Resolve absolute/relative paths using workspace root for relatives."""
        return resolve_with_workspace(raw_path, self)

    def resolve_dataset_alias(self, alias: str) -> Optional[Path]:
        """Resolve a dataset alias from jerry.yaml into an absolute project.yaml path."""
        raw = (self.config.datasets or {}).get(alias)
        if not raw:
            return None
        candidate = self.resolve_path(raw)
        if candidate.is_dir():
            candidate = candidate / "project.yaml"
        return candidate.resolve()

    def resolve_plugin_root(self) -> Optional[Path]:
        raw = self.config.plugin_root
        if not raw:
            return None
        return self.resolve_path(raw)


def resolve_with_workspace(
    raw_path: str | Path,
    workspace: WorkspaceContext | None,
) -> Path:
    """Resolve paths against workspace root when present, else current dir."""
    return resolve_workspace_path(
        raw_path,
        workspace.root if workspace is not None else None,
    )


def load_workspace_context(start_dir: Optional[Path] = None) -> Optional[WorkspaceContext]:
    """Search from start_dir upward for jerry.yaml and return parsed config."""
    directory = (start_dir or workspace_cwd()).resolve()
    for path in [directory, *directory.parents]:
        candidate = path / "jerry.yaml"
        if candidate.is_file():
            data = load_yaml(candidate)
            if not isinstance(data, dict):
                raise TypeError("jerry.yaml must define a mapping at the top level")
            cfg = WorkspaceConfig.model_validate(data)
            return WorkspaceContext(file_path=candidate, config=cfg)
    return None
