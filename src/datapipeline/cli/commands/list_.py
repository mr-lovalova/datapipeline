from pathlib import Path

from datapipeline.config.workspace import WorkspaceContext
from datapipeline.cli.workspace_utils import resolve_default_project_yaml
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.bootstrap.core import load_streams
from datapipeline.services.scaffold.discovery import (
    list_domains,
    list_dtos,
    list_loaders,
    list_mappers,
    list_parsers,
)
from datapipeline.services.scaffold.utils import error_exit


def _default_project_path(root_dir: Path) -> Path | None:
    candidate = root_dir / "config" / "project.yaml"
    if candidate.exists():
        return candidate
    default_proj = root_dir / "config" / "datasets" / "default" / "project.yaml"
    if default_proj.exists():
        return default_proj
    datasets_dir = root_dir / "config" / "datasets"
    if datasets_dir.exists():
        for p in sorted(datasets_dir.rglob("project.yaml")):
            if p.is_file():
                return p
    return None


def handle(subcmd: str, *, workspace: WorkspaceContext | None = None) -> None:
    root_dir, name, pyproject = pkg_root(None)
    if subcmd == "sources":
        # Discover sources by scanning sources_dir for YAML files
        proj_path = resolve_default_project_yaml(workspace) if workspace is not None else None
        if proj_path is None:
            proj_path = _default_project_path(root_dir)
        if proj_path is None:
            error_exit("No project.yaml found under config/.")
        try:
            streams = load_streams(proj_path)
        except FileNotFoundError as exc:
            error_exit(str(exc))
        aliases = sorted(streams.raw.keys())
        for alias in aliases:
            print(alias)
    elif subcmd == "domains":
        for k in list_domains():
            print(k)
    elif subcmd == "parsers":
        for k in sorted(list_parsers().keys()):
            print(k)
    elif subcmd == "mappers":
        for k in sorted(list_mappers().keys()):
            print(k)
    elif subcmd == "loaders":
        for k in sorted(list_loaders().keys()):
            print(k)
    elif subcmd == "dtos":
        for k in sorted(list_dtos().keys()):
            print(k)
