from pathlib import Path
import ast

from datapipeline.services.constants import (
    COMBINERS_GROUP,
    LOADERS_GROUP,
    MAPPERS_GROUP,
    PARSERS_GROUP,
)
from datapipeline.services.entrypoints import read_group_entries
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.project import load_project
from datapipeline.services.streams.loader import load_streams


def list_dtos(root: Path | None = None) -> dict[str, str]:
    """Return mapping of DTO class name -> module path."""
    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    dtos_dir = base / "dtos"
    if not dtos_dir.exists():
        return {}

    package_name = base.name
    found: dict[str, str] = {}
    for path in sorted(dtos_dir.glob("*.py")):
        if path.name == "__init__.py":
            continue
        try:
            tree = ast.parse(path.read_text())
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        module = f"{package_name}.dtos.{path.stem}"
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and _is_dataclass(node):
                found[node.name] = module
    return found


def _is_dataclass(node: ast.ClassDef) -> bool:
    for deco in node.decorator_list:
        if isinstance(deco, ast.Name) and deco.id == "dataclass":
            return True
        if isinstance(deco, ast.Attribute) and deco.attr == "dataclass":
            return True
    return False


def list_parsers(root: Path | None = None) -> dict[str, str]:
    _, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, PARSERS_GROUP)


def list_loaders(root: Path | None = None) -> dict[str, str]:
    _, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, LOADERS_GROUP)


def list_mappers(root: Path | None = None) -> dict[str, str]:
    _, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, MAPPERS_GROUP)


def list_combiners(root: Path | None = None) -> dict[str, str]:
    _, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, COMBINERS_GROUP)


def list_domains(root: Path | None = None) -> list[str]:
    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    dom_dir = base / "domains"
    if not dom_dir.exists():
        return []
    return sorted(
        p.name for p in dom_dir.iterdir() if p.is_dir() and (p / "model.py").exists()
    )


def list_sources(project_yaml: Path) -> list[str]:
    streams = load_streams(load_project(project_yaml))
    return sorted(streams.sources)


def list_streams(project_yaml: Path) -> list[str]:
    streams = load_streams(load_project(project_yaml))
    return sorted(streams.streams)
