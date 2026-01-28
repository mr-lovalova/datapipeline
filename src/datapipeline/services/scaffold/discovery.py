from __future__ import annotations

from pathlib import Path
import ast
from typing import Optional

from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.entrypoints import read_group_entries
from datapipeline.services.constants import PARSERS_GROUP, LOADERS_GROUP, MAPPERS_GROUP
from datapipeline.services.project_paths import sources_dir as resolve_sources_dir, streams_dir as resolve_streams_dir


def list_dtos(*, root: Optional[Path] = None) -> dict[str, str]:
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
        except Exception:
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


def list_parsers(*, root: Optional[Path] = None) -> dict[str, str]:
    root_dir, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, PARSERS_GROUP)


def list_loaders(*, root: Optional[Path] = None) -> dict[str, str]:
    root_dir, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, LOADERS_GROUP)


def list_mappers(*, root: Optional[Path] = None) -> dict[str, str]:
    root_dir, _, pyproject = pkg_root(root)
    if not pyproject.exists():
        return {}
    return read_group_entries(pyproject, MAPPERS_GROUP)


def list_domains(*, root: Optional[Path] = None) -> list[str]:
    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    dom_dir = base / "domains"
    if not dom_dir.exists():
        return []
    return sorted(
        p.name
        for p in dom_dir.iterdir()
        if p.is_dir() and (p / "model.py").exists()
    )


def list_sources(project_yaml: Path) -> list[str]:
    from datapipeline.utils.load import load_yaml
    from datapipeline.services.constants import PARSER_KEY, LOADER_KEY, SOURCE_ID_KEY

    sources_dir = resolve_sources_dir(project_yaml)
    if not sources_dir.exists():
        return []
    out: list[str] = []
    for p in sorted(sources_dir.rglob("*.y*ml")):
        try:
            data = load_yaml(p)
        except Exception:
            continue
        if isinstance(data, dict) and isinstance(data.get(PARSER_KEY), dict) and isinstance(data.get(LOADER_KEY), dict):
            alias = data.get(SOURCE_ID_KEY)
            if isinstance(alias, str):
                out.append(alias)
    return sorted(set(out))


def list_streams(project_yaml: Path) -> list[str]:
    from datapipeline.utils.load import load_yaml
    from datapipeline.services.constants import STREAM_ID_KEY

    streams_dir = resolve_streams_dir(project_yaml)
    if not streams_dir.exists():
        return []
    out: list[str] = []
    for p in sorted(streams_dir.rglob("*.y*ml")):
        try:
            data = load_yaml(p)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("kind") in {"ingest", "composed"}:
            sid = data.get(STREAM_ID_KEY)
            if isinstance(sid, str) and sid:
                out.append(sid)
    return sorted(set(out))
