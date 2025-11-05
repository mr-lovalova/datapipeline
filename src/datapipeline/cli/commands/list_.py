from pathlib import Path

from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.bootstrap.core import load_streams


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


def handle(subcmd: str) -> None:
    root_dir, name, pyproject = pkg_root(None)
    if subcmd == "sources":
        # Discover sources by scanning sources_dir for YAML files
        proj_path = _default_project_path(root_dir)
        if proj_path is None:
            print("[error] No project.yaml found under config/.")
            return
        try:
            streams = load_streams(proj_path)
        except FileNotFoundError as exc:
            print(f"[error] {exc}")
            return
        aliases = sorted(streams.raw.keys())
        for alias in aliases:
            print(alias)
    elif subcmd == "domains":
        base = resolve_base_pkg_dir(root_dir, name)
        dom_dir = base / "domains"
        if dom_dir.exists():
            names = sorted(p.name for p in dom_dir.iterdir()
                           if p.is_dir() and (p / "model.py").exists())
            for k in names:
                print(k)
