import sys
from pathlib import Path
from typing import Optional

from datapipeline.services.path_policy import workspace_cwd


def pkg_root(start: Optional[Path] = None) -> tuple[Path, str, Path]:
    here = start or workspace_cwd()
    for d in [here, *here.parents]:
        pyproject = d / "pyproject.toml"
        if pyproject.exists():
            pkg_name = d.name
            src_dir = d / "src"
            if src_dir.exists():
                candidates = [
                    p for p in src_dir.iterdir()
                    if p.is_dir() and (p / "__init__.py").exists()
                ]
                if len(candidates) == 1:
                    pkg_name = candidates[0].name
            return d, pkg_name, pyproject
    print("[error] pyproject.toml not found (searched current and parent dirs)", file=sys.stderr)
    raise SystemExit(1)


def resolve_base_pkg_dir(root_dir: Path, pkg_name: str) -> Path:
    preferred = root_dir / "src" / pkg_name
    if preferred.exists():
        return preferred
    src_dir = root_dir / "src"
    if src_dir.exists():
        candidates = [p for p in src_dir.iterdir() if p.is_dir()
                      and (p / "__init__.py").exists()]
        if len(candidates) == 1:
            return candidates[0]
    preferred.mkdir(parents=True, exist_ok=True)
    (preferred / "__init__.py").touch(exist_ok=True)
    return preferred
