import glob as _glob
import os
from pathlib import Path


def workspace_roots_for(path: Path) -> list[Path]:
    """Return all ancestor directories that contain `jerry.yaml`."""
    roots: list[Path] = []
    resolved = path.resolve()
    for candidate in [resolved, *resolved.parents]:
        if (candidate / "jerry.yaml").is_file():
            roots.append(candidate)
    return roots


def workspace_cwd() -> Path:
    """Return the resolved current working directory used by workspace flows."""
    return Path.cwd().resolve()


def resolve_relative_to_base(
    raw_path: str | Path,
    base: Path,
    resolve: bool = True,
) -> Path:
    """Resolve `raw_path` against `base` when relative."""
    path = Path(raw_path)
    if not path.is_absolute():
        path = base / path
    return path.resolve() if resolve else path


def resolve_workspace_path(
    raw_path: str | Path,
    workspace_root: Path | None,
    cwd: Path | None = None,
) -> Path:
    """Resolve path using workspace-root policy (workspace root, else cwd)."""
    if Path(raw_path).is_absolute():
        return Path(raw_path).resolve()
    base = workspace_root.resolve() if workspace_root is not None else (cwd or workspace_cwd())
    return resolve_relative_to_base(raw_path, base, resolve=True)


def resolve_project_path(
    project_yaml: Path,
    raw_path: str | Path,
    resolve: bool = True,
) -> Path:
    """Resolve project-relative path from the directory containing project.yaml."""
    return resolve_relative_to_base(raw_path, project_yaml.parent, resolve=resolve)


def relative_to_workspace(target: Path, workspace_root: Path) -> Path:
    """Compute a stable relative path from workspace root to target path."""
    target_resolved = target.resolve()
    workspace_resolved = workspace_root.resolve()
    try:
        return target_resolved.relative_to(workspace_resolved)
    except ValueError:
        return Path(os.path.relpath(target_resolved, workspace_resolved))


def path_has_content(path: Path, use_glob: bool) -> bool:
    if use_glob:
        return bool(_glob.glob(str(path)))
    return path.exists()


def resolve_relative_fs_loader_path(
    raw_path: str,
    project_root: Path,
    source_dir: Path,
    use_glob: bool,
) -> str:
    """Resolve a source `loader.args.path` using the shared fs path policy."""
    raw = Path(raw_path)
    if raw.is_absolute():
        return str(raw)

    candidates: list[Path] = [
        resolve_relative_to_base(raw, source_dir, resolve=True),
        resolve_relative_to_base(raw, project_root, resolve=True),
        resolve_relative_to_base(raw, project_root.parent, resolve=True),
    ]
    for workspace_root in workspace_roots_for(project_root):
        candidates.append(resolve_relative_to_base(raw, workspace_root, resolve=True))
    candidates.append(resolve_relative_to_base(raw, workspace_cwd(), resolve=True))

    for candidate in candidates:
        if path_has_content(candidate, use_glob):
            return str(candidate)

    # Deterministic fallback when no candidate currently exists.
    return str(resolve_relative_to_base(raw, project_root, resolve=True))
