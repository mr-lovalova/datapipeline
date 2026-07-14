import os
from pathlib import Path


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


def resolve_artifact_output_path(
    raw_path: str | Path,
    artifacts_root: Path,
) -> Path:
    """Return a declared artifact output that cannot escape through a symlink."""
    root = Path(artifacts_root).resolve()
    output = resolve_relative_to_base(raw_path, root, resolve=False)
    resolved_output = output.resolve()
    try:
        resolved_output.relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"Artifact output '{raw_path}' must stay under artifacts root '{root}'."
        ) from exc
    if resolved_output != output:
        raise ValueError(
            f"Artifact output '{raw_path}' must not resolve through a symlink."
        )
    return output


def resolve_workspace_path(
    raw_path: str | Path,
    workspace_root: Path | None,
    cwd: Path | None = None,
) -> Path:
    """Resolve path using workspace-root policy (workspace root, else cwd)."""
    if Path(raw_path).is_absolute():
        return Path(raw_path).resolve()
    base = (
        workspace_root.resolve()
        if workspace_root is not None
        else (cwd or workspace_cwd())
    )
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


def sanitize_path_segment(value: str, default: str = "run") -> str:
    """Return a filesystem-safe path segment for user-provided labels."""
    cleaned = "".join(
        ch if ch.isalnum() or ch in ("_", "-", ".") else "_"
        for ch in str(value).strip()
    )
    return cleaned or default


def resolve_relative_fs_loader_path(
    raw_path: str,
    project_root: Path,
) -> str:
    """Resolve a source `loader.args.path` using the shared fs path policy."""
    raw = Path(raw_path)
    if raw.is_absolute():
        return str(raw)
    return str(resolve_relative_to_base(raw, project_root, resolve=True))
