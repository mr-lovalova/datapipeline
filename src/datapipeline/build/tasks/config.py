from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable

from datapipeline.services.project_paths import read_project


def _resolve_relative(project_yaml: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (project_yaml.parent / path)


def _normalized_label(path: Path, base_dir: Path) -> str:
    try:
        return str(path.resolve().relative_to(base_dir))
    except ValueError:
        return str(path.resolve())


def _hash_file(hasher, path: Path, base_dir: Path) -> None:
    hasher.update(_normalized_label(path, base_dir).encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(path.read_bytes())
    hasher.update(b"\0")


def _yaml_files(directory: Path) -> Iterable[Path]:
    if not directory.exists():
        return []
    return sorted(p for p in directory.rglob("*.y*ml") if p.is_file())


def compute_config_hash(project_yaml: Path, tasks_path: Path) -> str:
    """Compute a deterministic hash across relevant config inputs."""

    hasher = hashlib.sha256()
    base_dir = project_yaml.parent.resolve()
    cfg = read_project(project_yaml)

    required = [
        project_yaml.resolve(),
        _resolve_relative(project_yaml, cfg.paths.dataset).resolve(),
        _resolve_relative(project_yaml, cfg.paths.postprocess).resolve(),
    ]

    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Expected config file missing: {path}")
        _hash_file(hasher, path, base_dir)

    if not tasks_path.is_dir():
        raise TypeError(
            f"project.paths.tasks must point to a directory, got: {tasks_path}"
        )
    hasher.update(
        f"[dir]{_normalized_label(tasks_path, base_dir)}".encode("utf-8")
    )
    for p in _yaml_files(tasks_path):
        _hash_file(hasher, p, base_dir)

    for dir_value in (cfg.paths.sources, cfg.paths.streams):
        directory = _resolve_relative(project_yaml, dir_value)
        hasher.update(
            f"[dir]{_normalized_label(directory, base_dir)}".encode("utf-8")
        )
        if not directory.exists():
            hasher.update(b"[missing]")
            continue
        for path in _yaml_files(directory):
            _hash_file(hasher, path, base_dir)

    return hasher.hexdigest()
