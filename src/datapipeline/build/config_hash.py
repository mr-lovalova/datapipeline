import hashlib
from pathlib import Path
from typing import Iterable

from datapipeline.services.config_refs import (
    collect_config_ref_keys,
    merged_project_env,
)
from datapipeline.services.path_policy import resolve_project_path
from datapipeline.services.project_paths import read_project
from datapipeline.utils.load import load_yaml


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


def _hash_env_refs(hasher, project_yaml: Path, config_files: list[Path]) -> None:
    env_ref_names: set[str] = set()
    for path in config_files:
        data = load_yaml(path, require_mapping=False)
        env_ref_names.update(
            key
            for _scheme, key in collect_config_ref_keys(data, scheme="env")
        )

    if not env_ref_names:
        return

    env = merged_project_env(project_yaml.resolve())
    for name in sorted(env_ref_names):
        hasher.update(f"[env]{name}".encode("utf-8"))
        hasher.update(b"\0")
        value = env.get(name)
        if value is None:
            hasher.update(b"[missing]")
        else:
            hasher.update(str(value).encode("utf-8"))
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
        resolve_project_path(project_yaml, cfg.paths.dataset),
        resolve_project_path(project_yaml, cfg.paths.postprocess),
    ]
    hashed_files: list[Path] = []

    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Expected config file missing: {path}")
        _hash_file(hasher, path, base_dir)
        hashed_files.append(path)

    if not tasks_path.is_dir():
        raise TypeError(
            f"project.paths.tasks must point to a directory, got: {tasks_path}"
        )
    hasher.update(
        f"[dir]{_normalized_label(tasks_path, base_dir)}".encode("utf-8")
    )
    for p in _yaml_files(tasks_path):
        _hash_file(hasher, p, base_dir)
        hashed_files.append(p)

    for dir_value in (cfg.paths.ingests, cfg.paths.sources, cfg.paths.streams):
        dir_values = dir_value if isinstance(dir_value, list) else [dir_value]
        for raw_path in dir_values:
            directory = resolve_project_path(project_yaml, raw_path)
            hasher.update(
                f"[dir]{_normalized_label(directory, base_dir)}".encode("utf-8")
            )
            if not directory.exists():
                hasher.update(b"[missing]")
                continue
            for path in _yaml_files(directory):
                _hash_file(hasher, path, base_dir)
                hashed_files.append(path)

    _hash_env_refs(hasher, project_yaml, hashed_files)

    return hasher.hexdigest()
