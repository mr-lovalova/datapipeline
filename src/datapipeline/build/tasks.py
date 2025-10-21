from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Iterable, Sequence, Tuple

from datapipeline.config.build import BuildConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.constants import PARTIONED_IDS
from datapipeline.services.project_paths import read_project
from datapipeline.utils.paths import ensure_parent


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


def compute_config_hash(project_yaml: Path, build_config_path: Path) -> str:
    """Compute a deterministic hash across relevant config inputs."""

    hasher = hashlib.sha256()
    base_dir = project_yaml.parent.resolve()
    cfg = read_project(project_yaml)

    required: Sequence[Path] = [
        project_yaml.resolve(),
        build_config_path.resolve(),
        _resolve_relative(project_yaml, cfg.paths.dataset).resolve(),
        _resolve_relative(project_yaml, cfg.paths.postprocess).resolve(),
    ]

    for path in required:
        if not path.exists():
            raise FileNotFoundError(f"Expected config file missing: {path}")
        _hash_file(hasher, path, base_dir)

    for dir_value in (cfg.paths.sources, cfg.paths.streams):
        directory = _resolve_relative(project_yaml, dir_value)
        hasher.update(f"[dir]{_normalized_label(directory, base_dir)}".encode("utf-8"))
        if not directory.exists():
            hasher.update(b"[missing]")
            continue
        for path in _yaml_files(directory):
            _hash_file(hasher, path, base_dir)

    return hasher.hexdigest()


def _collect_partitioned_ids(runtime: Runtime, include_targets: bool) -> Sequence[str]:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    feature_cfgs = list(dataset.features or [])
    if include_targets:
        feature_cfgs += list(dataset.targets or [])

    ids: set[str] = set()
    vectors = build_vector_pipeline(runtime, feature_cfgs, dataset.group_by, stage=None)
    for _, vector in vectors:
        ids.update(vector.values.keys())
    return sorted(ids)


def materialize_partitioned_ids(runtime: Runtime, config: BuildConfig) -> Tuple[str, int]:
    """Write the partitioned-id list and return (relative_path, count)."""

    task_cfg = config.partitioned_ids
    ids = _collect_partitioned_ids(runtime, include_targets=task_cfg.include_targets)

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)

    with destination.open("w", encoding="utf-8") as fh:
        for fid in ids:
            fh.write(f"{fid}\n")

    return str(relative_path), len(ids)


def execute_build(runtime: Runtime, config: BuildConfig) -> Dict[str, Dict[str, object]]:
    """Materialize artifacts described by build.yaml."""

    rel_path, count = materialize_partitioned_ids(runtime, config)
    return {
        PARTIONED_IDS: {
            "relative_path": rel_path,
            "count": count,
        }
    }
