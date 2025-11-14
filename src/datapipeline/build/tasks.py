from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Iterable, Iterator, Sequence, Tuple

from datapipeline.config.build import BuildConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.domain.sample import Sample
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.split import build_labeler
from datapipeline.runtime import Runtime
from datapipeline.services.constants import PARTIONED_IDS, SCALER_STATISTICS
from datapipeline.services.project_paths import read_project
from datapipeline.utils.paths import ensure_parent
from datapipeline.transforms.feature.scaler import StandardScaler


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
        hasher.update(
            f"[dir]{_normalized_label(directory, base_dir)}".encode("utf-8"))
        if not directory.exists():
            hasher.update(b"[missing]")
            continue
        for path in _yaml_files(directory):
            _hash_file(hasher, path, base_dir)

    return hasher.hexdigest()


def _collect_partitioned_ids(runtime: Runtime, include_targets: bool) -> Sequence[str]:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or []) if include_targets else []

    sanitized_features = [cfg.model_copy(update={"scale": False}) for cfg in feature_cfgs]
    sanitized_targets = [cfg.model_copy(update={"scale": False}) for cfg in target_cfgs]

    ids: set[str] = set()
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        sanitized_features,
        dataset.group_by,
        stage=None,
        target_configs=sanitized_targets,
    )
    for sample in vectors:
        ids.update(sample.features.values.keys())
        if sample.targets:
            ids.update(sample.targets.values.keys())
    return sorted(ids)


def materialize_partitioned_ids(runtime: Runtime, config: BuildConfig) -> Tuple[str, int]:
    """Write the partitioned-id list and return (relative_path, count)."""

    task_cfg = config.partitioned_ids
    ids = _collect_partitioned_ids(
        runtime, include_targets=task_cfg.include_targets)

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)

    with destination.open("w", encoding="utf-8") as fh:
        for fid in ids:
            fh.write(f"{fid}\n")

    return str(relative_path), len(ids)


def materialize_scaler_statistics(runtime: Runtime, config: BuildConfig) -> Tuple[str, Dict[str, object]] | None:
    task_cfg = config.scaler
    if not task_cfg.enabled:
        return None

    dataset = load_dataset(runtime.project_yaml, "vectors")
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or []) if task_cfg.include_targets else []
    if not feature_cfgs and not target_cfgs:
        return None

    sanitized_features = [cfg.model_copy(update={"scale": False}) for cfg in feature_cfgs]
    sanitized_targets = [cfg.model_copy(update={"scale": False}) for cfg in target_cfgs]

    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        sanitized_features,
        dataset.group_by,
        stage=None,
        target_configs=sanitized_targets,
    )

    cfg = getattr(runtime, "split", None)
    labeler = build_labeler(cfg) if cfg else None
    if not labeler and task_cfg.split_label != "all":
        raise RuntimeError(
            f"Cannot compute scaler statistics for split '{task_cfg.split_label}' "
            "when no split configuration is defined in the project."
        )

    def _train_stream() -> Iterator[Sample]:
        for sample in vectors:
            if labeler and labeler.label(sample.key, sample.features) != task_cfg.split_label:
                continue
            yield sample

    scaler = StandardScaler()
    total_observations = scaler.fit(_train_stream())

    if not scaler.statistics:
        raise RuntimeError(
            f"No scaler statistics computed for split '{task_cfg.split_label}'."
        )

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)

    scaler.save(destination)

    meta: Dict[str, object] = {
        "features": len(scaler.statistics),
        "split": task_cfg.split_label,
        "observations": total_observations,
    }

    return str(relative_path), meta


# execute_build removed in favor of per-artifact sessions in the CLI
