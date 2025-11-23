from __future__ import annotations

from pathlib import Path
from typing import Sequence, Tuple

from datapipeline.config.build import PartitionedIdsConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent


def _collect_partitioned_ids(runtime: Runtime, target: str) -> Sequence[str]:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    target = (target or "features").lower()
    if target not in {"features", "targets"}:
        raise ValueError("target must be 'features' or 'targets'")
    cfgs = list(dataset.features or []) if target == "features" else list(dataset.targets or [])
    sanitized = [cfg.model_copy(update={"scale": False}) for cfg in cfgs]
    if not sanitized:
        return []

    ids: set[str] = set()
    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        sanitized,
        dataset.group_by,
    )
    for sample in vectors:
        ids.update(sample.features.values.keys())
    return sorted(ids)


def materialize_partitioned_ids(runtime: Runtime, task_cfg: PartitionedIdsConfig) -> Tuple[str, int]:
    ids = _collect_partitioned_ids(runtime, target=task_cfg.target)

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)

    with destination.open("w", encoding="utf-8") as fh:
        for fid in ids:
            fh.write(f"{fid}\n")

    return str(relative_path), len(ids)
