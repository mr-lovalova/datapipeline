from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterator, Tuple

from datapipeline.config.tasks import ScalerTask
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.domain.sample import Sample
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.split import build_labeler
from datapipeline.runtime import Runtime
from datapipeline.transforms.feature.scaler import StandardScaler
from datapipeline.utils.paths import ensure_parent


def materialize_scaler_statistics(runtime: Runtime, task_cfg: ScalerTask) -> Tuple[str, Dict[str, object]] | None:
    if not task_cfg.enabled:
        return None

    dataset = load_dataset(runtime.project_yaml, "vectors")
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or [])
    if not feature_cfgs and not target_cfgs:
        return None

    sanitized_features = [cfg.model_copy(update={"scale": False}) for cfg in feature_cfgs]
    sanitized_targets = [cfg.model_copy(update={"scale": False}) for cfg in target_cfgs]

    context = PipelineContext(runtime)
    vectors = build_vector_pipeline(
        context,
        sanitized_features,
        dataset.group_by,
        target_configs=sanitized_targets,
        rectangular=False,
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
