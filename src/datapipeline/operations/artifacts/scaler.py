from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterator

from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.tasks import ScalerTask
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.split import TimeSplitConfig
from datapipeline.domain.sample import Sample
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.pipelines.full.split import build_labeler
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.runtime import Runtime
from datapipeline.transforms.feature.scaler import (
    StandardScaler,
    StandardScalerAccumulator,
)
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.utils.paths import ensure_parent


def materialize_scaler_statistics(
    runtime: Runtime,
    task_cfg: ScalerTask,
) -> ArtifactOutput | None:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or [])
    if not dataset_requires_scaler(dataset):
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
        sample_keys=dataset.sample_keys,
    )

    cfg = getattr(runtime, "split", None)
    labeler = build_labeler(cfg) if cfg else None
    if task_cfg.folds is not None:
        return _materialize_temporal_scaler_statistics(
            runtime=runtime,
            task_cfg=task_cfg,
            vectors=vectors,
            split_cfg=cfg,
            labeler=labeler,
        )

    if not labeler and task_cfg.split_label != "all":
        raise RuntimeError(
            f"Cannot compute scaler statistics for split '{task_cfg.split_label}' "
            "when no split configuration is defined in the project."
        )

    def _train_stream() -> Iterator[Sample]:
        include_all = task_cfg.split_label == "all"
        for sample in vectors:
            if not include_all and labeler and labeler.label(sample.key, sample.features) != task_cfg.split_label:
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

    scaler.save(
        destination,
        split=task_cfg.split_label,
        observations=total_observations,
    )

    meta: Dict[str, object] = {
        "features": len(scaler.statistics),
        "split": task_cfg.split_label,
        "observations": total_observations,
    }

    return ArtifactOutput(relative_path=str(relative_path), meta=meta)


def _materialize_temporal_scaler_statistics(
    *,
    runtime: Runtime,
    task_cfg: ScalerTask,
    vectors: Iterator[Sample],
    split_cfg,
    labeler,
) -> ArtifactOutput:
    if not isinstance(split_cfg, TimeSplitConfig):
        raise RuntimeError("Scaler folds require project split mode 'time'.")
    if split_cfg.boundaries is None or split_cfg.labels is None:
        raise RuntimeError("Scaler folds require time split boundaries and labels.")
    if labeler is None:
        raise RuntimeError("Scaler folds require project split configuration.")

    split_labels = set(split_cfg.labels)
    for fold_index, fold in enumerate(task_cfg.folds or []):
        for label in [*fold.fit, *fold.apply]:
            if label not in split_labels:
                raise RuntimeError(
                    f"Scaler fold {fold_index} references unknown split label {label!r}."
                )

    fit_indexes_by_label: dict[str, list[int]] = defaultdict(list)
    for fold_index, fold in enumerate(task_cfg.folds or []):
        for label in fold.fit:
            fit_indexes_by_label[label].append(fold_index)

    accumulators = [StandardScalerAccumulator() for _fold in task_cfg.folds or []]
    observations = [0 for _fold in task_cfg.folds or []]

    for sample in vectors:
        label = labeler.label(sample.key, sample.features)
        values = getattr(sample.features, "values", {})
        for fold_index in fit_indexes_by_label.get(label, ()):
            observations[fold_index] += accumulators[fold_index].observe(values)

    folds_payload: list[dict[str, Any]] = []
    for fold_index, fold in enumerate(task_cfg.folds or []):
        scaler = accumulators[fold_index].to_scaler()
        if not scaler.statistics:
            raise RuntimeError(
                f"No scaler statistics computed for scaler fold {fold_index}."
            )
        folds_payload.append(
            {
                "fit": list(fold.fit),
                "apply": list(fold.apply),
                "observations": observations[fold_index],
                "scaler": scaler.to_dict(),
            }
        )

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)

    payload = {
        "kind": "temporal_scaler",
        "version": 1,
        "split": {
            "mode": "time",
            "boundaries": list(split_cfg.boundaries),
            "labels": list(split_cfg.labels),
        },
        "folds": folds_payload,
    }
    write_json_artifact(destination, payload)

    total_observations = sum(observations)
    meta: Dict[str, object] = {
        "mode": "temporal",
        "folds": len(folds_payload),
        "observations": total_observations,
    }

    return ArtifactOutput(relative_path=str(relative_path), meta=meta)
