from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterator

from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.tasks import ScalerTask
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.config.split import (
    HASH_SPLIT_FEATURE_PREFIX,
    HashSplitConfig,
    TimeSplitConfig,
)
from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.vector import Vector
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.feature.dag import build_feature_pipeline
from datapipeline.pipelines.vector.keygen import group_key_for
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
    validate_dataset_feature_identity(runtime, dataset)
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or [])
    if not dataset_requires_scaler(dataset):
        return None

    cfg = getattr(runtime, "split", None)
    labeler = build_labeler(cfg) if cfg else None
    scaled_cfgs = _scaled_configs([*feature_cfgs, *target_cfgs])
    if task_cfg.folds is not None:
        return _materialize_temporal_scaler_statistics(
            runtime=runtime,
            task_cfg=task_cfg,
            configs=scaled_cfgs,
            group_by=dataset.group_by,
            sample_keys=dataset.sample_keys,
            split_cfg=cfg,
            labeler=labeler,
        )

    if not labeler and task_cfg.split_label != "all":
        raise RuntimeError(
            f"Cannot compute scaler statistics for split '{task_cfg.split_label}' "
            "when no split configuration is defined in the project."
        )

    split_feature_id = _split_feature_id(cfg)
    if split_feature_id is not None and task_cfg.split_label != "all":
        scaler, total_observations = _fit_standard_scaler_from_feature_split(
            runtime=runtime,
            features=feature_cfgs,
            configs=scaled_cfgs,
            group_by=dataset.group_by,
            sample_keys=dataset.sample_keys,
            split_label=task_cfg.split_label,
            labeler=labeler,
            split_feature_id=split_feature_id,
        )
    else:
        scaler, total_observations = _fit_standard_scaler_from_feature_streams(
            runtime=runtime,
            configs=scaled_cfgs,
            group_by=dataset.group_by,
            sample_keys=dataset.sample_keys,
            split_label=task_cfg.split_label,
            labeler=labeler,
        )

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


def _scaled_configs(configs: list[FeatureRecordConfig]) -> list[FeatureRecordConfig]:
    scaled: list[FeatureRecordConfig] = []
    for cfg in configs:
        scale = getattr(cfg, "scale", False)
        if isinstance(scale, dict) or bool(scale):
            scaled.append(cfg.model_copy(update={"scale": False}))
    return scaled


def _split_feature_id(split_cfg) -> str | None:
    if not isinstance(split_cfg, HashSplitConfig):
        return None
    if not split_cfg.key.startswith(HASH_SPLIT_FEATURE_PREFIX):
        return None
    return split_cfg.key.removeprefix(HASH_SPLIT_FEATURE_PREFIX)


def _feature_value(item: FeatureRecord | FeatureRecordSequence):
    if isinstance(item, FeatureRecordSequence):
        return list(item.values)
    return item.value


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()


def _iter_unscaled_feature_values(
    *,
    runtime: Runtime,
    configs: list[FeatureRecordConfig],
    group_by: str,
    sample_keys: list[str],
) -> Iterator[tuple[tuple, str, object]]:
    context = PipelineContext(runtime)
    for cfg in configs:
        stream = build_feature_pipeline(
            context,
            cfg,
            sample_keys=sample_keys,
            group_by_cadence=group_by,
        )
        try:
            for item in stream:
                yield group_key_for(item, group_by), item.id, _feature_value(item)
        finally:
            _close_iterator(stream)


def _fit_standard_scaler_from_feature_streams(
    *,
    runtime: Runtime,
    configs: list[FeatureRecordConfig],
    group_by: str,
    sample_keys: list[str],
    split_label: str,
    labeler,
) -> tuple[StandardScaler, int]:
    include_all = split_label == "all"
    accumulator = StandardScalerAccumulator()
    observations = 0
    for key, feature_id, value in _iter_unscaled_feature_values(
        runtime=runtime,
        configs=configs,
        group_by=group_by,
        sample_keys=sample_keys,
    ):
        if (
            not include_all
            and labeler
            and labeler.label(key, Vector(values={})) != split_label
        ):
            continue
        observations += accumulator.observe({feature_id: value})
    return accumulator.to_scaler(), observations


def _split_labels_by_key(
    *,
    runtime: Runtime,
    features: list[FeatureRecordConfig],
    group_by: str,
    sample_keys: list[str],
    labeler,
    split_feature_id: str,
) -> dict[tuple, str]:
    values_by_key: dict[tuple, list[object]] = defaultdict(list)
    for key, feature_id, value in _iter_unscaled_feature_values(
        runtime=runtime,
        configs=[cfg.model_copy(update={"scale": False}) for cfg in features],
        group_by=group_by,
        sample_keys=sample_keys,
    ):
        if feature_id == split_feature_id:
            values_by_key[key].append(value)
    if not values_by_key:
        raise RuntimeError(
            f"Hash split feature '{split_feature_id}' produced no scaler labels."
        )
    labels: dict[tuple, str] = {}
    for key, values in values_by_key.items():
        split_value = values[0] if len(values) == 1 else list(values)
        labels[key] = labeler.label(
            key,
            Vector(values={split_feature_id: split_value}),
        )
    return labels


def _fit_standard_scaler_from_feature_split(
    *,
    runtime: Runtime,
    features: list[FeatureRecordConfig],
    configs: list[FeatureRecordConfig],
    group_by: str,
    sample_keys: list[str],
    split_label: str,
    labeler,
    split_feature_id: str,
) -> tuple[StandardScaler, int]:
    labels_by_key = _split_labels_by_key(
        runtime=runtime,
        features=features,
        group_by=group_by,
        sample_keys=sample_keys,
        labeler=labeler,
        split_feature_id=split_feature_id,
    )
    accumulator = StandardScalerAccumulator()
    observations = 0
    for key, feature_id, value in _iter_unscaled_feature_values(
        runtime=runtime,
        configs=configs,
        group_by=group_by,
        sample_keys=sample_keys,
    ):
        label = labels_by_key.get(key)
        if label is None:
            raise RuntimeError(
                f"Hash split feature '{split_feature_id}' has no value for sample key {key!r}."
            )
        if label != split_label:
            continue
        observations += accumulator.observe({feature_id: value})
    return accumulator.to_scaler(), observations


def _materialize_temporal_scaler_statistics(
    *,
    runtime: Runtime,
    task_cfg: ScalerTask,
    configs: list[FeatureRecordConfig],
    group_by: str,
    sample_keys: list[str],
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

    for key, feature_id, value in _iter_unscaled_feature_values(
        runtime=runtime,
        configs=configs,
        group_by=group_by,
        sample_keys=sample_keys,
    ):
        label = labeler.label(key, Vector(values={}))
        for fold_index in fit_indexes_by_label.get(label, ()):
            observations[fold_index] += accumulators[fold_index].observe(
                {feature_id: value}
            )

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
