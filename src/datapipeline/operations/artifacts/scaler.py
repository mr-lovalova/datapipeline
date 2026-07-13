from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

from datapipeline.artifacts.scaler import (
    TemporalScalerArtifact,
    TemporalScalerFold,
    TemporalScalerSplit,
    save_scaler_artifact,
)
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.config.split import (
    HASH_SPLIT_GROUP_KEY,
    HashSplitConfig,
    TimeSplitConfig,
)
from datapipeline.config.tasks import ScalerTask
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.vector import Vector
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.feature.pipeline import run_feature_pipeline
from datapipeline.pipelines.full.split import HashLabeler, TimeLabeler, build_labeler
from datapipeline.pipelines.vector.keygen import group_key_for
from datapipeline.runtime import Runtime
from datapipeline.transforms.feature.scaler import (
    ScalerAccumulator,
)
from datapipeline.utils.time import parse_cadence


def materialize_scaler_statistics(
    runtime: Runtime,
    task_cfg: ScalerTask,
) -> ArtifactOutput | None:
    dataset = load_dataset(runtime.project_yaml)
    validate_dataset_feature_identity(runtime, dataset)
    if not dataset_requires_scaler(dataset):
        return None

    cadence = dataset.sample.cadence
    scaled_configs = _scaled_configs([*dataset.features, *dataset.targets])
    if task_cfg.folds is not None:
        return _materialize_temporal_scaler_statistics(
            runtime,
            task_cfg,
            scaled_configs,
            cadence,
            dataset.sample.keys,
        )

    split_config = runtime.split
    labeler = build_labeler(split_config) if split_config is not None else None
    if labeler is None and task_cfg.split_label != "all":
        raise RuntimeError(
            f"Cannot compute scaler statistics for split {task_cfg.split_label!r} "
            "when no split configuration is defined in the project."
        )
    if (
        task_cfg.split_label != "all"
        and isinstance(split_config, HashSplitConfig)
        and split_config.key != HASH_SPLIT_GROUP_KEY
    ):
        raise ValueError(
            "Scaler split fitting requires hash split key 'group'; "
            "feature-based split keys can change during feature processing. "
            "Use key 'group', a time split, or scaler split_label 'all'."
        )

    accumulator = _fit_standard_scaler(
        runtime,
        scaled_configs,
        cadence,
        dataset.sample.keys,
        task_cfg,
        labeler,
    )
    if accumulator.observations == 0:
        raise RuntimeError(
            f"No scaler statistics computed for split {task_cfg.split_label!r}."
        )
    artifact = accumulator.artifact(split=task_cfg.split_label)
    relative_path = Path(task_cfg.output)
    save_scaler_artifact(runtime.artifacts_root / relative_path, artifact)

    return ArtifactOutput(
        relative_path=str(relative_path),
        meta={
            "features": len(artifact.statistics),
            "split": task_cfg.split_label,
            "observations": artifact.observations,
        },
    )


def _scaled_configs(configs: list[FeatureRecordConfig]) -> list[FeatureRecordConfig]:
    return [
        config.model_copy(update={"scale": False, "sequence": None})
        for config in configs
        if config.scale
    ]


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()


def _iter_unscaled_features(
    runtime: Runtime,
    configs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
) -> Iterator[tuple[tuple, FeatureRecord]]:
    context = PipelineContext(runtime)
    cadence_step = parse_cadence(cadence)
    for config in configs:
        stream = run_feature_pipeline(
            context,
            config,
            sample_keys=sample_keys,
            group_by_cadence=cadence,
        )
        try:
            for feature in stream:
                if not isinstance(feature, FeatureRecord):
                    raise TypeError(
                        "Scaler fitting requires scalar feature records before "
                        "sequence construction."
                    )
                yield group_key_for(feature, cadence_step), feature
        finally:
            _close_iterator(stream)


def _fit_standard_scaler(
    runtime: Runtime,
    configs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
    task: ScalerTask,
    labeler: HashLabeler | TimeLabeler | None,
) -> ScalerAccumulator:
    accumulator = ScalerAccumulator(task.with_mean, task.with_std, task.epsilon)
    include_all = task.split_label == "all"
    empty_vector = Vector(values={})
    for key, feature in _iter_unscaled_features(
        runtime,
        configs,
        cadence,
        sample_keys,
    ):
        if (
            not include_all
            and labeler is not None
            and labeler.label(key, empty_vector) != task.split_label
        ):
            continue
        accumulator.observe(feature.id, feature.value)
    return accumulator


def _materialize_temporal_scaler_statistics(
    runtime: Runtime,
    task_cfg: ScalerTask,
    configs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
) -> ArtifactOutput:
    split_config = runtime.split
    if not isinstance(split_config, TimeSplitConfig):
        raise RuntimeError("Scaler folds require project split mode 'time'.")
    folds = task_cfg.folds
    if folds is None:
        raise RuntimeError("Temporal scaler fitting requires scaler folds.")

    split_labels = set(split_config.labels)
    for fold_index, fold in enumerate(folds):
        for label in (*fold.fit, *fold.apply):
            if label not in split_labels:
                raise RuntimeError(
                    f"Scaler fold {fold_index} references unknown split label {label!r}."
                )
    applied_labels = {label for fold in folds for label in fold.apply}
    missing_apply = split_labels - applied_labels
    if missing_apply:
        raise RuntimeError(
            "Scaler folds do not apply to split labels: "
            + ", ".join(sorted(missing_apply))
        )

    fit_indexes_by_label: dict[str, list[int]] = defaultdict(list)
    for fold_index, fold in enumerate(folds):
        for label in fold.fit:
            fit_indexes_by_label[label].append(fold_index)

    accumulators = [
        ScalerAccumulator(task_cfg.with_mean, task_cfg.with_std, task_cfg.epsilon)
        for _ in folds
    ]
    labeler = TimeLabeler(split_config)
    empty_vector = Vector(values={})
    for _key, feature in _iter_unscaled_features(
        runtime,
        configs,
        cadence,
        sample_keys,
    ):
        label = labeler.label(feature.record.time, empty_vector)
        for fold_index in fit_indexes_by_label.get(label, ()):
            accumulators[fold_index].observe(feature.id, feature.value)

    artifact_folds: list[TemporalScalerFold] = []
    for fold_index, (fold, accumulator) in enumerate(zip(folds, accumulators)):
        if accumulator.observations == 0:
            raise RuntimeError(
                f"No scaler statistics computed for scaler fold {fold_index}."
            )
        artifact_folds.append(
            TemporalScalerFold(
                fit=tuple(fold.fit),
                apply=tuple(fold.apply),
                scaler=accumulator.artifact(),
            )
        )

    artifact = TemporalScalerArtifact(
        split=TemporalScalerSplit(
            boundaries=tuple(split_config.boundaries),
            labels=tuple(split_config.labels),
        ),
        folds=tuple(artifact_folds),
    )
    relative_path = Path(task_cfg.output)
    save_scaler_artifact(runtime.artifacts_root / relative_path, artifact)
    observations = sum(fold.scaler.observations for fold in artifact.folds)
    return ArtifactOutput(
        relative_path=str(relative_path),
        meta={
            "mode": "temporal",
            "folds": len(artifact.folds),
            "observations": observations,
        },
    )
