from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from datapipeline.artifacts.scaler import (
    TemporalScalerArtifact,
    TemporalScalerFold,
    TemporalScalerSplit,
    save_scaler_artifact,
)
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.split import (
    HASH_SPLIT_GROUP_KEY,
    HashSplitConfig,
    TimeSplitConfig,
)
from datapipeline.config.tasks import ScalerTask
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.vector import Vector
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.feature.projector import FeatureProjector
from datapipeline.pipelines.full.split import HashLabeler, TimeLabeler, build_labeler
from datapipeline.pipelines.stream.pipeline import run_stream_pipeline
from datapipeline.runtime import Runtime, require_runtime_stream
from datapipeline.transforms.feature.scaler import (
    ScalerAccumulator,
)
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence


@dataclass(frozen=True)
class _ScalerInput:
    group_key: tuple
    features: tuple[FeatureRecord, ...]


def materialize_scaler_statistics(
    runtime: Runtime,
    task_cfg: ScalerTask,
) -> ArtifactOutput | None:
    dataset = runtime.dataset
    if not dataset_requires_scaler(dataset):
        return None

    cadence = dataset.sample.cadence
    scaled_configs = [
        config for config in (*dataset.features, *dataset.targets) if config.scale
    ]
    if task_cfg.folds is not None:
        return _materialize_temporal_scaler_statistics(
            runtime,
            task_cfg,
            scaled_configs,
            cadence,
            dataset.sample.keys,
        )

    split_config = dataset.split
    labeler = build_labeler(split_config) if split_config is not None else None
    if labeler is None and task_cfg.split_label != "all":
        raise RuntimeError(
            f"Cannot compute scaler statistics for split {task_cfg.split_label!r} "
            "when no split configuration is defined in dataset.yaml."
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


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()


def _iter_scaler_inputs(
    runtime: Runtime,
    configs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
) -> Iterator[_ScalerInput]:
    context = PipelineContext(runtime)
    cadence_step = parse_cadence(cadence)
    sample_key_contract = SampleKeyContract(sample_keys)
    configs_by_stream: dict[str, list[FeatureRecordConfig]] = defaultdict(list)
    for config in configs:
        configs_by_stream[config.stream].append(config)

    for stream_id, stream_configs in configs_by_stream.items():
        runtime_stream = require_runtime_stream(runtime, stream_id)
        projector = FeatureProjector(
            runtime_stream.partition_by,
            sample_key_contract,
        )
        records = run_stream_pipeline(context, stream_id)
        try:
            for record in records:
                features = tuple(projector.project(record, stream_configs))
                yield _ScalerInput(
                    group_key=(
                        floor_time_to_cadence(record.time, cadence_step),
                        *features[0].entity_key,
                    ),
                    features=features,
                )
        finally:
            _close_iterator(records)


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
    inputs = _iter_scaler_inputs(
        runtime,
        configs,
        cadence,
        sample_keys,
    )
    try:
        for item in inputs:
            if (
                not include_all
                and labeler is not None
                and labeler.label(item.group_key, empty_vector) != task.split_label
            ):
                continue
            for feature in item.features:
                accumulator.observe(feature.id, feature.value)
    finally:
        _close_iterator(inputs)
    return accumulator


def _materialize_temporal_scaler_statistics(
    runtime: Runtime,
    task_cfg: ScalerTask,
    configs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
) -> ArtifactOutput:
    split_config = runtime.dataset.split
    if not isinstance(split_config, TimeSplitConfig):
        raise RuntimeError("Scaler folds require dataset split mode 'time'.")
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
    inputs = _iter_scaler_inputs(
        runtime,
        configs,
        cadence,
        sample_keys,
    )
    try:
        for item in inputs:
            label = labeler.label(item.features[0].time, empty_vector)
            for fold_index in fit_indexes_by_label.get(label, ()):
                accumulator = accumulators[fold_index]
                for feature in item.features:
                    accumulator.observe(feature.id, feature.value)
    finally:
        _close_iterator(inputs)

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
