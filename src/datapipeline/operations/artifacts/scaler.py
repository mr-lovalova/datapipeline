from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from datapipeline.artifacts.scaler import (
    FoldedScalerArtifact,
    StandardScalerArtifact,
    save_scaler_artifact,
)
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.split import DatasetFold
from datapipeline.config.tasks import ScalerTask
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.dataset.split import build_labeler
from datapipeline.pipelines.feature.projector import FeatureProjector
from datapipeline.pipelines.stream.pipeline import run_stream_pipeline
from datapipeline.runtime import Runtime, require_runtime_stream
from datapipeline.transforms.vector.scaler import ScalerAccumulator
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

    configs = tuple(
        config
        for config in (*dataset.features, *dataset.targets)
        if config.scale
    )
    if dataset.split is None:
        standard = _fit_standard_scaler(runtime, configs, task_cfg)
        artifact: StandardScalerArtifact | FoldedScalerArtifact = standard
        meta = {
            "features": len(standard.statistics),
            "observations": standard.observations,
        }
    else:
        folded = _fit_folded_scaler(
            runtime,
            configs,
            dataset.split.folds,
            task_cfg,
        )
        artifact = folded
        meta = {
            "folds": len(folded.folds),
            "observations": sum(
                scaler.observations for scaler in folded.folds.values()
            ),
        }

    relative_path = Path(task_cfg.output)
    save_scaler_artifact(runtime.artifacts_root / relative_path, artifact)
    return ArtifactOutput(relative_path=str(relative_path), meta=meta)


def _fit_standard_scaler(
    runtime: Runtime,
    configs: Sequence[FeatureRecordConfig],
    task: ScalerTask,
) -> StandardScalerArtifact:
    accumulator = _new_accumulator(task)
    expected_ids: set[str] = set()
    inputs = _iter_scaler_inputs(runtime, configs)
    try:
        for item in inputs:
            for feature in item.features:
                expected_ids.add(feature.id)
                accumulator.observe(feature.id, feature.value)
    finally:
        _close_iterator(inputs)
    return _finish_scaler(accumulator, expected_ids, "dataset")


def _fit_folded_scaler(
    runtime: Runtime,
    configs: Sequence[FeatureRecordConfig],
    folds: Sequence[DatasetFold],
    task: ScalerTask,
) -> FoldedScalerArtifact:
    split = runtime.dataset.split
    assert split is not None

    train_folds_by_label: dict[str, list[str]] = defaultdict(list)
    output_folds_by_label: dict[str, list[str]] = defaultdict(list)
    accumulators = {fold.id: _new_accumulator(task) for fold in folds}
    expected_ids: dict[str, set[str]] = {fold.id: set() for fold in folds}
    for fold in folds:
        for label in fold.train:
            train_folds_by_label[label].append(fold.id)
        for label in (*fold.train, *fold.validation, *fold.test):
            output_folds_by_label[label].append(fold.id)

    labeler = build_labeler(split)
    inputs = _iter_scaler_inputs(runtime, configs)
    try:
        for item in inputs:
            label = labeler.label(item.group_key)
            for fold_id in output_folds_by_label.get(label, ()):
                expected_ids[fold_id].update(
                    feature.id for feature in item.features
                )
            for fold_id in train_folds_by_label.get(label, ()):
                accumulator = accumulators[fold_id]
                for feature in item.features:
                    accumulator.observe(feature.id, feature.value)
    finally:
        _close_iterator(inputs)

    return FoldedScalerArtifact(
        folds={
            fold.id: _finish_scaler(
                accumulators[fold.id],
                expected_ids[fold.id],
                f"dataset fold {fold.id!r}",
            )
            for fold in folds
        }
    )


def _finish_scaler(
    accumulator: ScalerAccumulator,
    expected_ids: set[str],
    scope: str,
) -> StandardScalerArtifact:
    if accumulator.observations == 0:
        raise RuntimeError(f"Scaler fitting produced no observations for {scope}.")
    artifact = accumulator.artifact()
    missing = expected_ids - artifact.statistics.keys()
    if missing:
        raise RuntimeError(
            f"Scaler fitting has no training observations for {scope} vector IDs: "
            + ", ".join(sorted(missing))
        )
    return artifact


def _new_accumulator(task: ScalerTask) -> ScalerAccumulator:
    return ScalerAccumulator(task.with_mean, task.with_std, task.epsilon)


def _iter_scaler_inputs(
    runtime: Runtime,
    configs: Sequence[FeatureRecordConfig],
) -> Iterator[_ScalerInput]:
    context = PipelineContext(runtime)
    cadence_step = parse_cadence(runtime.dataset.sample.cadence)
    sample_key_contract = SampleKeyContract(runtime.dataset.sample.keys)
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


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()
