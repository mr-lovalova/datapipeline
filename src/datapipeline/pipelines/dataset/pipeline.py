from collections.abc import Collection, Generator, Sequence
from functools import partial

from datapipeline.artifacts.registry import SCALER_SPEC
from datapipeline.artifacts.scaler import (
    FoldedScalerArtifact,
    StandardScalerArtifact,
)
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.split import DatasetFold
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.domain.sample import Sample
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.pipeline import Pipeline, Stage
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.dataset.postprocess import (
    build_postprocess_plan,
    select_fold_samples,
)
from datapipeline.pipelines.dataset.split import build_labeler
from datapipeline.pipelines.sample.input import build_sample_input
from datapipeline.transforms.vector.scaler import SampleScaler


def run_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    target_configs: Sequence[SeriesConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Generator[Sample, None, None]:
    return run_pipeline(
        context,
        build_dataset_pipeline(
            context,
            feature_configs,
            group_by_cadence,
            target_configs=target_configs,
            rectangular=rectangular,
            sample_keys=sample_keys,
        ),
    )


def build_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    target_configs: Sequence[SeriesConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Pipeline:
    postprocess = build_postprocess_plan(context)
    return Pipeline(
        name="dataset",
        input=build_sample_input(
            context,
            feature_configs,
            group_by_cadence,
            target_configs,
            rectangular,
            sample_keys,
        ),
        stages=postprocess.stages,
    )


def run_scaled_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    target_configs: Sequence[SeriesConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Generator[Sample, None, None]:
    artifact = context.require_artifact(SCALER_SPEC)
    if not isinstance(artifact, StandardScalerArtifact):
        raise RuntimeError(
            "A dataset without folds requires a standard scaler artifact."
        )
    scaler = _sample_scaler(artifact, feature_configs, target_configs)
    postprocess = build_postprocess_plan(context)
    return run_pipeline(
        context,
        Pipeline(
            name="dataset",
            input=build_sample_input(
                context,
                feature_configs,
                group_by_cadence,
                target_configs,
                rectangular,
                sample_keys,
            ),
            stages=(
                Stage(name="scale_samples", apply=scaler.apply),
                *postprocess.stages,
            ),
        ),
    )


def run_fold_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[SeriesConfig],
    group_by_cadence: str,
    fold: DatasetFold,
    labels: Collection[str],
    target_configs: Sequence[SeriesConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Generator[Sample, None, None]:
    split = context.runtime.dataset.split
    if split is None:
        raise ValueError("Fold dataset output requires dataset split configuration.")

    included = frozenset(labels)
    fold_labels = frozenset((*fold.train, *fold.validation, *fold.test))
    if not included:
        raise ValueError(f"Dataset fold {fold.id!r} has no selected output labels.")
    if not included <= fold_labels:
        unknown = ", ".join(sorted(included - fold_labels))
        raise ValueError(
            f"Dataset fold {fold.id!r} does not contain selected labels: {unknown}."
        )

    pipeline_input = build_sample_input(
        context,
        feature_configs,
        group_by_cadence,
        target_configs,
        rectangular,
        sample_keys,
    )
    stages = [
        Stage(
            name="select_fold_samples",
            apply=partial(select_fold_samples, build_labeler(split), included),
        ),
    ]
    if dataset_requires_scaler(context.runtime.dataset):
        artifact = context.require_artifact(SCALER_SPEC)
        if not isinstance(artifact, FoldedScalerArtifact):
            raise RuntimeError("A split dataset requires a folded scaler artifact.")
        scaler = _sample_scaler(
            artifact.for_fold(fold.id),
            feature_configs,
            target_configs,
        )
        stages.append(Stage(name="scale_samples", apply=scaler.apply))

    postprocess = build_postprocess_plan(context)
    stages.extend(postprocess.stages)
    return run_pipeline(
        context,
        Pipeline(
            name=f"dataset:{fold.id}",
            input=pipeline_input,
            stages=tuple(stages),
        ),
    )


def _sample_scaler(
    artifact: StandardScalerArtifact,
    feature_configs: Sequence[SeriesConfig],
    target_configs: Sequence[SeriesConfig] | None,
) -> SampleScaler:
    return SampleScaler(
        artifact,
        scaled_feature_ids=tuple(
            config.id for config in feature_configs if config.scale
        ),
        scaled_target_ids=tuple(
            config.id
            for config in (() if target_configs is None else target_configs)
            if config.scale
        ),
    )
