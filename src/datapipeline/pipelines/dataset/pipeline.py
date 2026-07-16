from collections.abc import Collection, Generator, Sequence
from functools import partial

from datapipeline.artifacts.registry import SCALER_SPEC
from datapipeline.artifacts.scaler import (
    FoldedScalerArtifact,
    StandardScalerArtifact,
)
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.split import DatasetFold
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode, SourceNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.dataset.nodes import (
    build_postprocess_plan,
    select_fold_samples,
)
from datapipeline.pipelines.dataset.split import build_labeler
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.domain.sample import Sample
from datapipeline.transforms.vector.scaler import SampleScaler


def run_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
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
    feature_configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Pipeline:
    postprocess = build_postprocess_plan(context)
    return Pipeline(
        name="dataset",
        nodes=(
            _vector_source(
                context,
                feature_configs,
                group_by_cadence,
                target_configs,
                rectangular,
                sample_keys,
            ),
            *postprocess.nodes,
        ),
    )


def run_scaled_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
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
            nodes=(
                _vector_source(
                    context,
                    feature_configs,
                    group_by_cadence,
                    target_configs,
                    rectangular,
                    sample_keys,
                ),
                PipelineNode(name="scale_samples", apply=scaler.apply),
                *postprocess.nodes,
            ),
        ),
    )


def run_fold_dataset_pipeline(
    context: PipelineContext,
    feature_configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    fold: DatasetFold,
    labels: Collection[str],
    target_configs: Sequence[FeatureRecordConfig] | None = None,
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

    nodes: list[PipelineNode | SourceNode] = [
        _vector_source(
            context,
            feature_configs,
            group_by_cadence,
            target_configs,
            rectangular,
            sample_keys,
        ),
        PipelineNode(
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
        nodes.append(PipelineNode(name="scale_samples", apply=scaler.apply))

    postprocess = build_postprocess_plan(context)
    nodes.extend(postprocess.nodes)
    return run_pipeline(
        context,
        Pipeline(name=f"dataset:{fold.id}", nodes=tuple(nodes)),
    )


def _sample_scaler(
    artifact: StandardScalerArtifact,
    feature_configs: Sequence[FeatureRecordConfig],
    target_configs: Sequence[FeatureRecordConfig] | None,
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


def _vector_source(
    context: PipelineContext,
    feature_configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None,
    rectangular: bool,
    sample_keys: Sequence[str],
) -> SourceNode:
    return SourceNode(
        name="vector_assemble",
        open=partial(
            build_vector_pipeline,
            context,
            feature_configs,
            group_by_cadence,
            target_configs,
            rectangular,
            sample_keys,
        ),
    )
