from collections.abc import Generator, Sequence
from functools import partial

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import SourceNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.dataset.nodes import build_postprocess_plan
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.domain.sample import Sample


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
            SourceNode(
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
            ),
            *postprocess.nodes,
        ),
    )
