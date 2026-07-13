from collections.abc import Iterator, Sequence
from functools import partial

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import SourceNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.full.nodes import build_postprocess_nodes
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.domain.sample import Sample


def run_full_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Iterator[Sample]:
    return run_pipeline(
        context,
        build_full_pipeline(
            context,
            configs,
            group_by_cadence,
            target_configs=target_configs,
            rectangular=rectangular,
            sample_keys=sample_keys,
        ),
    )


def build_full_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Pipeline:
    return Pipeline(
        name="pipeline:serve",
        nodes=(
            SourceNode(
                name="vector_assemble",
                open=partial(
                    build_vector_pipeline,
                    context,
                    configs,
                    group_by_cadence,
                    target_configs,
                    rectangular,
                    sample_keys,
                ),
            ),
            *build_postprocess_nodes(context),
        ),
    )
