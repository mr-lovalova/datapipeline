from collections.abc import Iterator, Sequence

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.dag import Dag
from datapipeline.dag.runner import run_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.sample import Sample


def build_full_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Iterator[Sample]:
    return run_dag(
        context,
        build_full_dag(
            context,
            configs,
            group_by_cadence,
            target_configs=target_configs,
            rectangular=rectangular,
            sample_keys=sample_keys,
        ),
    )


def build_full_dag(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
    sample_keys: Sequence[str] = (),
) -> Dag:
    return Dag(
        name="pipeline:serve",
        nodes=(
            PipelineNode(
                name="vector_assemble",
                op=build_vector_pipeline,
                args=(
                    context,
                    configs,
                    group_by_cadence,
                    target_configs,
                    rectangular,
                    sample_keys,
                ),
                output="vectors",
            ),
            PipelineNode(
                name="post_process",
                op=post_process,
                args=(context,),
                input="vectors",
                output="post_processed",
            ),
        ),
    )
