from collections.abc import Iterator, Sequence
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.dag import StageDag
from datapipeline.dag.runner import run_stage_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.pipelines.vector.dag import build_vector_pipeline
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.full.split import apply_split_stage


def build_full_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
) -> Iterator[Any]:
    full_dag = StageDag(
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
            PipelineNode(
                name="split",
                op=apply_split_stage,
                args=(context.runtime,),
                input="post_processed",
                output="served",
            ),
        ),
    )
    return run_stage_dag(context, full_dag)
