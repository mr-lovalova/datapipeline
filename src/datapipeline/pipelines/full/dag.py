from collections.abc import Iterator, Sequence
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.dag import Dag
from datapipeline.dag.runner import run_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.pipelines.vector.dag import (
    VECTOR_ASSEMBLE_DAG_NAME,
    build_vector_dag,
    build_vector_pipeline,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.full.split import apply_split_stage


def build_full_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
) -> Iterator[Any]:
    return run_dag(
        context,
        build_full_dag(
            context,
            configs,
            group_by_cadence,
            target_configs=target_configs,
            rectangular=rectangular,
        ),
    )


def build_full_dag(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
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
                ),
                output="vectors",
                kind="dag_call",
                calls_dag=VECTOR_ASSEMBLE_DAG_NAME,
                child_dags=(
                    build_vector_dag(
                        context,
                        configs,
                        group_by_cadence,
                        target_configs,
                        rectangular,
                    ),
                ),
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
