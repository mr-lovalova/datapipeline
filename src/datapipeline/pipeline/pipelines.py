import heapq
from itertools import tee
from collections.abc import Iterator, Sequence
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.dag_stream import run_stage_dag
from datapipeline.execution.nodes.feature_nodes import (
    build_feature_nodes,
    build_record_nodes,
)
from datapipeline.execution.nodes.vector_nodes import (
    align_vectors_node,
    sample_assembly_node,
)
from datapipeline.execution.stage_dag import StageDag
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.stages import (
    align_stream,
    vector_assemble_stage,
    window_keys,
)
from datapipeline.pipeline.utils.keygen import group_key_for


def build_record_pipeline(
    context: PipelineContext,
    record_stream_id: str,
    stage: int | None = None,
) -> Iterator[Any]:
    dag = StageDag(
        name=f"record:{record_stream_id}",
        nodes=build_record_nodes(record_stream_id),
    ).upto_stage(stage)
    return run_stage_dag(context, dag)


def build_feature_pipeline(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    stage: int | None = None,
) -> Iterator[Any]:
    runtime = context.runtime
    record_stream_id = cfg.record_stream

    batch_size = runtime.registries.sort_batch_size.get(record_stream_id)
    partition_by = runtime.registries.partition_by.get(record_stream_id)
    dag = StageDag(
        name=f"feature:{cfg.id}",
        nodes=(
            *build_record_nodes(record_stream_id),
            *build_feature_nodes(
                cfg,
                batch_size=batch_size,
                partition_by=partition_by,
            ),
        ),
    )
    return run_stage_dag(context, dag.upto_stage(stage))


def build_vector_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    *,
    rectangular: bool = True,
) -> Iterator[Any]:
    """Build the vector assembly pipeline for features and optionally attach targets."""
    feature_cfgs = list(configs)
    target_cfgs = list(target_configs or [])
    if not feature_cfgs and not target_cfgs:
        return iter(())

    feature_vectors = _assemble_vectors(
        context,
        feature_cfgs,
        group_by_cadence,
    )

    keys_feature = None
    keys_target = None
    if rectangular:
        start, end = context.window_bounds(rectangular_required=True)
        keys = window_keys(start, end, group_by_cadence)
        if keys is not None:
            # share keys across feature/target alignment
            if target_cfgs:
                keys_feature, keys_target = tee(keys, 2)
            else:
                keys_feature = keys
    target_vectors = None
    if target_cfgs:
        target_vectors = _assemble_vectors(
            context,
            target_cfgs,
            group_by_cadence,
        )
        if keys_target is not None:
            target_vectors = align_stream(target_vectors, keys=keys_target)

    vector_dag = StageDag(
        name="vector:assemble",
        nodes=(
            align_vectors_node(keys=keys_feature, node_name="align_feature_vectors"),
            sample_assembly_node(target_vectors=target_vectors),
        ),
    )
    return run_stage_dag(context, vector_dag, seed=feature_vectors)


def _assemble_vectors(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
) -> Iterator[tuple[tuple, Vector]]:
    if not configs:
        return iter(())
    streams = [
        build_feature_pipeline(
            context,
            cfg,
        )
        for cfg in configs
    ]

    merged = heapq.merge(
        *streams, key=lambda fr: group_key_for(fr, group_by_cadence)
    )
    return vector_assemble_stage(merged, group_by_cadence)
