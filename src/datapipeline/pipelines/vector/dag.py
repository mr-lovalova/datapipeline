import heapq
import logging
from collections.abc import Iterator, Sequence
from itertools import tee
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.vector import Vector
from datapipeline.dag.dag import Dag
from datapipeline.dag.runner import run_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.feature.dag import build_feature_dag, build_feature_pipeline
from datapipeline.pipelines.vector.nodes import (
    align_stream,
    sample_assemble_stage,
    vector_assemble_stage,
    window_keys,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.vector.keygen import group_key_for

logger = logging.getLogger(__name__)
VECTOR_ASSEMBLE_DAG_NAME = "vector:assemble"


def _close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if not callable(closer):
        return
    try:
        closer()
    except Exception:
        logger.debug("Failed to close vector iterator during teardown", exc_info=True)


def build_vector_pipeline(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
) -> Iterator[Any]:
    feature_cfgs = list(configs)
    target_cfgs = list(target_configs or [])
    if not feature_cfgs and not target_cfgs:
        return iter(())

    return run_dag(
        context,
        build_vector_dag(
            context,
            feature_cfgs,
            group_by_cadence,
            target_configs=target_cfgs,
            rectangular=rectangular,
        ),
    )


def build_vector_dag(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    target_configs: Sequence[FeatureRecordConfig] | None = None,
    rectangular: bool = True,
) -> Dag:
    feature_cfgs = list(configs)
    target_cfgs = list(target_configs or [])
    keys_feature = None
    keys_target = None
    if rectangular:
        start, end = context.window_bounds(rectangular_required=True)
        keys = window_keys(start, end, group_by_cadence)
        if keys is not None:
            if target_cfgs:
                keys_feature, keys_target = tee(keys, 2)
            else:
                keys_feature = keys

    nodes = [
        PipelineNode(
            name="feature_fanout",
            op=_assemble_vectors,
            args=(context, feature_cfgs, group_by_cadence, "Assembling feature vectors"),
            output="feature_vectors",
            kind="dag_fanout",
            calls_dag="feature:*",
            child_dags=tuple(build_feature_dag(context, cfg) for cfg in feature_cfgs),
        ),
        PipelineNode(
            name="align_feature_vectors",
            op=align_stream,
            input="feature_vectors",
            output="aligned_feature_vectors",
            kwargs={"keys": keys_feature},
        ),
    ]
    sample_kwinputs: dict[str, str] = {}
    if target_cfgs:
        nodes.extend(
            [
                PipelineNode(
                    name="target_fanout",
                    op=_assemble_vectors,
                    args=(
                        context,
                        target_cfgs,
                        group_by_cadence,
                        "Assembling target vectors",
                    ),
                    output="target_vectors",
                    kind="dag_fanout",
                    calls_dag="feature:*",
                    child_dags=tuple(
                        build_feature_dag(context, cfg) for cfg in target_cfgs
                    ),
                ),
                PipelineNode(
                    name="align_target_vectors",
                    op=align_stream,
                    input="target_vectors",
                    output="aligned_target_vectors",
                    kwargs={"keys": keys_target},
                ),
            ]
        )
        sample_kwinputs["target_vectors"] = "aligned_target_vectors"
    return Dag(
        name=VECTOR_ASSEMBLE_DAG_NAME,
        nodes=(
            *nodes,
            PipelineNode(
                name="sample_assembly",
                op=sample_assemble_stage,
                input="aligned_feature_vectors",
                kwinputs=sample_kwinputs,
            ),
        ),
    )


def _assemble_vectors(
    context: PipelineContext,
    configs: Sequence[FeatureRecordConfig],
    group_by_cadence: str,
    progress_stage: str = "Assembling vectors",
) -> Iterator[tuple[tuple, Vector]]:
    if not configs:
        return iter(())

    def _merged_feature_streams() -> Iterator[Any]:
        streams = [build_feature_pipeline(context, cfg) for cfg in configs]
        if not streams:
            return
        merged_stream = heapq.merge(
            *streams,
            key=lambda fr: group_key_for(fr, group_by_cadence),
        )
        try:
            yield from merged_stream
        finally:
            _close_iterator(merged_stream)
            for stream in streams:
                _close_iterator(stream)

    return vector_assemble_stage(
        _merged_feature_streams(),
        group_by_cadence,
        progress_stage=progress_stage,
    )
