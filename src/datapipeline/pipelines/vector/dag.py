import heapq
import logging
from collections.abc import Iterator, Sequence
from itertools import tee
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.vector import Vector
from datapipeline.dag.dag import StageDag
from datapipeline.dag.runner import run_stage_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.feature.dag import build_feature_pipeline
from datapipeline.pipelines.vector.nodes import (
    align_stream,
    sample_assemble_stage,
    vector_assemble_stage,
    window_keys,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.vector.keygen import group_key_for

logger = logging.getLogger(__name__)


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
            PipelineNode(
                name="align_feature_vectors",
                op=align_stream,
                input="seed",
                kwargs={"keys": keys_feature},
            ),
            PipelineNode(
                name="sample_assembly",
                op=sample_assemble_stage,
                input="align_feature_vectors",
                kwargs={"target_vectors": target_vectors},
            ),
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

    return vector_assemble_stage(_merged_feature_streams(), group_by_cadence)
