from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from datapipeline.execution.nodes.spec import PipelineNode
from datapipeline.pipeline.stages import (
    align_stream,
    sample_assemble_stage,
    vector_assemble_stage,
)


def vector_assembly_node(group_by_cadence: str) -> PipelineNode:
    def _run(_context, merged_features: Iterable[Any] | None):
        return vector_assemble_stage(
            iter(()) if merged_features is None else iter(merged_features),
            group_by_cadence,
        )

    return PipelineNode(name="vector_assembly", run=_run)


def align_vectors_node(
    *,
    keys: Iterator[tuple] | None,
    node_name: str = "align_vectors",
) -> PipelineNode:
    def _run(_context, vectors: Iterable[Any] | None):
        if vectors is None:
            return iter(())
        if keys is None:
            return iter(vectors)
        return align_stream(iter(vectors), keys=keys)

    return PipelineNode(name=node_name, run=_run)


def sample_assembly_node(
    *,
    target_vectors: Iterator[tuple[tuple, Any]] | None = None,
) -> PipelineNode:
    def _run(_context, feature_vectors: Iterable[Any] | None):
        return sample_assemble_stage(
            iter(()) if feature_vectors is None else iter(feature_vectors),
            target_vectors=target_vectors,
        )

    return PipelineNode(name="sample_assembly", run=_run)
