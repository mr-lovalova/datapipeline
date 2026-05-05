import logging
import time
from dataclasses import dataclass
from itertools import islice
from typing import Iterator, Literal, Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.runner import run_dag
from datapipeline.dag.transform_observability import default_observer_registry
from datapipeline.domain.sample import Sample
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput, RuntimeOutputBatch
from datapipeline.pipelines import (
    build_feature_pipeline,
    build_full_dag,
    build_full_pipeline,
)
from datapipeline.runtime import Runtime
from datapipeline.utils.window import resolve_window_bounds

logger = logging.getLogger(__name__)
PreviewScope = Literal["record", "feature", "sample"]


@dataclass(frozen=True)
class PreviewNode:
    name: str
    scope: PreviewScope
    serve_node_index: int | None = None


SERVE_PREVIEW_NODES = (
    PreviewNode("open_source", "record"),
    PreviewNode("map_records", "record"),
    PreviewNode("record_transforms", "record"),
    PreviewNode("order_records", "record"),
    PreviewNode("stream_transforms", "record"),
    PreviewNode("debug_transforms", "record"),
    PreviewNode("build_feature_stream", "feature"),
    PreviewNode("feature_transforms", "feature"),
    PreviewNode("order_feature_records", "feature"),
    PreviewNode("sample_assembly", "sample", serve_node_index=0),
    PreviewNode("post_process", "sample", serve_node_index=1),
    PreviewNode("split", "sample", serve_node_index=2),
)


def limit_items(items: Iterator[object], limit: Optional[int]) -> Iterator[object]:
    if limit is None:
        yield from items
    else:
        yield from islice(items, limit)


def throttle_vectors(
    vectors: Iterator[Sample],
    throttle_ms: Optional[float],
) -> Iterator[Sample]:
    if not throttle_ms or throttle_ms <= 0:
        yield from vectors
        return
    delay = throttle_ms / 1000.0
    for item in vectors:
        yield item
        time.sleep(delay)


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()


def _managed_items(stream: Iterator[object]) -> Iterator[object]:
    try:
        yield from stream
    finally:
        _close_iterator(stream)


def _runtime_output(
    stream: Iterator[object],
    target: OutputTarget,
    limit: Optional[int],
) -> RuntimeOutput:
    return RuntimeOutput(
        rows=limit_items(_managed_items(stream), limit),
        target=target,
    )


def _sample_output(
    stream: Iterator[Sample],
    target: OutputTarget,
    limit: Optional[int],
    throttle_ms: Optional[float],
) -> RuntimeOutput:
    return _runtime_output(throttle_vectors(stream, throttle_ms), target, limit)


def _preview_node(preview_index: int) -> PreviewNode:
    if preview_index < 0 or preview_index >= len(SERVE_PREVIEW_NODES):
        raise ValueError(
            f"preview_index must be between 0 and {len(SERVE_PREVIEW_NODES) - 1}"
        )
    return SERVE_PREVIEW_NODES[preview_index]


def _preview_plan(
    preview_cfgs: list,
    selected_node: PreviewNode,
) -> list[tuple[str, object]]:
    if selected_node.scope != "record":
        return [(cfg.id, cfg) for cfg in preview_cfgs]

    seen: set[str] = set()
    plan: list[tuple[str, object]] = []
    for cfg in preview_cfgs:
        stream_id = cfg.record_stream
        if stream_id in seen:
            continue
        seen.add(stream_id)
        plan.append((stream_id, cfg))
    return plan


def _serve_preview(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list,
    target_cfgs: list,
    group_by: str,
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
    preview_index: int,
) -> RuntimeOutputBatch:
    selected_node = _preview_node(preview_index)
    if selected_node.scope == "sample":
        runtime.window_bounds = resolve_window_bounds(runtime, True)
        full_dag = build_full_dag(
            context,
            feature_cfgs,
            group_by,
            target_configs=target_cfgs,
            rectangular=True,
        )
        if selected_node.serve_node_index is None:
            raise ValueError(
                f"Preview node '{selected_node.name}' is not a sample node."
            )
        stream = run_dag(
            context,
            full_dag.upto_node(selected_node.serve_node_index),
        )
        return RuntimeOutputBatch(
            outputs=(_sample_output(stream, target, limit, throttle_ms),),
        )

    outputs: list[RuntimeOutput] = []
    for output_id, cfg in _preview_plan(feature_cfgs + target_cfgs, selected_node):
        stream = build_feature_pipeline(context, cfg, node=preview_index)
        outputs.append(_runtime_output(stream, target.for_feature(output_id), limit))
    return RuntimeOutputBatch(outputs=tuple(outputs))


def _serve_full(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list,
    target_cfgs: list,
    group_by: str,
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
) -> RuntimeOutputBatch:
    runtime.window_bounds = resolve_window_bounds(runtime, True)
    vectors = build_full_pipeline(
        context,
        feature_cfgs,
        group_by,
        target_configs=target_cfgs,
        rectangular=True,
    )
    return RuntimeOutputBatch(
        outputs=(_sample_output(vectors, target, limit, throttle_ms),),
    )


def serve_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
    preview_index: Optional[int],
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> RuntimeOutputBatch | None:
    _ = operation_task, visuals

    context = PipelineContext(
        runtime,
        observer_registry=default_observer_registry(),
    )
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or [])
    if not feature_cfgs and not target_cfgs:
        logger.warning("(no features configured; nothing to serve)")
        return

    if preview_index is not None:
        return _serve_preview(
            context=context,
            runtime=runtime,
            feature_cfgs=feature_cfgs,
            target_cfgs=target_cfgs,
            group_by=dataset.group_by,
            limit=limit,
            target=target,
            throttle_ms=throttle_ms,
            preview_index=preview_index,
        )
    return _serve_full(
        context=context,
        runtime=runtime,
        feature_cfgs=feature_cfgs,
        target_cfgs=target_cfgs,
        group_by=dataset.group_by,
        limit=limit,
        target=target,
        throttle_ms=throttle_ms,
    )
