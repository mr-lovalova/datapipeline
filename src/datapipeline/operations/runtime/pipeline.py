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
from datapipeline.operations.persistence import (
    RuntimeOutput,
    RuntimeOutputBatch,
    SplitRuntimeOutput,
)
from datapipeline.pipelines import (
    build_feature_pipeline,
    build_full_dag,
    build_full_pipeline,
    build_stream_id_dag,
    build_stream_id_pipeline,
)
from datapipeline.pipelines.full.split import build_labeler
from datapipeline.runtime import Runtime, StreamPipelineKind
from datapipeline.utils.window import resolve_window_bounds

logger = logging.getLogger(__name__)
PreviewScope = Literal["record", "feature", "sample"]


@dataclass(frozen=True)
class PreviewNode:
    name: str
    scope: PreviewScope
    stream_pipeline: StreamPipelineKind | None = None
    record_node_index: int | None = None
    feature_node_index: int | None = None
    serve_node_index: int | None = None


SERVE_PREVIEW_NODES = (
    PreviewNode("ingest:open_source", "record", "ingest", 0),
    PreviewNode("ingest:map_records", "record", "ingest", 1),
    PreviewNode("ingest:record_transforms", "record", "ingest", 2),
    PreviewNode("ingest:order_records", "record", "ingest", 3),
    PreviewNode("stream:open_records", "record", "stream", 0),
    PreviewNode("stream:map_records", "record", "stream", 1),
    PreviewNode("stream:order_records", "record", "stream", 2),
    PreviewNode("stream:stream_transforms", "record", "stream", 3),
    PreviewNode("stream:debug_transforms", "record", "stream", 4),
    PreviewNode("build_feature_stream", "feature", feature_node_index=0),
    PreviewNode("feature_transforms", "feature", feature_node_index=1),
    PreviewNode("order_feature_records", "feature", feature_node_index=2),
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


def _stream_pipeline_kind(runtime: Runtime, stream_id: str) -> StreamPipelineKind:
    return runtime.registries.stream_specs.get(stream_id).pipeline


def _record_preview_index_range(pipeline: StreamPipelineKind) -> str:
    if pipeline == "ingest":
        return "0-3"
    return "4-8"


def _validate_record_preview_streams(
    runtime: Runtime,
    stream_ids: list[str],
    selected_node: PreviewNode,
    preview_index: int,
) -> None:
    if selected_node.stream_pipeline is None:
        raise ValueError(f"Preview node '{selected_node.name}' is not a record node.")

    mismatches: list[tuple[str, StreamPipelineKind]] = []
    for stream_id in stream_ids:
        actual_kind = _stream_pipeline_kind(runtime, stream_id)
        if actual_kind != selected_node.stream_pipeline:
            mismatches.append((stream_id, actual_kind))

    if not mismatches:
        return

    expected_range = _record_preview_index_range(selected_node.stream_pipeline)
    actual_ranges = sorted(
        {_record_preview_index_range(actual_kind) for _, actual_kind in mismatches}
    )
    mismatch_text = ", ".join(
        f"'{stream_id}' is a {actual_kind} stream"
        for stream_id, actual_kind in mismatches
    )
    raise ValueError(
        f"Preview index {preview_index} ('{selected_node.name}') applies to "
        f"{selected_node.stream_pipeline} streams ({expected_range}), but "
        f"{mismatch_text}. Use preview indices {' or '.join(actual_ranges)} "
        "for those record streams, or 9-14 for feature/sample previews."
    )


def _record_preview_stream(
    context: PipelineContext,
    runtime: Runtime,
    stream_id: str,
    selected_node: PreviewNode,
) -> Iterator[object]:
    if selected_node.stream_pipeline is None or selected_node.record_node_index is None:
        raise ValueError(f"Preview node '{selected_node.name}' is not a record node.")
    actual_kind = _stream_pipeline_kind(runtime, stream_id)
    if actual_kind != selected_node.stream_pipeline:
        raise ValueError(
            f"Preview node '{selected_node.name}' applies to "
            f"{selected_node.stream_pipeline} "
            f"streams, but '{stream_id}' is a {actual_kind} stream."
        )
    return build_stream_id_pipeline(
        context,
        stream_id,
        node=selected_node.record_node_index,
    )


def _feature_preview_index(
    context: PipelineContext,
    stream_id: str,
    selected_node: PreviewNode,
) -> int:
    if selected_node.feature_node_index is None:
        raise ValueError(f"Preview node '{selected_node.name}' is not a feature node.")
    record_dag = build_stream_id_dag(context, stream_id)
    return record_dag.node_count + selected_node.feature_node_index


def _serve_preview(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list,
    target_cfgs: list,
    group_by: str,
    sample_keys: list[str],
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
            sample_keys=sample_keys,
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
    preview_plan = _preview_plan(feature_cfgs + target_cfgs, selected_node)
    if selected_node.scope == "record":
        _validate_record_preview_streams(
            runtime,
            [str(output_id) for output_id, _ in preview_plan],
            selected_node,
            preview_index,
        )
    for output_id, cfg in preview_plan:
        if selected_node.scope == "record":
            stream = _record_preview_stream(
                context,
                runtime,
                str(output_id),
                selected_node,
            )
        else:
            stream = build_feature_pipeline(
                context,
                cfg,
                node=_feature_preview_index(context, cfg.record_stream, selected_node),
                sample_keys=sample_keys,
                group_by_cadence=group_by,
            )
        outputs.append(_runtime_output(stream, target.for_feature(output_id), limit))
    return RuntimeOutputBatch(outputs=tuple(outputs))


def _serve_full(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list,
    target_cfgs: list,
    group_by: str,
    sample_keys: list[str],
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
        sample_keys=sample_keys,
    )
    return RuntimeOutputBatch(
        outputs=(_sample_output(vectors, target, limit, throttle_ms),),
    )


def _serve_split_outputs(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list,
    target_cfgs: list,
    group_by: str,
    sample_keys: list[str],
    split_labels: list[str],
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
) -> RuntimeOutputBatch:
    if target.transport != "fs":
        raise ValueError("serve splits require fs output")
    split_cfg = getattr(runtime, "split", None)
    if split_cfg is None:
        raise ValueError("serve splits require project split configuration")

    runtime.window_bounds = resolve_window_bounds(runtime, True)
    vectors = build_full_pipeline(
        context,
        feature_cfgs,
        group_by,
        target_configs=target_cfgs,
        rectangular=True,
        sample_keys=sample_keys,
    )
    labeler = build_labeler(split_cfg)
    rows = throttle_vectors(_managed_items(vectors), throttle_ms)
    targets = {label: target.for_split(label) for label in split_labels}

    return RuntimeOutputBatch(
        outputs=(
            SplitRuntimeOutput(
                rows=rows,
                targets=targets,
                label_for_row=lambda sample: labeler.label(sample.key, sample.features),
                limit_per_target=limit,
            ),
        ),
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
    runtime.sample_keys = dataset.sample_keys
    if not feature_cfgs and not target_cfgs:
        logger.warning("(no features configured; nothing to serve)")
        return

    split_labels = list(getattr(getattr(runtime, "run", None), "splits", None) or [])
    if split_labels:
        if preview_index is not None:
            raise ValueError("serve splits do not support preview indices")
        return _serve_split_outputs(
            context=context,
            runtime=runtime,
            feature_cfgs=feature_cfgs,
            target_cfgs=target_cfgs,
            group_by=dataset.group_by,
            sample_keys=dataset.sample_keys,
            split_labels=split_labels,
            limit=limit,
            target=target,
            throttle_ms=throttle_ms,
        )

    if preview_index is not None:
        return _serve_preview(
            context=context,
            runtime=runtime,
            feature_cfgs=feature_cfgs,
            target_cfgs=target_cfgs,
            group_by=dataset.group_by,
            sample_keys=dataset.sample_keys,
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
        sample_keys=dataset.sample_keys,
        limit=limit,
        target=target,
        throttle_ms=throttle_ms,
    )
