import logging
import time
from itertools import islice
from typing import Iterator, Optional, TypeVar

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.config.preview import PreviewStage
from datapipeline.config.tasks import PipelineTask
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.runner import run_pipeline
from datapipeline.domain.sample import Sample
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import (
    RuntimeOutput,
    RuntimeOutputBatch,
    SplitRuntimeOutput,
)
from datapipeline.pipelines.feature.pipeline import run_feature_pipeline
from datapipeline.pipelines.full.pipeline import (
    build_full_pipeline,
    run_full_pipeline,
)
from datapipeline.pipelines.full.split import build_labeler
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import AlignedRuntimeStream, Runtime, require_runtime_stream
from datapipeline.utils.window import resolve_window_bounds

logger = logging.getLogger(__name__)
T = TypeVar("T")

_RECORD_PREVIEWS = {"source", "mapped", "records"}


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


def _managed_items(stream: Iterator[T]) -> Iterator[T]:
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


def _preview_plan(
    preview_cfgs: list[FeatureRecordConfig],
    preview: PreviewStage,
) -> list[tuple[str, FeatureRecordConfig]]:
    if preview not in _RECORD_PREVIEWS:
        return [(cfg.id, cfg) for cfg in preview_cfgs]

    seen: set[str] = set()
    plan: list[tuple[str, FeatureRecordConfig]] = []
    for cfg in preview_cfgs:
        stream_id = cfg.record_stream
        if stream_id in seen:
            continue
        seen.add(stream_id)
        plan.append((stream_id, cfg))
    return plan


def _record_preview_stream(
    context: PipelineContext,
    stream_id: str,
    preview: PreviewStage,
) -> Iterator[object]:
    pipeline = build_stream_pipeline(context, stream_id)
    if preview == "source":
        return run_pipeline(context, pipeline.through_node(0))
    if preview == "mapped":
        stream = require_runtime_stream(context.runtime, stream_id)
        node_name = (
            "combine_records"
            if isinstance(stream, AlignedRuntimeStream)
            else "map_records"
        )
        return run_pipeline(context, pipeline.through_node_named(node_name))
    if preview == "records":
        return run_pipeline(context, pipeline)
    raise ValueError(f"Preview stage {preview!r} does not produce records")


def _serve_preview(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list[FeatureRecordConfig],
    target_cfgs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
    preview: PreviewStage,
) -> RuntimeOutputBatch:
    if preview in {"samples", "postprocess"}:
        runtime.window_bounds = resolve_window_bounds(runtime, True)
        full_pipeline = build_full_pipeline(
            context,
            feature_cfgs,
            cadence,
            target_configs=target_cfgs,
            rectangular=True,
            sample_keys=sample_keys,
        )
        selected_pipeline = (
            full_pipeline.through_node(0) if preview == "samples" else full_pipeline
        )
        sample_stream = run_pipeline(context, selected_pipeline)
        return RuntimeOutputBatch(
            outputs=(_sample_output(sample_stream, target, limit, throttle_ms),),
        )

    outputs: list[RuntimeOutput] = []
    preview_plan = _preview_plan(feature_cfgs + target_cfgs, preview)
    for output_id, cfg in preview_plan:
        if preview in _RECORD_PREVIEWS:
            stream = _record_preview_stream(
                context,
                str(output_id),
                preview,
            )
        elif preview == "features":
            stream = run_feature_pipeline(
                context,
                cfg,
                sample_keys=sample_keys,
                group_by_cadence=cadence,
            )
        else:
            raise ValueError(f"Unsupported preview stage: {preview!r}")
        outputs.append(_runtime_output(stream, target.for_feature(output_id), limit))
    return RuntimeOutputBatch(outputs=tuple(outputs))


def _serve_full(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list[FeatureRecordConfig],
    target_cfgs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
) -> RuntimeOutputBatch:
    runtime.window_bounds = resolve_window_bounds(runtime, True)
    vectors = run_full_pipeline(
        context,
        feature_cfgs,
        cadence,
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
    feature_cfgs: list[FeatureRecordConfig],
    target_cfgs: list[FeatureRecordConfig],
    cadence: str,
    sample_keys: list[str],
    split_labels: list[str],
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
) -> RuntimeOutputBatch:
    if target.transport != "fs":
        raise ValueError("serve splits require fs output")
    split_cfg = runtime.split
    if split_cfg is None:
        raise ValueError("serve splits require project split configuration")

    runtime.window_bounds = resolve_window_bounds(runtime, True)
    vectors = run_full_pipeline(
        context,
        feature_cfgs,
        cadence,
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
    preview: PreviewStage | None,
    visuals: Optional[str] = None,
    operation_task: PipelineTask | None = None,
) -> RuntimeOutputBatch | None:
    _ = operation_task, visuals
    validate_dataset_feature_identity(runtime, dataset)

    context = PipelineContext(runtime)
    feature_cfgs = list(dataset.features)
    target_cfgs = list(dataset.targets)
    if not feature_cfgs and not target_cfgs:
        logger.warning("(no features configured; nothing to serve)")
        return None
    cadence = dataset.sample.cadence

    split_labels = list(runtime.split_labels)
    if split_labels:
        if preview is not None:
            raise ValueError("serve splits do not support previews")
        return _serve_split_outputs(
            context=context,
            runtime=runtime,
            feature_cfgs=feature_cfgs,
            target_cfgs=target_cfgs,
            cadence=cadence,
            sample_keys=dataset.sample.keys,
            split_labels=split_labels,
            limit=limit,
            target=target,
            throttle_ms=throttle_ms,
        )

    if preview is not None:
        return _serve_preview(
            context=context,
            runtime=runtime,
            feature_cfgs=feature_cfgs,
            target_cfgs=target_cfgs,
            cadence=cadence,
            sample_keys=dataset.sample.keys,
            limit=limit,
            target=target,
            throttle_ms=throttle_ms,
            preview=preview,
        )
    return _serve_full(
        context=context,
        runtime=runtime,
        feature_cfgs=feature_cfgs,
        target_cfgs=target_cfgs,
        cadence=cadence,
        sample_keys=dataset.sample.keys,
        limit=limit,
        target=target,
        throttle_ms=throttle_ms,
    )
