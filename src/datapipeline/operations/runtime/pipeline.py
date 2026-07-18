import logging
import time
from collections.abc import Iterator, Mapping
from functools import partial
from itertools import islice
from typing import TypeVar

from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.config.dataset.split import (
    DatasetFold,
    SplitConfig,
    resolve_fold_output,
)
from datapipeline.config.preview import PreviewStage
from datapipeline.domain.sample import Sample
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.runner import run_pipeline
from datapipeline.io.output import OutputTarget, output_destination_key
from datapipeline.operations.persistence import (
    RoutedRuntimeOutput,
    RuntimeOutput,
    RuntimeOutputBatch,
)
from datapipeline.pipelines.dataset.pipeline import (
    build_dataset_pipeline,
    run_fold_dataset_pipeline,
    run_dataset_pipeline,
    run_scaled_dataset_pipeline,
)
from datapipeline.pipelines.dataset.split import HashLabeler, TimeLabeler, build_labeler
from datapipeline.pipelines.variable.pipeline import run_variable_pipeline
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    Runtime,
    require_runtime_stream,
)
from datapipeline.utils.window import resolve_window_bounds

logger = logging.getLogger(__name__)
T = TypeVar("T")

_RECORD_PREVIEWS = {"input", "canonical", "records"}


def limit_items(items: Iterator[object], limit: int | None) -> Iterator[object]:
    if limit is None:
        yield from items
    else:
        yield from islice(items, limit)


def throttle_vectors(
    vectors: Iterator[Sample],
    throttle_ms: float | None,
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
    limit: int | None,
) -> RuntimeOutput:
    return RuntimeOutput(
        rows=limit_items(_managed_items(stream), limit),
        target=target,
    )


def _sample_output(
    stream: Iterator[Sample],
    target: OutputTarget,
    limit: int | None,
    throttle_ms: float | None,
) -> RuntimeOutput:
    return _runtime_output(throttle_vectors(stream, throttle_ms), target, limit)


def _preview_plan(
    preview_cfgs: list[VariableConfig],
    preview: PreviewStage,
) -> list[tuple[str, VariableConfig]]:
    if preview not in _RECORD_PREVIEWS:
        return [(cfg.id, cfg) for cfg in preview_cfgs]

    seen: set[str] = set()
    plan: list[tuple[str, VariableConfig]] = []
    for cfg in preview_cfgs:
        stream_id = cfg.stream
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
    stream = require_runtime_stream(context.runtime, stream_id)
    if preview == "input":
        if isinstance(stream, DerivedRuntimeStream):
            upstream = build_stream_pipeline(context, stream.input_stream)
            return run_pipeline(
                context,
                pipeline.through_node(upstream.node_count - 1),
            )
        return run_pipeline(context, pipeline.through_node(0))
    if preview == "canonical":
        if isinstance(stream, DerivedRuntimeStream):
            upstream = build_stream_pipeline(context, stream.input_stream)
            return run_pipeline(
                context,
                pipeline.through_node(upstream.node_count - 1),
            )
        if isinstance(stream, AlignedRuntimeStream):
            node_name = "combine_records"
        else:
            node_name = "map_records"
        return run_pipeline(context, pipeline.through_node_named(node_name))
    if preview == "records":
        return run_pipeline(context, pipeline)
    raise ValueError(f"Preview stage {preview!r} does not produce records")


def _serve_preview(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list[VariableConfig],
    target_cfgs: list[VariableConfig],
    cadence: str,
    sample_keys: list[str],
    limit: int | None,
    target: OutputTarget,
    throttle_ms: float | None,
    preview: PreviewStage,
) -> RuntimeOutputBatch:
    if preview in {"samples", "postprocess"}:
        runtime.window_bounds = resolve_window_bounds(runtime, True)
        dataset_pipeline = build_dataset_pipeline(
            context,
            feature_cfgs,
            cadence,
            target_configs=target_cfgs,
            rectangular=True,
            sample_keys=sample_keys,
        )
        selected_pipeline = (
            dataset_pipeline.through_node(0)
            if preview == "samples"
            else dataset_pipeline
        )
        sample_stream = run_pipeline(context, selected_pipeline)
        return RuntimeOutputBatch(
            outputs=(_sample_output(sample_stream, target, limit, throttle_ms),),
        )

    outputs: list[RuntimeOutput] = []
    preview_plan = _preview_plan(feature_cfgs + target_cfgs, preview)
    resolved_outputs: list[tuple[str, VariableConfig, OutputTarget]] = []
    destinations: dict[str, str] = {}
    for output_id, cfg in preview_plan:
        output_target = target.for_output(output_id)
        destination = output_target.destination
        if destination is not None:
            collision_key = output_destination_key(destination)
            if collision_key in destinations:
                first_id = destinations[collision_key]
                raise ValueError(
                    f"Preview outputs {first_id!r} and {output_id!r} resolve to "
                    f"the same destination: {destination}"
                )
            destinations[collision_key] = output_id
        resolved_outputs.append((output_id, cfg, output_target))

    for output_id, cfg, output_target in resolved_outputs:
        if preview in _RECORD_PREVIEWS:
            stream = _record_preview_stream(
                context,
                str(output_id),
                preview,
            )
        elif preview == "variables":
            stream = run_variable_pipeline(
                context,
                cfg,
                sample_keys=sample_keys,
                group_by_cadence=cadence,
            )
        else:
            raise ValueError(f"Unsupported preview stage: {preview!r}")
        outputs.append(_runtime_output(stream, output_target, limit))
    return RuntimeOutputBatch(outputs=tuple(outputs))


def _serve_dataset(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list[VariableConfig],
    target_cfgs: list[VariableConfig],
    cadence: str,
    sample_keys: list[str],
    limit: int | None,
    target: OutputTarget,
    throttle_ms: float | None,
) -> RuntimeOutputBatch:
    runtime.window_bounds = resolve_window_bounds(runtime, True)
    run = (
        run_scaled_dataset_pipeline
        if dataset_requires_scaler(runtime.dataset)
        else run_dataset_pipeline
    )
    vectors = run(
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


def _serve_fold_outputs(
    *,
    context: PipelineContext,
    runtime: Runtime,
    feature_cfgs: list[VariableConfig],
    target_cfgs: list[VariableConfig],
    cadence: str,
    sample_keys: list[str],
    output_ids: tuple[str, ...],
    limit: int | None,
    target: OutputTarget,
    throttle_ms: float | None,
) -> RuntimeOutputBatch:
    if target.transport != "fs":
        raise ValueError("Fold outputs require fs output.")
    split_cfg = runtime.dataset.split
    if split_cfg is None:
        raise ValueError("Fold outputs require dataset split configuration.")

    runtime.window_bounds = resolve_window_bounds(runtime, True)
    labeler = build_labeler(split_cfg)
    outputs: list[RoutedRuntimeOutput] = []
    for fold, labels_by_output in _selected_fold_outputs(split_cfg, output_ids):
        outputs_by_label = {
            label: output_id
            for output_id, labels in labels_by_output.items()
            for label in labels
        }
        samples = run_fold_dataset_pipeline(
            context,
            feature_cfgs,
            cadence,
            fold,
            outputs_by_label,
            target_configs=target_cfgs,
            rectangular=True,
            sample_keys=sample_keys,
        )
        rows = throttle_vectors(_managed_items(samples), throttle_ms)
        outputs.append(
            RoutedRuntimeOutput(
                rows=rows,
                targets={
                    output_id: target.for_output(output_id)
                    for output_id in labels_by_output
                },
                output_for_row=partial(
                    _fold_output_for_sample,
                    labeler,
                    outputs_by_label,
                ),
                limit_per_output=limit,
            )
        )
    return RuntimeOutputBatch(outputs=tuple(outputs))


def _fold_output_for_sample(
    labeler: HashLabeler | TimeLabeler,
    outputs_by_label: Mapping[str, str],
    sample: Sample,
) -> str | None:
    return outputs_by_label.get(labeler.label(sample.key))


def _selected_fold_outputs(
    split: SplitConfig,
    output_ids: tuple[str, ...],
) -> list[tuple[DatasetFold, dict[str, tuple[str, ...]]]]:
    selected: dict[
        str,
        tuple[DatasetFold, dict[str, tuple[str, ...]]],
    ] = {}
    for output_id in output_ids:
        fold, labels = resolve_fold_output(split, output_id)
        entry = selected.get(fold.id)
        if entry is None:
            outputs: dict[str, tuple[str, ...]] = {}
            selected[fold.id] = fold, outputs
        else:
            _, outputs = entry
        outputs[output_id] = labels
    return list(selected.values())


def run_pipeline_operation(
    runtime: Runtime,
    limit: int | None,
    target: OutputTarget,
    throttle_ms: float | None,
    preview: PreviewStage | None,
) -> RuntimeOutputBatch | None:
    dataset = runtime.dataset

    context = PipelineContext(runtime)
    feature_cfgs = list(dataset.features)
    target_cfgs = list(dataset.targets)
    if not feature_cfgs and not target_cfgs:
        logger.warning("(no features configured; nothing to serve)")
        return None
    cadence = dataset.sample.cadence

    output_ids = runtime.output_ids
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

    if dataset.split is not None:
        if not output_ids:
            raise ValueError("A split dataset requires at least one fold output.")
        return _serve_fold_outputs(
            context=context,
            runtime=runtime,
            feature_cfgs=feature_cfgs,
            target_cfgs=target_cfgs,
            cadence=cadence,
            sample_keys=dataset.sample.keys,
            output_ids=output_ids,
            limit=limit,
            target=target,
            throttle_ms=throttle_ms,
        )

    return _serve_dataset(
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
