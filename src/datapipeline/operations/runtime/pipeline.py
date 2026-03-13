import logging
import time
from itertools import islice
from typing import Iterator, Optional

from datapipeline.pipelines.record.nodes import RECORD_NODE_COUNT
from datapipeline.dag.transform_observability import default_observer_registry
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.domain.sample import Sample
from datapipeline.dag.context import PipelineContext
from datapipeline.operations.persistence import RuntimeOutput, RuntimeOutputBatch
from datapipeline.pipelines import build_feature_pipeline, build_full_pipeline
from datapipeline.runtime import Runtime
from datapipeline.utils.window import resolve_window_bounds
from datapipeline.io.output import OutputTarget
from datapipeline.services.runs import finish_run_failed, finish_run_success, set_latest_run

logger = logging.getLogger(__name__)


def _preview_plan(
    preview_cfgs: list,
    stage: int,
) -> list[tuple[str, object]]:
    """Plan preview outputs for stage-index based serve.

    Early indices map to record nodes and are record-stream scoped.
    Later indices map to feature nodes and are feature scoped.
    """
    if stage <= (RECORD_NODE_COUNT - 1):
        seen: set[str] = set()
        plan: list[tuple[str, object]] = []
        for cfg in preview_cfgs:
            stream_id = cfg.record_stream
            if stream_id in seen:
                continue
            seen.add(stream_id)
            plan.append((stream_id, cfg))
        return plan

    return [(cfg.id, cfg) for cfg in preview_cfgs]


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


def _is_full_pipeline_stage(stage: int | None) -> bool:
    return stage is None


def serve_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int],
    target: OutputTarget,
    throttle_ms: Optional[float],
    stage: Optional[int],
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> RuntimeOutputBatch | None:
    _ = operation_task, visuals
    run_paths = target.run

    def _finish_run(success: bool) -> None:
        if run_paths is None:
            return
        if success:
            finish_run_success(run_paths)
            if _is_full_pipeline_stage(stage):
                set_latest_run(run_paths)
            return
        finish_run_failed(run_paths)

    try:
        context = PipelineContext(
            runtime,
            observer_registry=default_observer_registry(),
        )

        feature_cfgs = list(dataset.features or [])
        target_cfgs = list(dataset.targets or [])
        preview_cfgs = feature_cfgs + target_cfgs

        if not preview_cfgs:
            logger.warning("(no features configured; nothing to serve)")
            _finish_run(True)
            return

        if stage is not None:
            outputs: list[RuntimeOutput] = []
            for output_id, cfg in _preview_plan(preview_cfgs, stage):
                stream = build_feature_pipeline(context, cfg, stage=stage)
                feature_target = target.for_feature(output_id)
                outputs.append(
                    RuntimeOutput(
                        rows=limit_items(_managed_items(stream), limit),
                        target=feature_target,
                    )
                )
            return RuntimeOutputBatch(outputs=tuple(outputs), on_complete=_finish_run)

        runtime.window_bounds = resolve_window_bounds(runtime, True)

        vectors = build_full_pipeline(
            context,
            feature_cfgs,
            dataset.group_by,
            target_configs=target_cfgs,
            rectangular=True,
        )
        return RuntimeOutputBatch(
            outputs=(
                RuntimeOutput(
                    rows=limit_items(
                        _managed_items(throttle_vectors(vectors, throttle_ms)),
                        limit,
                    ),
                    target=target,
                ),
            ),
            on_complete=_finish_run,
        )
    except KeyboardInterrupt:
        _finish_run(False)
        raise
    except Exception:
        _finish_run(False)
        raise
