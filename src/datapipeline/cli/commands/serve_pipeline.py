import logging
import time
from itertools import islice
from typing import Iterator, Optional

from datapipeline.pipelines.record.nodes import RECORD_NODE_COUNT
from datapipeline.dag.transform_observability import default_observer_registry
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.domain.sample import Sample
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines import build_feature_pipeline, build_full_pipeline
from datapipeline.runtime import Runtime
from datapipeline.utils.window import resolve_window_bounds
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.protocols import Writer
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


def serve_stream(
    items: Iterator[object],
    limit: Optional[int],
    writer: Writer,
) -> int:
    count = 0
    try:
        for item in limit_items(items, limit):
            writer.write(item)
            count += 1
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
    return count


def report_serve(target: OutputTarget, count: int) -> None:
    if target.destination:
        logger.info("Saved %d items to %s", count, target.destination)
        return
    if target.transport == "stdout" and target.format == "jsonl":
        logger.info("(streamed %d items)", count)
        return
    logger.info("(printed %d items to stdout)", count)


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
) -> None:
    run_paths = target.run
    run_status: str | None = None
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
            run_status = "success"
            return

        if stage is not None:
            for output_id, cfg in _preview_plan(preview_cfgs, stage):
                stream = build_feature_pipeline(context, cfg, stage=stage)
                feature_target = target.for_feature(output_id)
                writer = writer_factory(
                    feature_target, visuals=visuals, item_type="record")
                count = serve_stream(stream, limit, writer=writer)
                report_serve(feature_target, count)
            run_status = "success"
            return

        runtime.window_bounds = resolve_window_bounds(runtime, True)

        vectors = build_full_pipeline(
            context,
            feature_cfgs,
            dataset.group_by,
            target_configs=target_cfgs,
            rectangular=True,
        )
        vectors = throttle_vectors(vectors, throttle_ms)

        writer = writer_factory(target, visuals=visuals)

        result_count = serve_stream(vectors, limit, writer=writer)
        report_serve(target, result_count)
        run_status = "success"
    except KeyboardInterrupt:
        logger.info("Serve interrupted by user")
        run_status = "failed"
    except Exception:
        run_status = "failed"
        raise
    finally:
        if run_paths is not None and run_status is not None:
            if run_status == "success":
                finish_run_success(run_paths)
                if _is_full_pipeline_stage(stage):
                    set_latest_run(run_paths)
            else:
                finish_run_failed(run_paths)
