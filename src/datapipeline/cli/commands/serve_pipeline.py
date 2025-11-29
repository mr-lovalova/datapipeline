from __future__ import annotations

import logging
import time
from itertools import islice
from typing import Iterator, Optional

from datapipeline.pipeline.observability import default_observer_registry
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.domain.sample import Sample
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_feature_pipeline, build_vector_pipeline
from datapipeline.pipeline.stages import post_process
from datapipeline.pipeline.split import apply_split_stage
from datapipeline.runtime import Runtime
from datapipeline.utils.window import resolve_window_bounds
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.protocols import Writer
from datapipeline.services.runs import finish_run_failed, finish_run_success, set_latest_run

logger = logging.getLogger(__name__)


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
    if target.transport == "stdout" and target.format in {"json-lines", "json", "jsonl"}:
        logger.info("(streamed %d items)", count)
        return
    logger.info("(printed %d items to stdout)", count)


def _is_full_pipeline_stage(stage: int | None) -> bool:
    return stage is None or stage >= 6


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

        rectangular = stage is None or stage > 5

        if stage is not None and stage <= 5:
            if target.payload != "sample":
                logger.warning(
                    "Ignoring payload '%s' for stage %s preview; preview outputs stream raw records.",
                    target.payload,
                    stage,
                )
            for cfg in preview_cfgs:
                stream = build_feature_pipeline(context, cfg, stage=stage)
                feature_target = target.for_feature(cfg.id)
                writer = writer_factory(
                    feature_target, visuals=visuals, item_type="record")
                count = serve_stream(stream, limit, writer=writer)
                report_serve(feature_target, count)
            run_status = "success"
            return

        if rectangular:
            runtime.window_bounds = resolve_window_bounds(runtime, True)

        vectors = build_vector_pipeline(
            context,
            feature_cfgs,
            dataset.group_by,
            target_configs=target_cfgs,
            rectangular=rectangular,
        )

        if stage in (None, 7):
            vectors = post_process(context, vectors)
        if stage is None:
            vectors = apply_split_stage(runtime, vectors)
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
