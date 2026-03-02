from typing import Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.io.output import OutputTarget
from datapipeline.runtime import Runtime

from .vector_stats_common import option, options_for_task, print_thresholds, resolve_summary


def inspect_thresholds_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target: OutputTarget | None = None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> None:
    _ = dataset, limit, target, throttle_ms, stage, visuals
    options = options_for_task(operation_task)
    summary, _sort_key, threshold = resolve_summary(runtime, options=options)
    if not bool(option(options, "quiet", False)):
        print_thresholds(summary, threshold=threshold)
