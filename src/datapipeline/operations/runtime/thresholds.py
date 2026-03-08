from typing import Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime

from .vector_stats_common import (
    options_for_task,
    report_payload,
    resolve_metrics,
    write_summary_output,
)


def inspect_thresholds_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target: OutputTarget | None = None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> RuntimeOutput:
    _ = dataset, limit, target, throttle_ms, stage, visuals
    options = options_for_task(operation_task)
    metrics, _sort_key, threshold = resolve_metrics(runtime, options=options)
    payload = report_payload(
        report="thresholds",
        metrics=metrics,
        threshold=threshold,
    )
    return write_summary_output(payload)
