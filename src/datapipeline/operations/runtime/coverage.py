from typing import Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime

from .vector_stats_common import (
    metrics_summary_output,
)


def inspect_coverage_with_runtime(
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
    return metrics_summary_output(
        runtime,
        operation_task,
        report="coverage",
        include_sort=True,
    )
