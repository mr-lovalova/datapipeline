from typing import Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import ThresholdsOptions, ThresholdsTask
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime

from .vector_stats_common import (
    metrics_summary_output,
)


def inspect_thresholds_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target: OutputTarget | None = None,
    throttle_ms: Optional[float] = None,
    preview_index: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: ThresholdsTask | None = None,
) -> RuntimeOutput:
    _ = dataset, limit, target, throttle_ms, preview_index, visuals
    options = operation_task.options if operation_task else ThresholdsOptions()
    return metrics_summary_output(
        runtime,
        report="thresholds",
        sort_key=options.sort,
        threshold=options.threshold,
        include_threshold=True,
    )
