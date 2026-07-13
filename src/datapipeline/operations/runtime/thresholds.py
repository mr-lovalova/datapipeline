from typing import Optional

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.config.tasks import ThresholdsOptions, ThresholdsTask
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime

from .vector_stats_common import build_metrics, load_collector


def inspect_thresholds_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target: OutputTarget | None = None,
    throttle_ms: Optional[float] = None,
    preview: PreviewStage | None = None,
    visuals: Optional[str] = None,
    operation_task: ThresholdsTask | None = None,
) -> RuntimeOutput:
    _ = dataset, limit, target, throttle_ms, preview, visuals
    options = operation_task.options if operation_task else ThresholdsOptions()
    collector = load_collector(runtime)
    metrics = build_metrics(
        collector,
        sort_key=options.sort,
        threshold=options.threshold,
    )
    return RuntimeOutput(
        payload={
            "report": "thresholds",
            "metrics": metrics,
            "threshold": options.threshold,
        }
    )
