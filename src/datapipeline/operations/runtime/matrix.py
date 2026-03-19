from pathlib import Path
from typing import Optional

from datapipeline.analysis.vector.matrix import export_matrix_data
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime

from .vector_stats_common import (
    load_collector,
    matrix_status_rows,
    options_for_task,
)

def inspect_matrix_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target: OutputTarget | None = None,
    throttle_ms: Optional[float] = None,
    step: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> RuntimeOutput:
    _ = dataset, limit, target, throttle_ms, step, visuals
    options = options_for_task(operation_task)
    collector = load_collector(runtime, options=options)
    rows = matrix_status_rows(collector)

    def _render_html(destination: Path) -> Path | None:
        collector.matrix_format = "html"
        collector.matrix_output = str(destination)
        return export_matrix_data(collector)

    return RuntimeOutput(
        rows=rows,
        html_renderer=_render_html,
        materialized_key="inspect_matrix",
        materialized_meta={"format": "html"},
    )
