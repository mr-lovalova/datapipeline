import logging
from typing import Optional

from datapipeline.analysis.vector.matrix import export_matrix_data
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.io.output import (
    OutputTarget,
    materialized_output_message,
    resolve_destination,
)
from datapipeline.runtime import Runtime

from .vector_stats_common import option, options_for_task, load_collector

logger = logging.getLogger(__name__)


def inspect_matrix_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target: OutputTarget | None = None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> None:
    _ = dataset, limit, throttle_ms, stage, visuals
    options = options_for_task(operation_task)
    matrix_format = str(option(options, "format", "html")).lower()
    if matrix_format not in {"csv", "html"}:
        raise ValueError(
            f"Invalid inspect matrix format '{matrix_format}'. Use 'csv' or 'html'."
        )
    matrix_path = resolve_destination(
        target,
        base_dir=runtime.artifacts_root / "inspect",
        default_filename=("matrix.csv" if matrix_format == "csv" else "matrix.html"),
    )
    collector = load_collector(
        runtime,
        options=options,
        matrix_format=matrix_format,
        matrix_path=matrix_path,
    )
    written = export_matrix_data(collector)
    if written is not None:
        emit_execution_message(
            materialized_output_message(
                "inspect_matrix",
                written,
                meta={"format": matrix_format},
            ),
            level=logging.INFO,
            logger=logger,
            message_kind="materialized",
        )
