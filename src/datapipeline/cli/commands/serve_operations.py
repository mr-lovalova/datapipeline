from __future__ import annotations

from typing import Callable

from datapipeline.cli.commands.serve_pipeline import serve_with_runtime
from datapipeline.config.tasks import ServeOperationTask
from datapipeline.operations.dispatch import dispatch_operation

ServeOperationRunner = Callable[..., None]


SERVE_OPERATION_RUNNERS: dict[str, ServeOperationRunner] = {
    "core.serve_pipeline": serve_with_runtime,
}


def run_serve_operation(
    *,
    operation: ServeOperationTask,
    runtime,
    dataset,
    limit: int | None,
    target,
    throttle_ms: float | None,
    stage: int | None,
    visuals: str,
) -> None:
    dispatch_operation(
        operation=operation,
        registry=SERVE_OPERATION_RUNNERS,
        operation_type="serve operation",
        runtime=runtime,
        dataset=dataset,
        limit=limit,
        target=target,
        throttle_ms=throttle_ms,
        stage=stage,
        visuals=visuals,
    )
