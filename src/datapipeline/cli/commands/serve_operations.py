from datapipeline.config.tasks import ServeOperationTask
from datapipeline.operations.dispatch import dispatch_operation
from datapipeline.plugins import SERVE_OPERATIONS_EP


def run_serve_operation(
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
        operation_group=SERVE_OPERATIONS_EP,
        operation_type="serve operation",
        runtime=runtime,
        dataset=dataset,
        limit=limit,
        target=target,
        throttle_ms=throttle_ms,
        stage=stage,
        visuals=visuals,
    )
