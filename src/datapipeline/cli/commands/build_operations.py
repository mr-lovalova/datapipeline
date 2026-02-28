from datapipeline.config.tasks import ArtifactTask
from datapipeline.operations.dispatch import dispatch_operation
from datapipeline.plugins import BUILD_OPERATIONS_EP

BuildOperationResult = tuple[str, dict[str, object]] | None


def run_build_operation(
    operation: ArtifactTask,
    runtime,
) -> BuildOperationResult:
    return dispatch_operation(
        operation=operation,
        operation_group=BUILD_OPERATIONS_EP,
        operation_type="build operation",
        runtime=runtime,
        task_cfg=operation,
    )
