from __future__ import annotations

from typing import Callable

from datapipeline.build.tasks.metadata import materialize_metadata
from datapipeline.build.tasks.scaler import materialize_scaler_statistics
from datapipeline.build.tasks.schema import materialize_vector_schema
from datapipeline.config.tasks import ArtifactTask
from datapipeline.operations.dispatch import dispatch_operation

BuildOperationResult = tuple[str, dict[str, object]] | None
BuildOperationRunner = Callable[..., BuildOperationResult]


BUILD_OPERATION_RUNNERS: dict[str, BuildOperationRunner] = {
    "core.build.schema": materialize_vector_schema,
    "core.build.metadata": materialize_metadata,
    "core.build.scaler": materialize_scaler_statistics,
}


def run_build_operation(
    *,
    operation: ArtifactTask,
    runtime,
) -> BuildOperationResult:
    return dispatch_operation(
        operation=operation,
        registry=BUILD_OPERATION_RUNNERS,
        operation_type="build operation",
        runtime=runtime,
        task_cfg=operation,
    )
