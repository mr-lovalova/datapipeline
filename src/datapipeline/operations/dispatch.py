
from typing import Callable, Mapping, TypeVar

from datapipeline.config.tasks import OperationTask

TReturn = TypeVar("TReturn")
OperationRunner = Callable[..., TReturn]


def dispatch_operation(
    
    operation: OperationTask,
    registry: Mapping[str, OperationRunner[TReturn]],
    operation_type: str,
    **kwargs,
) -> TReturn:
    runner = registry.get(operation.entrypoint)
    if runner is None:
        raise ValueError(
            f"Unknown {operation_type} entrypoint '{operation.entrypoint}' "
            f"for operation '{operation.effective_name()}'."
        )
    return runner(**kwargs)
