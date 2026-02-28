from typing import Callable, TypeVar

from datapipeline.config.tasks import OperationTask
from datapipeline.utils.load import load_ep

TReturn = TypeVar("TReturn")
OperationRunner = Callable[..., TReturn]


def dispatch_operation(
    operation: OperationTask,
    operation_group: str,
    operation_type: str,
    **kwargs,
) -> TReturn:
    try:
        runner = load_ep(operation_group, operation.entrypoint)
    except ValueError as exc:
        raise ValueError(
            f"Unknown {operation_type} entrypoint '{operation.entrypoint}' "
            f"for operation '{operation.effective_name()}'."
        ) from exc
    if not callable(runner):
        raise TypeError(
            f"Entrypoint '{operation.entrypoint}' for {operation_type} "
            f"must resolve to a callable."
        )
    return runner(**kwargs)
