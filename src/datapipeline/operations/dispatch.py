from typing import Any, Callable, TypeVar

from datapipeline.config.tasks import Task
from datapipeline.utils.load import load_ep

TReturn = TypeVar("TReturn")
TPersisted = TypeVar("TPersisted")
OperationRunner = Callable[..., TReturn]


def dispatch_operation(
    operation: Task,
    operation_group: str,
    **kwargs,
) -> TReturn:
    try:
        runner = load_ep(operation_group, operation.entrypoint)
    except ValueError as exc:
        raise ValueError(
            f"Unknown entrypoint '{operation.entrypoint}' "
            f"for operation '{operation.id}'."
        ) from exc
    if not callable(runner):
        raise TypeError(
            f"Entrypoint '{operation.entrypoint}' for operation '{operation.id}' "
            f"must resolve to a callable."
        )
    return runner(**kwargs)


def execute_operation(
    operation: Task,
    operation_group: str,
    *,
    persist: Callable[[TReturn], TPersisted],
    **kwargs,
) -> TPersisted:
    result = dispatch_operation(
        operation=operation,
        operation_group=operation_group,
        **kwargs,
    )
    return persist(result)
