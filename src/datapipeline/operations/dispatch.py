from collections.abc import Callable

from datapipeline.config.tasks import Task
from datapipeline.utils.load import load_ep

OperationRunner = Callable[..., object]


def load_operation_runner(
    operation: Task,
    operation_group: str,
) -> OperationRunner:
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
    return runner
