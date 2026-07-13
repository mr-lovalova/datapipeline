import logging
from typing import Any, Callable

from datapipeline.cli.visuals.execution import (
    make_pipeline_observer,
    make_operation_observer,
)
from datapipeline.cli.visuals.backend import get_visuals_backend
from datapipeline.execution.observability import operation_observer
from datapipeline.runtime import Runtime


def _run_work(
    backend,
    runtime: Runtime,
    level: int,
    work: Callable[[], Any],
):
    previous_observer = getattr(runtime, "pipeline_observer", None)
    installed = previous_observer is None
    if installed:
        runtime.pipeline_observer = make_pipeline_observer(
            logging.getLogger("datapipeline.execution.observer")
        )
    try:
        observer = make_operation_observer(
            logging.getLogger("datapipeline.operation.observer")
        )
        with operation_observer(observer):
            with backend.wrap_execution(level):
                return work()
    finally:
        if installed:
            runtime.pipeline_observer = previous_observer


def run_with_backend(
    visuals: str, runtime: Runtime, level: int, work: Callable[[], Any]
):
    """Execute a unit of work inside a visuals backend context."""
    backend = get_visuals_backend(visuals)
    return _run_work(backend, runtime, level, work)
