import logging
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from typing import Any, Callable

from datapipeline.cli.logging_setup import root_logging_scope
from datapipeline.cli.visuals.execution import (
    make_operation_observer,
    make_pipeline_observer,
)
from datapipeline.cli.visuals.rich.progress import (
    rich_visuals_supported,
    visual_execution,
)
from datapipeline.execution.observability import operation_observer
from datapipeline.execution.settings import ObservabilitySettings
from datapipeline.runtime import Runtime


@dataclass(frozen=True)
class ExecutionSpec:
    observability: ObservabilitySettings
    runtime: Runtime


def run_execution(spec: ExecutionSpec, work: Callable[[], Any]) -> Any:
    level = spec.observability.log_decision.value
    with root_logging_scope(level, spec.observability.log_output):
        visuals_active = spec.observability.visuals == "on" and rich_visuals_supported()
        visuals: AbstractContextManager[Any]
        if visuals_active:
            visuals = visual_execution(level)
        else:
            visuals = nullcontext()

        runtime = spec.runtime
        previous_pipeline_observer = runtime.pipeline_observer
        previous_observe_node_events = runtime.observe_node_events
        if previous_pipeline_observer is None:
            runtime.pipeline_observer = make_pipeline_observer(
                logging.getLogger("datapipeline.execution.observer")
            )
            runtime.observe_node_events = visuals_active or level <= logging.DEBUG

        try:
            observer = make_operation_observer(
                logging.getLogger("datapipeline.operation.observer")
            )
            with operation_observer(observer):
                with visuals:
                    return work()
        finally:
            if previous_pipeline_observer is None:
                runtime.pipeline_observer = None
                runtime.observe_node_events = previous_observe_node_events
