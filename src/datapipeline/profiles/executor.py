from dataclasses import dataclass
from typing import Any, Callable

from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.runner import run_with_backend
from datapipeline.config.resolution import ObservabilitySettings
from datapipeline.runtime import Runtime


@dataclass(frozen=True)
class ExecutionSpec:
    observability: ObservabilitySettings
    runtime: Runtime


def run_execution(spec: ExecutionSpec, work: Callable[[], Any]) -> Any:
    configure_root_logging(
        level=spec.observability.log_decision.value,
        output=spec.observability.log_output,
    )
    return run_with_backend(
        visuals=spec.observability.visuals,
        runtime=spec.runtime,
        level=spec.observability.log_decision.value,
        work=work,
    )
