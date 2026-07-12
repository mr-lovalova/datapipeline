from dataclasses import dataclass
from typing import Any, Callable

from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.runner import run_with_backend
from datapipeline.config.resolution import LogLevelDecision, LogOutputSettings
from datapipeline.runtime import Runtime


@dataclass(frozen=True, kw_only=True)
class ExecutionSpec:
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    runtime: Runtime


def run_execution(spec: ExecutionSpec, work: Callable[[], Any]) -> Any:
    configure_root_logging(level=spec.log_decision.value, output=spec.log_output)
    return run_with_backend(
        visuals=spec.visuals,
        runtime=spec.runtime,
        level=spec.log_decision.value,
        work=work,
    )
