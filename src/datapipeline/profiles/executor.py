from dataclasses import dataclass
from typing import Any, Callable

from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.execution import emit_profile_started
from datapipeline.cli.visuals.runner import run_with_backend
from datapipeline.config.resolution import LogLevelDecision, LogOutputSettings
from datapipeline.runtime import Runtime


@dataclass(frozen=True, kw_only=True)
class ExecutionSpec:
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    runtime: Runtime


@dataclass(frozen=True, kw_only=True)
class ProfileExecutionSpec(ExecutionSpec):
    command: str
    name: str
    index: int
    total: int


def run_execution(spec: ExecutionSpec, work: Callable[[], Any]) -> Any:
    configure_root_logging(level=spec.log_decision.value, output=spec.log_output)
    return run_with_backend(
        visuals=spec.visuals,
        runtime=spec.runtime,
        level=spec.log_decision.value,
        work=work,
    )


def run_profile(spec: ProfileExecutionSpec, work: Callable[[], Any]) -> Any:
    def work_with_profile_start() -> Any:
        emit_profile_started(spec.command, spec.name, spec.index, spec.total)
        return work()

    return run_execution(spec, work_with_profile_start)
