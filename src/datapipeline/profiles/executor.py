from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any, Callable

from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.cli.visuals.execution import emit_profile_start
from datapipeline.cli.visuals.runner import run_job
from datapipeline.config.resolution import LogLevelDecision, LogOutputSettings
from datapipeline.runtime import Runtime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProfileExecutionSpec:
    command: str
    name: str
    idx: int
    total: int
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    sections: tuple[str, ...] = ()
    label: str | None = None
    runtime: Runtime | None = None
    use_visual_runner: bool = True
    render_header: bool = True
    profile_path: Path | None = None


def _format_outputs(settings: LogOutputSettings) -> str:
    parts: list[str] = []
    for target in settings.outputs:
        destination = str(target.destination) if target.destination is not None else "-"
        parts.append(f"{target.transport}:{target.scope}:{destination}")
    return ",".join(parts) if parts else "-"


def _log_profile_start(
    command: str,
    name: str,
    visuals: str,
    log_decision: LogLevelDecision,
    log_output: LogOutputSettings,
    profile_path: Path | None,
) -> None:
    message = (
        "Profile start "
        f"command={command} "
        f"name={name} "
        f"log_level={log_decision.name} "
        f"visuals={visuals} "
        f"log_outputs={_format_outputs(log_output)}"
    )
    if profile_path is not None:
        message = f"{message} profile={profile_path}"
    emit_profile_start(
        message,
        logger=logger,
    )


def _render_profile_header(spec: ProfileExecutionSpec) -> None:
    backend = get_visuals_backend(spec.visuals or "on")
    sections = tuple(section for section in spec.sections if section)
    label = spec.label or spec.name
    presented = False
    try:
        presented = backend.on_job_start(sections, label, spec.idx, spec.total)
    except Exception:
        presented = False
    if not presented:
        prefix = " / ".join(sections) if sections else "Job"
        logger.info("%s: '%s' (%d/%d)", prefix, label, spec.idx, spec.total)


def run_profile(
    spec: ProfileExecutionSpec,
    work: Callable[[], Any],
) -> Any:
    configure_root_logging(level=spec.log_decision.value, output=spec.log_output)
    profile_path = spec.profile_path
    if spec.use_visual_runner:
        if spec.runtime is None:
            raise ValueError("runtime is required when use_visual_runner=True")

        def _work_with_profile_start() -> Any:
            _log_profile_start(
                command=spec.command,
                name=spec.name,
                visuals=spec.visuals,
                log_decision=spec.log_decision,
                log_output=spec.log_output,
                profile_path=profile_path,
            )
            return work()

        return run_job(
            sections=spec.sections,
            label=spec.label or spec.name,
            visuals=spec.visuals or "on",
            level=spec.log_decision.value,
            runtime=spec.runtime,
            work=_work_with_profile_start,
            idx=spec.idx,
            total=spec.total,
        )
    if spec.render_header:
        _render_profile_header(spec)
    _log_profile_start(
        command=spec.command,
        name=spec.name,
        visuals=spec.visuals,
        log_decision=spec.log_decision,
        log_output=spec.log_output,
        profile_path=profile_path,
    )
    return work()
