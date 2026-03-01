import json
from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Any, Callable, Mapping

from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.cli.visuals.runner import run_job
from datapipeline.config.resolution import LogLevelDecision, LogOutputSettings
from datapipeline.runtime import Runtime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProfileExecutionSpec:
    kind: str
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
    artifact_payload: Mapping[str, Any] | None = None
    artifact_writer: Callable[[Mapping[str, Any]], Path | None] | None = None


def _format_outputs(settings: LogOutputSettings) -> str:
    parts: list[str] = []
    for target in settings.outputs:
        destination = str(target.destination) if target.destination is not None else "-"
        parts.append(f"{target.transport}:{target.scope}:{destination}")
    return ",".join(parts) if parts else "-"


def _persist_profile_artifact(
    kind: str,
    artifact_payload: Mapping[str, Any] | None,
    artifact_writer: Callable[[Mapping[str, Any]], Path | None] | None,
) -> Path | None:
    if artifact_writer is None or artifact_payload is None:
        return None
    try:
        return artifact_writer(artifact_payload)
    except Exception:
        logger.warning("Failed to persist %s profile artifact", kind, exc_info=True)
        return None


def _log_profile_start(
    kind: str,
    name: str,
    idx: int,
    total: int,
    visuals: str,
    log_decision: LogLevelDecision,
    log_output: LogOutputSettings,
    profile_path: Path | None,
) -> None:
    _ = idx, total
    payload: dict[str, object] = {
        "kind": kind,
        "name": name,
        "log_level": log_decision.name,
        "visuals": visuals,
        "log_outputs": _format_outputs(log_output),
    }
    if profile_path is not None:
        payload["profile"] = str(profile_path)
    message = f"Profile:\n{json.dumps(payload, indent=2, default=str)}"
    emit_execution_message(
        message,
        level=logging.DEBUG,
        logger=logger,
        message_kind="task_config",
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
    profile_path = _persist_profile_artifact(
        kind=spec.kind,
        artifact_payload=spec.artifact_payload,
        artifact_writer=spec.artifact_writer,
    )
    if spec.use_visual_runner:
        if spec.runtime is None:
            raise ValueError("runtime is required when use_visual_runner=True")

        def _work_with_profile_start() -> Any:
            _log_profile_start(
                kind=spec.kind,
                name=spec.name,
                idx=spec.idx,
                total=spec.total,
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
    _render_profile_header(spec)
    _log_profile_start(
        kind=spec.kind,
        name=spec.name,
        idx=spec.idx,
        total=spec.total,
        visuals=spec.visuals,
        log_decision=spec.log_decision,
        log_output=spec.log_output,
        profile_path=profile_path,
    )
    return work()
