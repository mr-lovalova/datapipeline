from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    resolve_observability_settings,
)


@dataclass(frozen=True)
class BuildSettings:
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    mode: str
    heartbeat_interval_seconds: float | None = None


def resolve_build_settings(
    project_path: Path | None = None,
    cli_log_level: str | None = None,
    cli_visuals: str | None = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
    force_flag: bool = False,
    base_log_level: str | None = None,
    build_profile: BuildProfile | None = None,
) -> BuildSettings:
    profile_observability = (
        build_profile.observability if build_profile is not None else None
    )
    if build_profile is not None and project_path is None:
        raise ValueError(
            "project_path is required when resolving build profile settings"
        )
    profile_mode_default = build_profile.mode if build_profile is not None else None
    observability = resolve_observability_settings(
        project_path,
        profile_observability,
        cli_visuals=cli_visuals,
        cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level or "INFO",
    )
    effective_mode = "FORCE" if force_flag else profile_mode_default or "AUTO"
    return BuildSettings(
        visuals=observability.visuals,
        log_decision=observability.log_decision,
        log_output=observability.log_output,
        mode=effective_mode,
        heartbeat_interval_seconds=observability.heartbeat_interval_seconds,
    )
