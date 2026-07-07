from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    cascade,
    logging_value,
    observability_value,
    resolve_heartbeat_interval_seconds,
    resolve_log_level,
    resolve_log_output,
    resolve_project_log_outputs,
    resolve_visuals,
)


@dataclass(frozen=True)
class BuildSettings:
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    mode: str
    force: bool
    profile_name: str | None
    heartbeat_interval_seconds: float | None = None


def resolve_build_settings(
    project_path: Path | None = None,
    cli_log_level: Optional[str] = None,
    cli_visuals: Optional[str] = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
    force_flag: bool = False,
    runtime_build_mode: str | None = None,
    base_log_level: str | None = None,
    build_profile: BuildProfile | None = None,
) -> BuildSettings:
    profile_observability = (
        observability_value(build_profile, "observability")
        if build_profile is not None
        else None
    )
    profile_name = (
        build_profile.name
        if build_profile is not None
        else None
    )
    if build_profile is not None and project_path is None:
        raise ValueError("project_path is required when resolving build profile settings")
    profile_mode_default = (
        str(getattr(build_profile, "mode")).upper()
        if build_profile is not None and getattr(build_profile, "mode", None)
        else None
    )
    runtime_mode_default = (
        str(runtime_build_mode).strip().upper()
        if runtime_build_mode is not None
        else None
    )
    profile_visuals = observability_value(profile_observability, "visuals")
    profile_heartbeat_interval = observability_value(
        profile_observability,
        "heartbeat_interval_seconds",
    )
    profile_log_level_default = logging_value(profile_observability, "level")
    if build_profile is not None and project_path is not None:
        profile_log_outputs = resolve_project_log_outputs(
            logging_value(profile_observability, "outputs"),
            project_path=project_path,
        )
    else:
        profile_log_outputs = []
    visuals = resolve_visuals(
        cli_visuals=cli_visuals,
        config_visuals=profile_visuals,
    )
    log_decision = resolve_log_level(
        cli_log_level,
        profile_log_level_default,
        fallback=str(base_log_level).upper() if base_log_level else "INFO",
    )
    effective_mode = (
        "FORCE"
        if force_flag
        else (cascade(runtime_mode_default, profile_mode_default, "AUTO") or "AUTO")
    )
    effective_mode = effective_mode.upper()
    force_build = force_flag or effective_mode == "FORCE"
    return BuildSettings(
        visuals=visuals.visuals,
        log_decision=log_decision,
        log_output=resolve_log_output(
            output_candidates=(
                list(cli_log_outputs or []),
                profile_log_outputs,
            ),
            allow_execution_scope=True,
        ),
        mode=effective_mode,
        force=force_build,
        profile_name=profile_name,
        heartbeat_interval_seconds=resolve_heartbeat_interval_seconds(
            cli_heartbeat_interval_seconds,
            profile_heartbeat_interval,
        ),
    )
