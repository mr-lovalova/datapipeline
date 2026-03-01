from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    cascade,
    resolve_log_level,
    resolve_log_output,
    resolve_project_log_outputs,
    resolve_visuals,
    resolve_workspace_log_outputs,
)
from datapipeline.config.workspace import WorkspaceContext


def _observability_value(observability, field: str):
    if observability is None:
        return None
    return getattr(observability, field, None)


def _logging_value(observability, field: str):
    logging_cfg = _observability_value(observability, "logging")
    if logging_cfg is None:
        return None
    return getattr(logging_cfg, field, None)


def _global_log_outputs(
    outputs: Sequence[LogOutputTarget] | None,
) -> list[LogOutputTarget]:
    if not outputs:
        return []
    return [target for target in outputs if target.scope != "run"]


@dataclass(frozen=True)
class BuildSettings:
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    mode: str
    force: bool
    profile_name: str | None


def resolve_build_settings(
    
    project_path: Path | None = None,
    workspace: WorkspaceContext | None = None,
    cli_log_level: Optional[str] = None,
    cli_visuals: Optional[str] = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    force_flag: bool = False,
    base_log_level: str | None = None,
    build_profile: BuildProfile | None = None,
) -> BuildSettings:
    shared = workspace.config.shared if workspace else None
    shared_observability = shared.observability if shared else None
    profile_observability = (
        _observability_value(build_profile, "observability")
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
    profile_visuals = _observability_value(profile_observability, "visuals")
    profile_log_level_default = _logging_value(profile_observability, "level")
    shared_log_level_default = _logging_value(shared_observability, "level")
    shared_visuals = _observability_value(shared_observability, "visuals")
    if build_profile is not None and project_path is not None:
        profile_log_outputs = resolve_project_log_outputs(
            _logging_value(profile_observability, "outputs"),
            project_path=project_path,
        )
    else:
        profile_log_outputs = resolve_workspace_log_outputs(
            _logging_value(profile_observability, "outputs"),
            workspace=workspace,
        )
    shared_log_outputs = resolve_workspace_log_outputs(
        _logging_value(shared_observability, "outputs"),
        workspace=workspace,
    )
    visuals = resolve_visuals(
        cli_visuals=cli_visuals,
        config_visuals=profile_visuals,
        workspace_visuals=shared_visuals,
    )
    log_decision = resolve_log_level(
        cli_log_level,
        profile_log_level_default,
        shared_log_level_default,
        fallback=str(base_log_level).upper() if base_log_level else "INFO",
    )
    effective_mode = "FORCE" if force_flag else (cascade(profile_mode_default, "AUTO") or "AUTO")
    effective_mode = effective_mode.upper()
    force_build = force_flag or effective_mode == "FORCE"
    return BuildSettings(
        visuals=visuals.visuals,
        log_decision=log_decision,
        log_output=resolve_log_output(
            output_candidates=(
                list(cli_log_outputs or []),
                _global_log_outputs(profile_log_outputs),
                _global_log_outputs(shared_log_outputs),
            ),
            allow_run_scope=False,
        ),
        mode=effective_mode,
        force=force_build,
        profile_name=profile_name,
    )
