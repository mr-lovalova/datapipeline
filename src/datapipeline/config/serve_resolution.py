from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    VisualSettings,
    cascade,
    materialize_log_output_for_run,
    resolve_log_level,
    resolve_log_output,
    resolve_project_log_outputs,
    resolve_visuals,
    resolve_workspace_log_outputs,
)
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import OutputTarget, resolve_output_target
from datapipeline.runtime import Runtime
from datapipeline.services.run_entries import RunEntry, iter_runtime_runs


def _run_config_value(run_cfg, field: str):
    """Return a run config field only when it was explicitly provided."""
    if run_cfg is None:
        return None
    fields_set = getattr(run_cfg, "model_fields_set", None)
    if fields_set is not None and field not in fields_set:
        return None
    return getattr(run_cfg, field, None)


def _observability_value(observability, field: str):
    if observability is None:
        return None
    return getattr(observability, field, None)


def _logging_value(observability, field: str):
    logging_cfg = _observability_value(observability, "logging")
    if logging_cfg is None:
        return None
    return getattr(logging_cfg, field, None)


def _run_observability_value(run_cfg, field: str):
    observability = _run_config_value(run_cfg, "observability")
    return _observability_value(observability, field)


def _run_logging_value(run_cfg, field: str):
    observability = _run_config_value(run_cfg, "observability")
    return _logging_value(observability, field)


@dataclass(frozen=True)
class RunProfile:
    idx: int
    total: int
    entry: RunEntry
    runtime: Runtime
    stage: Optional[int]
    limit: Optional[int]
    throttle_ms: Optional[float]
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    visuals: VisualSettings
    output: OutputTarget

    @property
    def label(self) -> str:
        return self.entry.name or f"run{self.idx}"


def resolve_run_profiles(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    
    keep: Optional[str],
    stage: Optional[int],
    limit: Optional[int],
    cli_output,
    workspace: WorkspaceContext | None,
    cli_log_level: Optional[str],
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: Optional[str] = None,
    create_run: bool = False,
) -> list[RunProfile]:
    shared = workspace.config.shared if workspace else None
    shared_observability = shared.observability if shared else None
    shared_visuals_default = _observability_value(shared_observability, "visuals")
    shared_log_level_default = _logging_value(shared_observability, "level")
    shared_log_outputs_default = _logging_value(shared_observability, "outputs")

    profiles: list[RunProfile] = []
    for idx, total_runs, entry, runtime in iter_runtime_runs(
        project_path, run_entries, keep
    ):
        entry_name = entry.name
        run_cfg = getattr(runtime, "run", None)

        resolved_stage = cascade(stage, _run_config_value(run_cfg, "stage"))
        resolved_limit = cascade(limit, _run_config_value(run_cfg, "limit"))
        throttle_ms = _run_config_value(run_cfg, "throttle_ms")
        log_decision = resolve_log_level(
            cli_log_level,
            _run_logging_value(run_cfg, "level"),
            shared_log_level_default,
            fallback=str(base_log_level).upper(),
        )
        run_log_outputs = resolve_project_log_outputs(
            _run_logging_value(run_cfg, "outputs"),
            project_path=project_path,
        )
        shared_log_outputs = resolve_workspace_log_outputs(
            shared_log_outputs_default,
            workspace=workspace,
        )
        log_output = resolve_log_output(
            output_candidates=(
                list(cli_log_outputs or []),
                run_log_outputs,
                shared_log_outputs,
            ),
            allow_run_scope=create_run,
        )

        run_visuals = _run_observability_value(run_cfg, "visuals")
        visuals = resolve_visuals(
            cli_visuals=cli_visuals,
            config_visuals=run_visuals,
            workspace_visuals=shared_visuals_default,
        )

        target = resolve_output_target(
            cli_output=cli_output,
            config_output=getattr(run_cfg, "output", None),
            default=None,
            base_path=project_path.parent,
            run_name=entry_name or f"run{idx}",
            stage=resolved_stage,
            create_run=create_run,
        )
        if create_run:
            log_output = materialize_log_output_for_run(
                settings=log_output,
                run_dir=(target.run.dataset_dir if target.run is not None else None),
                run_label=entry_name or f"run{idx}",
            )

        profiles.append(
            RunProfile(
                idx=idx,
                total=total_runs,
                entry=entry,
                runtime=runtime,
                stage=resolved_stage,
                limit=resolved_limit,
                throttle_ms=throttle_ms,
                log_decision=log_decision,
                log_output=log_output,
                visuals=visuals,
                output=target,
            )
        )
    return profiles


__all__ = ["RunProfile", "resolve_run_profiles", "_run_config_value"]
