from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.services.run_entries import RunEntry, iter_runtime_runs
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
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
    workspace_output_defaults,
)
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import (
    OutputTarget,
    resolve_output_target,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap


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


def _global_log_outputs(
    outputs: Sequence[LogOutputTarget] | None,
) -> list[LogOutputTarget]:
    if not outputs:
        return []
    return [target for target in outputs if target.scope != "run"]


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


@dataclass(frozen=True)
class BuildSettings:
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    mode: str
    force: bool
    profile_name: str | None


@dataclass(frozen=True)
class DatasetContext:
    project: Path
    dataset: FeatureDatasetConfig
    runtime: Runtime
    pipeline_context: PipelineContext

    @property
    def features(self):
        return list(self.dataset.features or [])

    @property
    def targets(self):
        return list(self.dataset.targets or [])


def load_dataset_context(project: Path | str) -> DatasetContext:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    runtime = bootstrap(project_path)
    context = PipelineContext(runtime)
    return DatasetContext(
        project=project_path,
        dataset=dataset,
        runtime=runtime,
        pipeline_context=context,
    )


def resolve_build_settings(
    *,
    project_path: Path | None = None,
    workspace: WorkspaceContext | None,
    cli_log_level: Optional[str],
    cli_visuals: Optional[str],
    cli_log_outputs: Sequence[LogOutputTarget] | None,
    force_flag: bool,
    base_log_level: str | None = None,
    build_profile=None,
) -> BuildSettings:
    shared = workspace.config.shared if workspace else None
    build_defaults = workspace.config.build if workspace else None
    shared_observability = shared.observability if shared else None
    build_observability = build_defaults.observability if build_defaults else None
    profile_observability = (
        _observability_value(build_profile, "observability")
        if build_profile is not None
        else None
    )
    profile_name = (
        build_profile.effective_name()
        if build_profile is not None
        else None
    )
    profile_mode_default = (
        str(getattr(build_profile, "mode")).upper()
        if build_profile is not None and getattr(build_profile, "mode", None)
        else None
    )
    profile_visuals = _observability_value(profile_observability, "visuals")
    profile_log_level_default = _logging_value(profile_observability, "level")
    shared_log_level_default = _logging_value(shared_observability, "level")
    build_log_level_default = _logging_value(build_observability, "level")
    shared_visuals = _observability_value(shared_observability, "visuals")
    build_visuals = _observability_value(build_observability, "visuals")
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
    build_log_outputs = resolve_workspace_log_outputs(
        _logging_value(build_observability, "outputs"),
        workspace=workspace,
    )
    build_mode_default = (
        build_defaults.mode.upper() if build_defaults and build_defaults.mode else None
    )
    visuals = resolve_visuals(
        cli_visuals=cli_visuals,
        config_visuals=cascade(profile_visuals, build_visuals),
        workspace_visuals=shared_visuals,
    )
    log_decision = resolve_log_level(
        cli_log_level,
        profile_log_level_default,
        build_log_level_default,
        shared_log_level_default,
        fallback=str(base_log_level).upper() if base_log_level else "INFO",
    )
    effective_mode = "FORCE" if force_flag else (
        cascade(profile_mode_default, build_mode_default, "AUTO") or "AUTO")
    effective_mode = effective_mode.upper()
    force_build = force_flag or effective_mode == "FORCE"
    return BuildSettings(
        visuals=visuals.visuals,
        log_decision=log_decision,
        log_output=resolve_log_output(
            output_candidates=(
                list(cli_log_outputs or []),
                _global_log_outputs(profile_log_outputs),
                _global_log_outputs(build_log_outputs),
                _global_log_outputs(shared_log_outputs),
            ),
            allow_run_scope=False,
        ),
        mode=effective_mode,
        force=force_build,
        profile_name=profile_name,
    )

def resolve_run_profiles(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    *,
    keep: Optional[str],
    stage: Optional[int],
    limit: Optional[int],
    cli_output,
    workspace: WorkspaceContext | None,
    cli_log_level: Optional[str],
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str,
    cli_visuals: Optional[str],
    create_run: bool = False,
) -> list[RunProfile]:
    shared = workspace.config.shared if workspace else None
    serve_defaults = workspace.config.serve if workspace else None
    shared_observability = shared.observability if shared else None
    serve_observability = serve_defaults.observability if serve_defaults else None
    shared_visuals_default = _observability_value(shared_observability, "visuals")
    shared_log_level_default = _logging_value(shared_observability, "level")
    shared_log_outputs_default = _logging_value(shared_observability, "outputs")
    serve_visuals_default = _observability_value(serve_observability, "visuals")
    serve_log_level_default = _logging_value(serve_observability, "level")
    serve_log_outputs_default = _logging_value(serve_observability, "outputs")
    serve_limit_default = serve_defaults.limit if serve_defaults else None
    serve_stage_default = serve_defaults.stage if serve_defaults else None
    serve_throttle_default = serve_defaults.throttle_ms if serve_defaults else None
    workspace_output_cfg = workspace_output_defaults(workspace)

    profiles: list[RunProfile] = []
    for idx, total_runs, entry, runtime in iter_runtime_runs(
        project_path, run_entries, keep
    ):
        entry_name = entry.name
        run_cfg = getattr(runtime, "run", None)

        resolved_stage = cascade(stage, _run_config_value(
            run_cfg, "stage"), serve_stage_default)
        resolved_limit = cascade(limit, _run_config_value(
            run_cfg, "limit"), serve_limit_default)
        throttle_ms = cascade(
            _run_config_value(run_cfg, "throttle_ms"),
            serve_throttle_default,
        )
        log_decision = resolve_log_level(
            cli_log_level,
            _run_logging_value(run_cfg, "level"),
            serve_log_level_default,
            shared_log_level_default,
            fallback=str(base_log_level).upper(),
        )
        run_log_outputs = resolve_project_log_outputs(
            _run_logging_value(run_cfg, "outputs"),
            project_path=project_path,
        )
        serve_log_outputs = resolve_workspace_log_outputs(
            serve_log_outputs_default,
            workspace=workspace,
        )
        shared_log_outputs = resolve_workspace_log_outputs(
            shared_log_outputs_default,
            workspace=workspace,
        )
        log_output = resolve_log_output(
            output_candidates=(
                list(cli_log_outputs or []),
                run_log_outputs,
                serve_log_outputs,
                shared_log_outputs,
            ),
            allow_run_scope=True,
        )

        run_visuals = _run_observability_value(run_cfg, "visuals")
        visuals = resolve_visuals(
            cli_visuals=cli_visuals,
            config_visuals=cascade(run_visuals, serve_visuals_default),
            workspace_visuals=shared_visuals_default,
        )

        runtime_output_cfg = workspace_output_cfg.model_copy() if workspace_output_cfg else None
        target = resolve_output_target(
            cli_output=cli_output,
            config_output=getattr(run_cfg, "output", None),
            default=runtime_output_cfg,
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
