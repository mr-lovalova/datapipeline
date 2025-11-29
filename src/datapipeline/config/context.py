from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.cli.commands.run_config import RunEntry, iter_runtime_runs
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.resolution import (
    LogLevelDecision,
    VisualSettings,
    cascade,
    resolve_log_level,
    resolve_visuals,
    workspace_output_defaults,
)
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import (
    OutputTarget,
    resolve_output_target,
)
from datapipeline.pipeline.context import PipelineContext
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
    visuals: VisualSettings
    output: OutputTarget

    @property
    def label(self) -> str:
        return self.entry.name or f"run{self.idx}"


@dataclass(frozen=True)
class BuildSettings:
    visuals: str
    progress: str
    mode: str
    force: bool


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
    workspace: WorkspaceContext | None,
    cli_visuals: Optional[str],
    cli_progress: Optional[str],
    force_flag: bool,
) -> BuildSettings:
    shared = workspace.config.shared if workspace else None
    build_defaults = workspace.config.build if workspace else None
    shared_visuals = shared.visuals if shared else None
    shared_progress = shared.progress if shared else None
    build_mode_default = (
        build_defaults.mode.upper() if build_defaults and build_defaults.mode else None
    )
    visuals = resolve_visuals(
        cli_visuals=cli_visuals,
        config_visuals=None,
        workspace_visuals=shared_visuals,
        cli_progress=cli_progress,
        config_progress=None,
        workspace_progress=shared_progress,
    )
    effective_mode = "FORCE" if force_flag else (
        cascade(build_mode_default, "AUTO") or "AUTO")
    effective_mode = effective_mode.upper()
    force_build = force_flag or effective_mode == "FORCE"
    return BuildSettings(
        visuals=visuals.visuals,
        progress=visuals.progress,
        mode=effective_mode,
        force=force_build,
    )


def resolve_run_profiles(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    *,
    keep: Optional[str],
    stage: Optional[int],
    limit: Optional[int],
    cli_output,
    cli_payload: Optional[str],
    workspace: WorkspaceContext | None,
    cli_log_level: Optional[str],
    base_log_level: str,
    cli_visuals: Optional[str],
    cli_progress: Optional[str],
    create_run: bool = False,
) -> list[RunProfile]:
    shared = workspace.config.shared if workspace else None
    serve_defaults = workspace.config.serve if workspace else None
    shared_visuals_default = shared.visuals if shared else None
    shared_progress_default = shared.progress if shared else None
    shared_log_level_default = shared.log_level if shared else None
    serve_log_level_default = serve_defaults.log_level if serve_defaults else None
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
            _run_config_value(run_cfg, "log_level"),
            serve_log_level_default,
            shared_log_level_default,
            fallback=str(base_log_level).upper(),
        )

        run_visuals = _run_config_value(run_cfg, "visuals")
        run_progress = _run_config_value(run_cfg, "progress")
        visuals = resolve_visuals(
            cli_visuals=cli_visuals,
            config_visuals=run_visuals,
            workspace_visuals=shared_visuals_default,
            cli_progress=cli_progress,
            config_progress=run_progress,
            workspace_progress=shared_progress_default,
        )

        runtime_output_cfg = workspace_output_cfg.model_copy() if workspace_output_cfg else None
        target = resolve_output_target(
            cli_output=cli_output,
            config_output=getattr(run_cfg, "output", None),
            default=runtime_output_cfg,
            base_path=project_path.parent,
            run_name=entry_name or f"run{idx}",
            payload_override=cli_payload,
            stage=resolved_stage,
            create_run=create_run,
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
                visuals=visuals,
                output=target,
            )
        )
    return profiles
