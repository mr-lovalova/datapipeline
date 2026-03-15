from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    VisualSettings,
    cascade,
    logging_value,
    materialize_log_output_for_run,
    observability_value,
    resolve_log_level,
    resolve_log_output,
    resolve_project_log_outputs,
    resolve_visuals,
)
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


@dataclass(frozen=True)
class RunProfile:
    idx: int
    total: int
    entry: RunEntry
    runtime: Runtime
    stage: Optional[int]
    limit: Optional[int]
    throttle_ms: Optional[float]
    cache_enabled: bool
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    visuals: VisualSettings
    output: OutputTarget
    build_mode: str

    @property
    def label(self) -> str:
        return self.entry.name or f"run{self.idx}"


def resolve_run_profiles(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    keep: Optional[str],
    stage: Optional[int],
    limit: Optional[int],
    cli_build_mode: Optional[str],
    cli_output,
    cli_log_level: Optional[str] = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: Optional[str] = None,
    cli_cache: Optional[bool] = None,
) -> list[RunProfile]:
    fallback_log_level = str(base_log_level).upper()
    cli_log_output_candidates = list(cli_log_outputs or [])

    profiles: list[RunProfile] = []
    for idx, total_runs, entry, runtime in iter_runtime_runs(
        project_path, run_entries, keep
    ):
        entry_name = entry.name
        run_label = entry_name or f"run{idx}"
        run_cfg = getattr(runtime, "run", None)
        run_observability = _run_config_value(run_cfg, "observability")
        build_cfg = _run_config_value(run_cfg, "build")
        profile_build_mode = getattr(build_cfg, "mode", None) if build_cfg is not None else None
        resolved_build_mode = str(
            cascade(cli_build_mode, profile_build_mode, "AUTO")
        ).upper()

        resolved_stage = cascade(stage, _run_config_value(run_cfg, "stage"))
        resolved_limit = cascade(limit, _run_config_value(run_cfg, "limit"))
        resolved_cache = bool(cascade(cli_cache, _run_config_value(run_cfg, "cache"), True))
        run_cmd = getattr(run_cfg, "cmd", None)
        create_run = run_cmd == "serve"
        if resolved_stage is not None and not create_run:
            raise ValueError(
                f"Runtime profile '{run_label}' does not support stage previews."
            )
        if keep is not None and not create_run:
            raise ValueError(
                f"Runtime profile '{run_label}' does not support keep filters."
            )
        throttle_ms = _run_config_value(run_cfg, "throttle_ms")
        runtime.cache_enabled = resolved_cache
        log_decision = resolve_log_level(
            cli_log_level,
            logging_value(run_observability, "level"),
            fallback=fallback_log_level,
        )
        run_log_outputs = resolve_project_log_outputs(
            logging_value(run_observability, "outputs"),
            project_path=project_path,
        )
        log_output = resolve_log_output(
            output_candidates=(
                cli_log_output_candidates,
                run_log_outputs,
            ),
            allow_run_scope=create_run,
        )

        run_visuals = observability_value(run_observability, "visuals")
        visuals = resolve_visuals(
            cli_visuals=cli_visuals,
            config_visuals=run_visuals,
        )

        target = resolve_output_target(
            cli_output=cli_output,
            config_output=getattr(run_cfg, "output", None),
            default=None,
            base_path=project_path.parent,
            run_name=run_label,
            stage=resolved_stage,
            create_run=create_run,
        )
        if create_run:
            log_output = materialize_log_output_for_run(
                settings=log_output,
                run_dir=(target.run.dataset_dir if target.run is not None else None),
                run_label=run_label,
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
                cache_enabled=resolved_cache,
                log_decision=log_decision,
                log_output=log_output,
                visuals=visuals,
                output=target,
                build_mode=resolved_build_mode,
            )
        )
    return profiles


__all__ = ["RunProfile", "resolve_run_profiles", "_run_config_value"]
