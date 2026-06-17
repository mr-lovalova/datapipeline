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
    observability_value,
    resolve_log_level,
    resolve_log_output,
    resolve_project_log_outputs,
    resolve_visuals,
)
from datapipeline.io.output import OutputTarget, resolve_output_target
from datapipeline.io.output import resolve_output_directory
from datapipeline.runtime import Runtime
from datapipeline.services.runs import RunPaths, start_run_for_directory
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
    preview_index: Optional[int]
    limit: Optional[int]
    throttle_ms: Optional[float]
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
    preview_index: Optional[int],
    limit: Optional[int],
    cli_build_mode: Optional[str],
    cli_output,
    cli_log_level: Optional[str] = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: Optional[str] = None,
    managed_run_targets: set[str] | None = None,
) -> list[RunProfile]:
    fallback_log_level = str(base_log_level).upper()
    cli_log_output_candidates = list(cli_log_outputs or [])
    managed_target_ids = None if managed_run_targets is None else set(managed_run_targets)

    resolved_entries = list(iter_runtime_runs(
        project_path, run_entries, keep
    ))
    has_runtime_serve_profiles = any(
        getattr(getattr(runtime, "run", None), "cmd", None) == "serve"
        and (managed_target_ids is None or entry.target_id in managed_target_ids)
        for _, _, entry, runtime in resolved_entries
    )
    shared_runtime_profile_counts: dict[Path, int] = {}
    shared_runs: dict[Path, RunPaths] = {}
    serve_roots: dict[int, Path | None] = {}

    for idx, _, entry, runtime in resolved_entries:
        entry_name = entry.name
        run_label = entry_name or f"run{idx}"
        run_cfg = getattr(runtime, "run", None)
        serve_root = resolve_output_directory(
            cli_output or getattr(run_cfg, "output", None),
            base_path=project_path.parent,
        )
        serve_roots[idx] = serve_root
        if (
            getattr(run_cfg, "cmd", None) == "serve"
            and (managed_target_ids is None or entry.target_id in managed_target_ids)
            and serve_root is not None
        ):
            shared_runtime_profile_counts[serve_root] = (
                shared_runtime_profile_counts.get(serve_root, 0) + 1
            )
        if (
            getattr(run_cfg, "cmd", None) == "serve"
            and (managed_target_ids is None or entry.target_id in managed_target_ids)
            and serve_root is not None
            and serve_root not in shared_runs
        ):
            run_paths, _ = start_run_for_directory(serve_root, preview_index=preview_index)
            shared_runs[serve_root] = run_paths

    profiles: list[RunProfile] = []
    for idx, total_runs, entry, runtime in resolved_entries:
        entry_name = entry.name
        run_label = entry_name or f"run{idx}"
        run_cfg = getattr(runtime, "run", None)
        run_observability = _run_config_value(run_cfg, "observability")
        build_cfg = _run_config_value(run_cfg, "build")
        profile_build_mode = getattr(build_cfg, "mode", None) if build_cfg is not None else None
        resolved_build_mode = str(
            cascade(cli_build_mode, profile_build_mode, "AUTO")
        ).upper()

        resolved_preview_index = cascade(
            preview_index,
            _run_config_value(run_cfg, "preview_index"),
        )
        resolved_limit = cascade(limit, _run_config_value(run_cfg, "limit"))
        run_cmd = getattr(run_cfg, "cmd", None)
        create_run = run_cmd == "serve" and (
            managed_target_ids is None or entry.target_id in managed_target_ids
        )
        if resolved_preview_index is not None and run_cmd != "serve":
            raise ValueError(
                f"Runtime profile '{run_label}' does not support preview indices."
            )
        if resolved_preview_index is not None and not has_runtime_serve_profiles:
            raise ValueError(
                f"Serve profile '{run_label}' does not support preview indices."
            )
        if not create_run:
            resolved_preview_index = None
        if keep is not None and run_cmd != "serve":
            raise ValueError(
                f"Runtime profile '{run_label}' does not support keep filters."
            )
        if keep is not None and run_cmd == "serve" and not has_runtime_serve_profiles:
            raise ValueError(
                f"Serve profile '{run_label}' does not support keep filters."
            )
        throttle_ms = _run_config_value(run_cfg, "throttle_ms")
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
            allow_execution_scope=True,
        )

        run_visuals = observability_value(run_observability, "visuals")
        visuals = resolve_visuals(
            cli_visuals=cli_visuals,
            config_visuals=run_visuals,
        )

        shared_run = None
        serve_root = serve_roots.get(idx)
        if run_cmd == "serve" and serve_root is not None:
            shared_run = shared_runs.get(serve_root)
        effective_output = cli_output or getattr(run_cfg, "output", None)
        if (
            create_run
            and serve_root is not None
            and shared_runtime_profile_counts.get(serve_root, 0) > 1
            and effective_output is not None
            and getattr(effective_output, "filename", None)
        ):
            raise ValueError(
                f"Serve profile '{run_label}' cannot set output.filename when multiple runtime serve profiles share one run."
            )

        target = resolve_output_target(
            cli_output=cli_output,
            config_output=getattr(run_cfg, "output", None),
            default=None,
            base_path=project_path.parent,
            run_name=run_label,
            run_paths=shared_run if create_run else None,
        )

        profiles.append(
            RunProfile(
                idx=idx,
                total=total_runs,
                entry=entry,
                runtime=runtime,
                preview_index=resolved_preview_index,
                limit=resolved_limit,
                throttle_ms=throttle_ms,
                log_decision=log_decision,
                log_output=log_output,
                visuals=visuals,
                output=target,
                build_mode=resolved_build_mode,
            )
        )
    return profiles


__all__ = ["RunProfile", "resolve_run_profiles", "_run_config_value"]
