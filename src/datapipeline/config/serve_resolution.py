from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.config.profiles import ServeOutputConfig
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
    cascade,
    resolve_observability_settings,
)
from datapipeline.config.split import HashSplitConfig, TimeSplitConfig
from datapipeline.io.output import OutputTarget, resolve_output_target
from datapipeline.io.output import resolve_output_directory
from datapipeline.runtime import Runtime
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.services.runs import RunPaths, get_run_paths
from datapipeline.services.run_entries import RunEntry, iter_runtime_runs


def _run_config_value(run_cfg, field: str):
    """Return a run config field only when it was explicitly provided."""
    if run_cfg is None:
        return None
    fields_set = getattr(run_cfg, "model_fields_set", None)
    if fields_set is not None and field not in fields_set:
        return None
    return getattr(run_cfg, field, None)


def _project_split_labels(runtime: Runtime) -> set[str] | None:
    split = getattr(runtime, "split", None)
    if split is None:
        return None
    if isinstance(split, TimeSplitConfig):
        if split.labels is None:
            return set()
        return set(split.labels)
    if isinstance(split, HashSplitConfig):
        if split.ratios is None:
            return set()
        return set(split.ratios.keys())
    return set()


def _validate_split_output_filenames(run_label: str, split_labels: list[str]) -> None:
    filenames: dict[str, str] = {}
    for label in split_labels:
        filename = sanitize_path_segment(label)
        existing = filenames.get(filename)
        if existing is not None:
            raise ValueError(
                f"Serve profile '{run_label}' split labels {existing!r} and "
                f"{label!r} resolve to the same output filename."
            )
        filenames[filename] = label


@dataclass(frozen=True)
class RunProfile:
    idx: int
    total: int
    entry: RunEntry
    runtime: Runtime
    preview_index: int | None
    limit: int | None
    throttle_ms: float | None
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    visuals: str
    output: OutputTarget
    heartbeat_interval_seconds: float | None = None

    @property
    def label(self) -> str:
        return self.entry.name or f"run{self.idx}"


def resolve_run_profiles(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    preview_index: int | None,
    limit: int | None,
    cli_output: ServeOutputConfig | None,
    cli_log_level: str | None = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: str | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
) -> list[RunProfile]:
    resolved_entries = list(iter_runtime_runs(project_path, run_entries))
    shared_runtime_profile_counts: dict[Path, int] = {}
    shared_runs: dict[Path, RunPaths] = {}
    serve_roots: dict[int, Path | None] = {}

    for idx, _, entry, runtime in resolved_entries:
        run_cfg = getattr(runtime, "run", None)
        serve_root = resolve_output_directory(
            cli_output or getattr(run_cfg, "output", None),
            base_path=project_path.parent,
        )
        serve_roots[idx] = serve_root
        if getattr(run_cfg, "cmd", None) == "serve" and serve_root is not None:
            shared_runtime_profile_counts[serve_root] = (
                shared_runtime_profile_counts.get(serve_root, 0) + 1
            )
        if (
            getattr(run_cfg, "cmd", None) == "serve"
            and serve_root is not None
            and serve_root not in shared_runs
        ):
            shared_runs[serve_root] = get_run_paths(serve_root)

    profiles: list[RunProfile] = []
    for idx, total_runs, entry, runtime in resolved_entries:
        entry_name = entry.name
        run_label = entry_name or f"run{idx}"
        run_cfg = getattr(runtime, "run", None)
        run_observability = _run_config_value(run_cfg, "observability")
        observability = resolve_observability_settings(
            project_path,
            run_observability,
            cli_visuals=cli_visuals,
            cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
        resolved_preview_index = cascade(
            preview_index,
            _run_config_value(run_cfg, "preview_index"),
        )
        resolved_limit = cascade(limit, _run_config_value(run_cfg, "limit"))
        run_cmd = getattr(run_cfg, "cmd", None)
        run_splits = list(getattr(run_cfg, "splits", None) or [])
        create_run = run_cmd == "serve"
        if resolved_preview_index is not None and run_cmd != "serve":
            raise ValueError(
                f"Runtime profile '{run_label}' does not support preview indices."
            )
        if not create_run:
            resolved_preview_index = None
        if run_splits and not create_run:
            raise ValueError(f"Serve profile '{run_label}' does not support splits.")
        if run_splits and resolved_preview_index is not None:
            raise ValueError(
                f"Serve profile '{run_label}' cannot combine preview_index with splits."
            )
        if run_splits:
            _validate_split_output_filenames(run_label, run_splits)
            project_labels = _project_split_labels(runtime)
            if project_labels is None:
                raise ValueError(
                    f"Serve profile '{run_label}' defines splits but project split is not configured."
                )
            unknown_labels = [
                label for label in run_splits if label not in project_labels
            ]
            if unknown_labels:
                unknown = ", ".join(repr(label) for label in unknown_labels)
                raise ValueError(
                    f"Serve profile '{run_label}' references unknown split labels: {unknown}"
                )
        throttle_ms = _run_config_value(run_cfg, "throttle_ms")
        shared_run = None
        serve_root = serve_roots.get(idx)
        if run_cmd == "serve" and serve_root is not None:
            shared_run = shared_runs.get(serve_root)
        effective_output = cli_output or getattr(run_cfg, "output", None)
        if (
            run_splits
            and effective_output is not None
            and getattr(effective_output, "filename", None)
        ):
            raise ValueError(
                f"Serve profile '{run_label}' cannot set output.filename with splits."
            )
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
        if run_splits and target.transport != "fs":
            raise ValueError(
                f"Serve profile '{run_label}' defines splits but output transport is not fs."
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
                log_decision=observability.log_decision,
                log_output=observability.log_output,
                visuals=observability.visuals,
                output=target,
                heartbeat_interval_seconds=observability.heartbeat_interval_seconds,
            )
        )

    output_owners: dict[Path, str] = {}
    for profile in profiles:
        output = profile.output
        if output.destination is None:
            continue
        run_cfg = getattr(profile.runtime, "run", None)
        split_labels = list(getattr(run_cfg, "splits", None) or [])
        destinations = (
            [(label, output.for_split(label).destination) for label in split_labels]
            if split_labels
            else [(None, output.destination)]
        )
        for split_label, destination in destinations:
            if destination is None:
                continue
            owner = f"profile '{profile.label}'"
            if split_label is not None:
                owner = f"{owner} split {split_label!r}"
            previous = output_owners.get(destination)
            if previous is not None:
                raise ValueError(
                    f"Runtime outputs for {previous} and {owner} resolve to the "
                    f"same path '{destination}'."
                )
            output_owners[destination] = owner

    preview_indices_by_run: dict[RunPaths, set[int | None]] = {}
    for profile in profiles:
        run = profile.output.run
        if run is not None:
            preview_indices_by_run.setdefault(run, set()).add(profile.preview_index)
    for run, preview_indices in preview_indices_by_run.items():
        if len(preview_indices) > 1:
            raise ValueError(
                "Serve profiles sharing output directory "
                f"'{run.serve_root}' must use the same preview_index."
            )
    return profiles


__all__ = ["RunProfile", "resolve_run_profiles", "_run_config_value"]
