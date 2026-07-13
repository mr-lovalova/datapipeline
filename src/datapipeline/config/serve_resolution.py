from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.config.profiles import (
    InspectProfile,
    ServeOutputConfig,
    ServeProfile,
)
from datapipeline.config.preview import PreviewStage
from datapipeline.config.resolution import (
    LogOutputTarget,
    ObservabilitySettings,
    cascade,
    resolve_observability_settings,
)
from datapipeline.config.split import TimeSplitConfig
from datapipeline.io.output import (
    OutputTarget,
    resolve_output_directory,
    resolve_output_target,
)
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap_build_runtime
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.services.runs import RunPaths, get_run_paths


@dataclass(frozen=True)
class ResolvedRuntimeProfile:
    name: str
    target_id: str
    runtime: Runtime
    preview: PreviewStage | None
    limit: int | None
    throttle_ms: float | None
    observability: ObservabilitySettings
    output: OutputTarget
    splits: tuple[str, ...]


def _project_split_labels(runtime: Runtime) -> set[str] | None:
    split = runtime.split
    if split is None:
        return None
    if isinstance(split, TimeSplitConfig):
        return set(split.labels)
    return set(split.ratios)


def _validate_split_output_filenames(
    profile_name: str, splits: tuple[str, ...]
) -> None:
    filenames: dict[str, str] = {}
    for label in splits:
        filename = sanitize_path_segment(label)
        existing = filenames.get(filename)
        if existing is not None:
            raise ValueError(
                f"Serve profile '{profile_name}' split labels {existing!r} and "
                f"{label!r} resolve to the same output filename."
            )
        filenames[filename] = label


def resolve_runtime_profiles(
    project_path: Path,
    profiles: Sequence[ServeProfile | InspectProfile],
    preview: PreviewStage | None,
    limit: int | None,
    cli_output: ServeOutputConfig | None,
    cli_log_level: str | None = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: str | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
) -> list[ResolvedRuntimeProfile]:
    resolved_profiles = [
        (profile, bootstrap_build_runtime(project_path)) for profile in profiles
    ]
    shared_profile_counts: dict[Path, int] = {}
    shared_runs: dict[Path, RunPaths] = {}
    serve_roots: dict[str, Path | None] = {}

    for profile, _runtime in resolved_profiles:
        serve_root = resolve_output_directory(
            cli_output or profile.output,
            base_path=project_path.parent,
        )
        serve_roots[profile.name] = serve_root
        if isinstance(profile, ServeProfile) and serve_root is not None:
            shared_profile_counts[serve_root] = (
                shared_profile_counts.get(serve_root, 0) + 1
            )
            if serve_root not in shared_runs:
                shared_runs[serve_root] = get_run_paths(serve_root)

    resolved: list[ResolvedRuntimeProfile] = []
    for profile, runtime in resolved_profiles:
        if isinstance(profile, ServeProfile):
            is_serve = True
            configured_preview = profile.preview
            configured_limit = profile.limit
            throttle_ms = profile.throttle_ms
            splits = tuple(profile.splits or ())
        else:
            is_serve = False
            configured_preview = None
            configured_limit = None
            throttle_ms = None
            splits = ()
        resolved_preview = cascade(preview, configured_preview)
        if resolved_preview is not None and not is_serve:
            raise ValueError(
                f"Runtime profile '{profile.name}' does not support previews."
            )
        resolved_limit = cascade(limit, configured_limit)

        if splits and resolved_preview is not None:
            raise ValueError(
                f"Serve profile '{profile.name}' cannot combine preview with splits."
            )
        if splits:
            _validate_split_output_filenames(profile.name, splits)
            project_labels = _project_split_labels(runtime)
            if project_labels is None:
                raise ValueError(
                    f"Serve profile '{profile.name}' defines splits but project split is not configured."
                )
            unknown_labels = [label for label in splits if label not in project_labels]
            if unknown_labels:
                unknown = ", ".join(repr(label) for label in unknown_labels)
                raise ValueError(
                    f"Serve profile '{profile.name}' references unknown split labels: {unknown}"
                )

        effective_output = cli_output or profile.output
        if splits and effective_output is not None and effective_output.filename:
            raise ValueError(
                f"Serve profile '{profile.name}' cannot set output.filename with splits."
            )

        serve_root = serve_roots[profile.name]
        if (
            is_serve
            and serve_root is not None
            and shared_profile_counts.get(serve_root, 0) > 1
            and effective_output is not None
            and effective_output.filename
        ):
            raise ValueError(
                f"Serve profile '{profile.name}' cannot set output.filename when "
                "multiple runtime serve profiles share one run."
            )

        target = resolve_output_target(
            cli_output=cli_output,
            config_output=profile.output,
            default=None,
            base_path=project_path.parent,
            run_name=profile.name,
            run_paths=(
                shared_runs.get(serve_root)
                if is_serve and serve_root is not None
                else None
            ),
        )
        if splits and target.transport != "fs":
            raise ValueError(
                f"Serve profile '{profile.name}' defines splits but output transport is not fs."
            )

        observability = resolve_observability_settings(
            project_path,
            profile.observability,
            cli_visuals=cli_visuals,
            cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
        resolved.append(
            ResolvedRuntimeProfile(
                name=profile.name,
                target_id=profile.target,
                runtime=runtime,
                preview=resolved_preview if is_serve else None,
                limit=resolved_limit,
                throttle_ms=throttle_ms,
                observability=observability,
                output=target,
                splits=splits,
            )
        )

    output_owners: dict[Path, str] = {}
    for resolved_profile in resolved:
        if resolved_profile.output.destination is None:
            continue
        destinations = (
            [
                (label, resolved_profile.output.for_split(label).destination)
                for label in resolved_profile.splits
            ]
            if resolved_profile.splits
            else [(None, resolved_profile.output.destination)]
        )
        for split_label, destination in destinations:
            if destination is None:
                continue
            owner = f"profile '{resolved_profile.name}'"
            if split_label is not None:
                owner = f"{owner} split {split_label!r}"
            previous = output_owners.get(destination)
            if previous is not None:
                raise ValueError(
                    f"Runtime outputs for {previous} and {owner} resolve to the "
                    f"same path '{destination}'."
                )
            output_owners[destination] = owner

    previews_by_run: dict[RunPaths, set[PreviewStage | None]] = {}
    for resolved_profile in resolved:
        run = resolved_profile.output.run
        if run is not None:
            previews_by_run.setdefault(run, set()).add(resolved_profile.preview)
    for run, previews in previews_by_run.items():
        if len(previews) > 1:
            raise ValueError(
                "Serve profiles sharing output directory "
                f"'{run.serve_root}' must use the same preview."
            )
    return resolved
