from dataclasses import dataclass
from pathlib import Path
from typing import Sequence
from unicodedata import normalize

from datapipeline.config.dataset.split import split_output_labels
from datapipeline.config.profiles import (
    InspectProfile,
    ServeOutputConfig,
    ServeProfile,
)
from datapipeline.config.preview import PreviewStage
from datapipeline.config.tasks import PipelineTask
from datapipeline.execution.settings import (
    LogOutputTarget,
    ObservabilitySettings,
    resolve_observability_settings,
)
from datapipeline.io.output import (
    OutputTarget,
    resolve_output_directory,
    resolve_output_target,
)
from datapipeline.services.definitions import PipelineDefinition
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.services.runs import RunPaths, get_run_paths


@dataclass(frozen=True)
class ResolvedRuntimeProfile:
    name: str
    operation_id: str
    preview: PreviewStage | None
    limit: int | None
    throttle_ms: float | None
    observability: ObservabilitySettings
    output: OutputTarget
    output_splits: tuple[str, ...]


def _dataset_output_splits(definition: PipelineDefinition) -> tuple[str, ...]:
    split = definition.dataset.split
    if split is None:
        return ()
    return split_output_labels(split)


def _validate_split_output_filenames(
    profile_name: str, output_splits: tuple[str, ...]
) -> None:
    filenames: dict[str, str] = {}
    for label in output_splits:
        filename = sanitize_path_segment(label)
        collision_key = normalize("NFC", filename).casefold()
        existing = filenames.get(collision_key)
        if existing is not None:
            raise ValueError(
                f"Serve profile '{profile_name}' split labels {existing!r} and "
                f"{label!r} resolve to the same output filename."
            )
        filenames[collision_key] = label


def resolve_runtime_profiles(
    definition: PipelineDefinition,
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
    project_path = definition.project.path
    runtime_operations = {
        operation.id: operation for operation in definition.runtime_operations
    }
    dataset_output_splits = _dataset_output_splits(definition)
    shared_profile_counts: dict[Path, int] = {}
    shared_runs: dict[Path, RunPaths] = {}
    serve_roots: dict[str, Path | None] = {}

    for profile in profiles:
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
    for profile in profiles:
        if isinstance(profile, ServeProfile):
            is_serve = True
            is_dataset_operation = isinstance(
                runtime_operations.get(profile.operation), PipelineTask
            )
            configured_preview = profile.preview
            configured_limit = profile.limit
            throttle_ms = profile.throttle_ms
            include_splits = tuple(profile.include_splits or ())
        else:
            is_serve = False
            is_dataset_operation = False
            configured_preview = None
            configured_limit = None
            throttle_ms = None
            include_splits = ()
        resolved_preview = preview if preview is not None else configured_preview
        if resolved_preview is not None and not is_serve:
            raise ValueError(
                f"Runtime profile '{profile.name}' does not support previews."
            )
        resolved_limit = limit if limit is not None else configured_limit

        if include_splits and resolved_preview is not None:
            raise ValueError(
                f"Serve profile '{profile.name}' cannot combine preview with "
                "include_splits."
            )
        if include_splits:
            if definition.dataset.split is None:
                raise ValueError(
                    f"Serve profile '{profile.name}' defines include_splits but "
                    "dataset split is not configured."
                )
            unknown_labels = [
                label for label in include_splits if label not in dataset_output_splits
            ]
            if unknown_labels:
                unknown = ", ".join(repr(label) for label in unknown_labels)
                raise ValueError(
                    f"Serve profile '{profile.name}' includes splits not published "
                    f"by the dataset: {unknown}"
                )
        output_splits = include_splits
        if is_dataset_operation and resolved_preview is None and not output_splits:
            output_splits = dataset_output_splits
        if output_splits:
            _validate_split_output_filenames(profile.name, output_splits)

        effective_output = cli_output or profile.output
        if output_splits and effective_output is not None and effective_output.filename:
            raise ValueError(
                f"Serve profile '{profile.name}' cannot set output.filename for "
                "split dataset output."
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
        if output_splits and target.transport != "fs":
            raise ValueError(
                f"Serve profile '{profile.name}' requires fs output for split "
                "dataset output."
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
                operation_id=profile.operation,
                preview=resolved_preview if is_serve else None,
                limit=resolved_limit,
                throttle_ms=throttle_ms,
                observability=observability,
                output=target,
                output_splits=output_splits,
            )
        )

    output_owners: dict[Path, str] = {}
    for resolved_profile in resolved:
        if resolved_profile.output.destination is None:
            continue
        destinations = (
            [
                (label, resolved_profile.output.for_split(label).destination)
                for label in resolved_profile.output_splits
            ]
            if resolved_profile.output_splits
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
