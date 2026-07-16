from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from datapipeline.config.dataset.split import split_output_labels
from datapipeline.config.preview import PreviewStage
from datapipeline.config.profiles import (
    InspectProfile,
    ServeOutputConfig,
    ServeProfile,
)
from datapipeline.config.tasks import OperationTask, PipelineTask
from datapipeline.execution.settings import (
    LogOutputTarget,
    ObservabilitySettings,
    resolve_observability_settings,
)
from datapipeline.io.output import (
    OutputTarget,
    output_destination_key,
    resolve_output_directory,
    resolve_output_target,
)
from datapipeline.io.runs import RunPaths, get_run_paths
from datapipeline.services.definitions import PipelineDefinition


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


def _resolve_serve_output_splits(
    definition: PipelineDefinition,
    profile: ServeProfile,
    preview: PreviewStage | None,
    operation: OperationTask | None,
) -> tuple[str, ...]:
    include_splits = tuple(profile.include_splits or ())
    if include_splits and preview is not None:
        raise ValueError(
            f"Serve profile '{profile.name}' cannot combine preview with "
            "include_splits."
        )

    split = definition.dataset.split
    dataset_output_splits = split_output_labels(split) if split is not None else ()
    if include_splits:
        if split is None:
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

    if isinstance(operation, PipelineTask) and preview is None and not include_splits:
        return dataset_output_splits
    return include_splits


def _validate_output_collisions(profiles: Sequence[ResolvedRuntimeProfile]) -> None:
    output_owners: dict[str, str] = {}
    for profile in profiles:
        if profile.output.destination is None:
            continue
        destinations = (
            [
                (label, profile.output.for_split(label).destination)
                for label in profile.output_splits
            ]
            if profile.output_splits
            else [(None, profile.output.destination)]
        )
        for split_label, destination in destinations:
            assert destination is not None
            owner = f"profile '{profile.name}'"
            if split_label is not None:
                owner = f"{owner} split {split_label!r}"
            destination_key = output_destination_key(destination)
            previous = output_owners.get(destination_key)
            if previous is not None:
                raise ValueError(
                    f"Runtime outputs for {previous} and {owner} resolve to the "
                    f"same path '{destination}'."
                )
            output_owners[destination_key] = owner


def resolve_serve_profiles(
    definition: PipelineDefinition,
    profiles: Sequence[ServeProfile],
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
    shared_runs: dict[Path, RunPaths] = {}
    previews_by_root: dict[Path, PreviewStage | None] = {}

    resolved: list[ResolvedRuntimeProfile] = []
    for profile in profiles:
        resolved_preview = preview if preview is not None else profile.preview
        resolved_limit = limit if limit is not None else profile.limit
        output_splits = _resolve_serve_output_splits(
            definition,
            profile,
            resolved_preview,
            runtime_operations.get(profile.operation),
        )
        serve_root = resolve_output_directory(
            cli_output or profile.output,
            base_path=project_path.parent,
        )
        run_paths = None
        if serve_root is not None:
            if (
                serve_root in previews_by_root
                and previews_by_root[serve_root] != resolved_preview
            ):
                raise ValueError(
                    "Serve profiles sharing output directory "
                    f"'{serve_root}' must use the same preview."
                )
            previews_by_root[serve_root] = resolved_preview
            run_paths = shared_runs.get(serve_root)
            if run_paths is None:
                run_paths = get_run_paths(serve_root)
                shared_runs[serve_root] = run_paths

        target = resolve_output_target(
            cli_output=cli_output,
            config_output=profile.output,
            default=None,
            base_path=project_path.parent,
            profile_name=profile.name,
            run_paths=run_paths,
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
                preview=resolved_preview,
                limit=resolved_limit,
                throttle_ms=profile.throttle_ms,
                observability=observability,
                output=target,
                output_splits=output_splits,
            )
        )

    _validate_output_collisions(resolved)
    return resolved


def resolve_inspect_profiles(
    definition: PipelineDefinition,
    profiles: Sequence[InspectProfile],
    limit: int | None,
    cli_output: ServeOutputConfig | None,
    cli_log_level: str | None = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: str | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
) -> list[ResolvedRuntimeProfile]:
    project_path = definition.project.path
    resolved: list[ResolvedRuntimeProfile] = []
    for profile in profiles:
        target = resolve_output_target(
            cli_output=cli_output,
            config_output=profile.output,
            default=None,
            base_path=project_path.parent,
            profile_name=profile.name,
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
                preview=None,
                limit=limit,
                throttle_ms=None,
                observability=observability,
                output=target,
                output_splits=(),
            )
        )

    _validate_output_collisions(resolved)
    return resolved
