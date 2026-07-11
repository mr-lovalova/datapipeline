import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, Sequence, cast

from pydantic import ValidationError

from datapipeline.build.config_hash import compute_config_hash
from datapipeline.config.build_resolution import resolve_build_settings
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import (
    apply_profile_defaults,
    profile_specs_with_defaults,
)
from datapipeline.config.profiles import (
    BuildProfile,
    InspectProfile,
    ServeOutputConfig,
    ServeProfile,
    normalize_artifact_mode,
)
from datapipeline.config.resolution import (
    LogOutputTarget,
    resolve_execution_log_outputs,
)
from datapipeline.config.serve_resolution import resolve_run_profiles
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import OutputResolutionError
from datapipeline.profiles.models import (
    ExecutionProfile,
    ProfileKind,
    ProfileDataset,
    ProfileRunRequest,
    ServeRunPlan,
)
from datapipeline.profiles.selection import select_profiles
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.project_paths import tasks_dir
from datapipeline.services.executions import execution_root
from datapipeline.services.run_entries import RunEntry
from datapipeline.services.runs import RunPaths

logger = logging.getLogger(__name__)

_OUTPUT_MATRIX_HELP = (
    "Valid output combinations:\n"
    "  stdout: format=jsonl|txt\n"
    "          jsonl view=raw|flat, txt has no view\n"
    "          encoding is not supported\n"
    "  fs:     format=jsonl|csv|pickle|txt|html\n"
    "          encoding is supported only for jsonl/csv/txt (default utf-8)\n"
    "          jsonl supports view=raw|flat\n"
    "          csv supports view=flat\n"
    "          pickle supports view=raw\n"
    "          txt/html output does not support view\n"
    "          html output support depends on the selected runtime operation\n"
)


@dataclass(frozen=True)
class ProfileResolveParams:
    command: ProfileKind
    project_path: Path
    run_name: str | None
    force: bool
    artifact_mode: str | None
    limit: int | None
    preview_index: int | None
    output_transport: str | None
    output_format: str | None
    output_directory: str | None
    output_encoding: str | None
    output_view: str | None
    cli_log_level: str | None
    cli_log_outputs: Sequence[LogOutputTarget] | None
    base_log_level: str
    cli_visuals: str | None
    cli_heartbeat_interval_seconds: float | None
    workspace: WorkspaceContext | None


def build_cli_output_config(
    transport: str | None,
    fmt: str | None,
    directory: str | None,
    output_encoding: str | None = None,
    workspace: WorkspaceContext | None = None,
    view: str | None = None,
) -> ServeOutputConfig | None:
    if (
        transport is None
        and fmt is None
        and directory is None
        and view is None
        and output_encoding is None
    ):
        return None

    if not transport or not fmt:
        logger.error("--output-transport and --output-format must be provided together")
        raise SystemExit(2)
    transport = transport.lower()
    fmt = fmt.lower()

    config_kwargs: dict[str, object] = {
        "format": fmt,
        "view": view,
        "encoding": output_encoding,
    }
    if transport == "fs":
        if not directory:
            logger.error("--output-directory is required when --output-transport=fs")
            raise SystemExit(2)
        config_kwargs["transport"] = "fs"
        config_kwargs["directory"] = resolve_workspace_path(
            directory,
            workspace.root if workspace is not None else None,
        )
    else:
        config_kwargs["transport"] = "stdout"
    if transport != "fs" and directory:
        logger.error("--output-directory is only valid when --output-transport=fs")
        raise SystemExit(2)
    try:
        return ServeOutputConfig.model_validate(config_kwargs)
    except ValidationError as exc:
        logger.error("Invalid output configuration: %s", exc.errors()[0]["msg"])
        logger.error(_OUTPUT_MATRIX_HELP)
        raise SystemExit(2) from exc


def _select_profiles(params: ProfileResolveParams):
    try:
        profiles, defaults = profile_specs_with_defaults(
            params.project_path,
            cmd=params.command,
        )
    except Exception as exc:
        logger.error("Failed to load %s profiles: %s", params.command, exc)
        raise SystemExit(2) from exc
    if not profiles:
        logger.error("Project does not define %s profiles.", params.command)
        raise SystemExit(2)
    try:
        selected = select_profiles(
            profiles,
            params.run_name,
            params.command,
        )
        return [apply_profile_defaults(profile, defaults) for profile in selected]
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc


def _resolve_build_execution_profiles(
    profiles: Sequence[BuildProfile],
    params: ProfileResolveParams,
    execution_dir: Path,
) -> list[ExecutionProfile]:
    resolved: list[ExecutionProfile] = []
    for profile in profiles:
        try:
            settings = resolve_build_settings(
                project_path=params.project_path,
                cli_log_level=params.cli_log_level,
                cli_visuals=params.cli_visuals,
                cli_log_outputs=params.cli_log_outputs,
                cli_heartbeat_interval_seconds=params.cli_heartbeat_interval_seconds,
                force_flag=params.force,
                base_log_level=params.base_log_level,
                build_profile=profile,
            )
        except ValueError as exc:
            logger.error("Invalid build configuration: %s", exc)
            raise SystemExit(2) from exc
        log_output = resolve_execution_log_outputs(
            settings=settings.log_output,
            execution_dir=execution_dir,
            command=params.command,
            label=profile.name,
        )
        settings = replace(settings, log_output=log_output)
        resolved.append(
            ExecutionProfile(
                name=profile.name,
                target_id=profile.target,
                visuals=settings.visuals,
                log_decision=settings.log_decision,
                log_output=log_output,
                build_settings=settings,
            )
        )
    return resolved


def _resolve_runtime_execution_profiles(
    *,
    profiles: Sequence[ServeProfile | InspectProfile],
    params: ProfileResolveParams,
    execution_dir: Path,
) -> list[ExecutionProfile]:
    cli_output_cfg = build_cli_output_config(
        params.output_transport,
        params.output_format,
        params.output_directory,
        params.output_encoding,
        workspace=params.workspace,
        view=params.output_view,
    )
    try:
        runtime_profiles = resolve_run_profiles(
            project_path=params.project_path,
            run_entries=[
                RunEntry(
                    name=profile.name,
                    config=profile,
                    target_id=profile.target,
                    path=getattr(profile, "source_path", None),
                )
                for profile in profiles
            ],
            preview_index=params.preview_index,
            limit=params.limit,
            cli_output=cli_output_cfg,
            cli_log_level=params.cli_log_level,
            cli_log_outputs=params.cli_log_outputs,
            base_log_level=params.base_log_level,
            cli_visuals=params.cli_visuals,
            cli_heartbeat_interval_seconds=params.cli_heartbeat_interval_seconds,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc
    except OutputResolutionError as exc:
        logger.error("Invalid output configuration: %s", exc)
        raise SystemExit(2) from exc

    datasets: dict[str, ProfileDataset] = {
        "vectors": load_dataset(params.project_path, "vectors")
    }
    resolved: list[ExecutionProfile] = []
    for profile in runtime_profiles:
        dataset_name: Literal["vectors", "features"] = (
            "vectors" if profile.preview_index is None else "features"
        )
        dataset = datasets.get(dataset_name)
        if dataset is None:
            dataset = load_dataset(params.project_path, dataset_name)
            datasets[dataset_name] = dataset
        resolved.append(
            ExecutionProfile(
                name=profile.label,
                target_id=profile.entry.target_id,
                visuals=profile.visuals.visuals,
                log_decision=profile.log_decision,
                log_output=resolve_execution_log_outputs(
                    settings=profile.log_output,
                    execution_dir=execution_dir,
                    command=params.command,
                    label=profile.label,
                ),
                runtime=profile.runtime,
                dataset=dataset,
                limit=profile.limit,
                output=profile.output,
                throttle_ms=profile.throttle_ms,
                preview_index=profile.preview_index,
                heartbeat_interval_seconds=profile.heartbeat_interval_seconds,
            )
        )
    return resolved


def _serve_run_plans(
    profiles: Sequence[ExecutionProfile],
) -> tuple[ServeRunPlan, ...]:
    plans_by_run: dict[RunPaths, ServeRunPlan] = {}
    for profile in profiles:
        if profile.output is None or profile.output.run is None:
            continue
        if profile.output.run not in plans_by_run:
            plans_by_run[profile.output.run] = ServeRunPlan(
                paths=profile.output.run,
                preview_index=profile.preview_index,
            )
    return tuple(plans_by_run.values())


def build_profile_run_request(
    *,
    kind: ProfileKind,
    project: str,
    run_name: str | None = None,
    force: bool = False,
    artifact_mode: str | None = None,
    limit: int | None = None,
    preview_index: int | None = None,
    output_transport: str | None = None,
    output_format: str | None = None,
    output_directory: str | None = None,
    output_encoding: str | None = None,
    output_view: str | None = None,
    cli_log_level: str | None = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: str | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
    workspace: WorkspaceContext | None = None,
) -> ProfileRunRequest | None:
    project_path = Path(project).resolve()
    params = ProfileResolveParams(
        command=kind,
        project_path=project_path,
        run_name=run_name,
        force=force,
        artifact_mode=artifact_mode,
        limit=limit,
        preview_index=preview_index,
        output_transport=output_transport,
        output_format=output_format,
        output_directory=output_directory,
        output_encoding=output_encoding,
        output_view=output_view,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
        cli_visuals=cli_visuals,
        cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
        workspace=workspace,
    )
    selected_profiles = _select_profiles(params)
    if not selected_profiles:
        return None

    try:
        config_hash = compute_config_hash(
            project_path,
            tasks_dir(project_path),
        )
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to load pipeline inputs: %s", exc)
        raise SystemExit(2) from exc

    try:
        declared_artifact_tasks, declared_operation_tasks = operation_specs(
            project_path
        )
    except Exception as exc:
        logger.error("Failed to load task definitions: %s", exc)
        raise SystemExit(2) from exc
    declared_tasks = list(declared_artifact_tasks) + list(declared_operation_tasks)

    tasks_by_id = {task.id: task for task in declared_tasks}
    for profile in selected_profiles:
        task = tasks_by_id.get(profile.target)
        if task is None:
            logger.error(
                "%s profile '%s' references unknown task target '%s'.",
                kind.capitalize(),
                profile.name,
                profile.target,
            )
            raise SystemExit(2)
        if kind == "build" and not isinstance(task, ArtifactTask):
            logger.error(
                "Build profile '%s' must target an artifact task; '%s' is a "
                "runtime task.",
                profile.name,
                profile.target,
            )
            raise SystemExit(2)
        if kind != "build" and not isinstance(task, OperationTask):
            logger.error(
                "%s profile '%s' must target a runtime task; '%s' is an artifact task.",
                kind.capitalize(),
                profile.name,
                profile.target,
            )
            raise SystemExit(2)

    execution_dir = execution_root(project_path)
    resolved_artifact_mode: str | None = None
    if kind == "build":
        profiles = _resolve_build_execution_profiles(
            cast(Sequence[BuildProfile], selected_profiles),
            params,
            execution_dir,
        )
    else:
        if artifact_mode is not None:
            try:
                resolved_artifact_mode = normalize_artifact_mode(artifact_mode)
            except ValueError as exc:
                logger.error("Invalid artifact mode: %s", exc)
                raise SystemExit(2) from exc
        else:
            profile_modes = {
                profile.artifact_mode or "AUTO" for profile in selected_profiles
            }
            if len(profile_modes) != 1:
                configured = ", ".join(
                    f"{profile.name}={profile.artifact_mode or 'AUTO'}"
                    for profile in selected_profiles
                )
                logger.error(
                    "Selected %s profiles disagree on artifact_mode: %s.",
                    kind,
                    configured,
                )
                raise SystemExit(2)
            resolved_artifact_mode = profile_modes.pop()
        profiles = _resolve_runtime_execution_profiles(
            profiles=cast(Sequence[ServeProfile | InspectProfile], selected_profiles),
            params=params,
            execution_dir=execution_dir,
        )

    if not profiles:
        return None

    try:
        current_config_hash = compute_config_hash(
            project_path,
            tasks_dir(project_path),
        )
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to verify pipeline inputs: %s", exc)
        raise SystemExit(2) from exc
    if current_config_hash != config_hash:
        logger.error("Pipeline inputs changed while profiles were being resolved.")
        raise SystemExit(2)

    return ProfileRunRequest(
        command=kind,
        project_path=project_path,
        tasks=declared_tasks,
        artifact_task_configs=declared_artifact_tasks,
        profiles=profiles,
        config_hash=config_hash,
        artifact_mode=resolved_artifact_mode,
        artifact_heartbeat_interval_seconds=(
            cli_heartbeat_interval_seconds if kind != "build" else None
        ),
        serve_run_plans=_serve_run_plans(profiles) if kind == "serve" else (),
    )
