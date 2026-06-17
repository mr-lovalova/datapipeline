import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, cast

from pydantic import ValidationError

from datapipeline.config.build_resolution import BuildSettings, resolve_build_settings
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
)
from datapipeline.config.resolution import (
    LogOutputTarget,
    materialize_log_output_for_execution,
)
from datapipeline.config.serve_resolution import resolve_run_profiles
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import OutputResolutionError
from datapipeline.profiles.models import (
    ExecutionProfile,
    ProfileKind,
    ProfileDataset,
    ProfileRunRequest,
)
from datapipeline.profiles.reporting import runtime_profile_report_payload
from datapipeline.profiles.selection import select_profiles
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.executions import (
    ExecutionPaths,
    get_execution_paths,
    start_execution,
)
from datapipeline.services.run_entries import RunEntry

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
    run_name: Optional[str]
    force: bool
    build_mode: Optional[str]
    limit: Optional[int]
    keep: Optional[str]
    preview_index: Optional[int]
    output_transport: Optional[str]
    output_format: Optional[str]
    output_directory: Optional[str]
    output_encoding: Optional[str]
    output_view: Optional[str]
    skip_build: bool
    cli_log_level: Optional[str]
    cli_log_outputs: Sequence[LogOutputTarget] | None
    base_log_level: str
    cli_visuals: Optional[str]
    workspace: WorkspaceContext | None

def build_cli_output_config(
    transport: Optional[str],
    fmt: Optional[str],
    directory: Optional[str],
    output_encoding: Optional[str] = None,
    workspace=None,
    view: Optional[str] = None,
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
    execution: ExecutionPaths,
) -> list[ExecutionProfile]:
    resolved: list[ExecutionProfile] = []
    for profile in profiles:
        try:
            settings = resolve_build_settings(
                project_path=params.project_path,
                cli_log_level=params.cli_log_level,
                cli_visuals=params.cli_visuals,
                cli_log_outputs=params.cli_log_outputs,
                force_flag=params.force,
                base_log_level=params.base_log_level,
                build_profile=profile,
            )
        except ValueError as exc:
            logger.error("Invalid log output configuration: %s", exc)
            raise SystemExit(2) from exc
        log_output = materialize_log_output_for_execution(
            settings=settings.log_output,
            execution_dir=execution.root,
            command=params.command,
            label=profile.name,
        )
        resolved.append(
            ExecutionProfile(
                name=profile.name,
                target_id=profile.target,
                visuals=settings.visuals or "on",
                log_decision=settings.log_decision,
                log_output=log_output,
                sections=("Build Profiles",),
                label=profile.name,
                execution=execution,
                build_settings=BuildSettings(
                    visuals=settings.visuals,
                    log_decision=settings.log_decision,
                    log_output=log_output,
                    mode=settings.mode,
                    force=settings.force,
                    profile_name=settings.profile_name,
                ),
            )
        )
    return resolved


def _resolve_runtime_execution_profiles(
    *,
    profiles: Sequence[ServeProfile | InspectProfile],
    params: ProfileResolveParams,
    execution: ExecutionPaths,
    managed_run_targets: set[str] | None = None,
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
            keep=params.keep,
            preview_index=params.preview_index,
            limit=params.limit,
            cli_build_mode=params.build_mode,
            cli_output=cli_output_cfg,
            cli_log_level=params.cli_log_level,
            cli_log_outputs=params.cli_log_outputs,
            base_log_level=params.base_log_level,
            cli_visuals=params.cli_visuals,
            managed_run_targets=managed_run_targets,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc
    except OutputResolutionError as exc:
        logger.error("Invalid output configuration: %s", exc)
        raise SystemExit(2) from exc

    datasets: dict[str, ProfileDataset] = {"vectors": load_dataset(params.project_path, "vectors")}
    resolved: list[ExecutionProfile] = []
    for profile in runtime_profiles:
        dataset_name = "vectors" if profile.preview_index is None else "features"
        dataset = datasets.get(dataset_name)
        if dataset is None:
            dataset = load_dataset(params.project_path, dataset_name)
            datasets[dataset_name] = dataset
        resolved.append(
            ExecutionProfile(
                name=profile.label,
                target_id=profile.entry.target_id,
                visuals=profile.visuals.visuals or "on",
                log_decision=profile.log_decision,
                log_output=materialize_log_output_for_execution(
                    settings=profile.log_output,
                    execution_dir=execution.root,
                    command=params.command,
                    label=profile.label,
                ),
                runtime=profile.runtime,
                sections=(f"{params.command.capitalize()} Profiles",),
                label=profile.label,
                profile_report=runtime_profile_report_payload(profile),
                dataset=dataset,
                execution=execution,
                limit=profile.limit,
                output=profile.output,
                throttle_ms=profile.throttle_ms,
                preview_index=profile.preview_index,
                build_mode=profile.build_mode,
            )
        )
    return resolved


def build_profile_run_request(
    *,
    kind: ProfileKind,
    project: str,
    run_name: Optional[str] = None,
    force: bool = False,
    build_mode: Optional[str] = None,
    limit: Optional[int] = None,
    keep: Optional[str] = None,
    preview_index: Optional[int] = None,
    output_transport: Optional[str] = None,
    output_format: Optional[str] = None,
    output_directory: Optional[str] = None,
    output_encoding: Optional[str] = None,
    output_view: Optional[str] = None,
    skip_build: bool = False,
    cli_log_level: Optional[str] = None,
    cli_log_outputs: Sequence[LogOutputTarget] | None = None,
    base_log_level: str = "INFO",
    cli_visuals: Optional[str] = None,
    workspace: WorkspaceContext | None = None,
) -> ProfileRunRequest | None:
    project_path = Path(project).resolve()
    try:
        declared_artifact_tasks, declared_operation_tasks = operation_specs(project_path)
    except Exception as exc:
        logger.error("Failed to load task definitions: %s", exc)
        raise SystemExit(2) from exc
    declared_tasks = list(declared_artifact_tasks) + list(declared_operation_tasks)

    params = ProfileResolveParams(
        command=kind,
        project_path=project_path,
        run_name=run_name,
        force=force,
        build_mode=build_mode,
        limit=limit,
        keep=keep,
        preview_index=preview_index,
        output_transport=output_transport,
        output_format=output_format,
        output_directory=output_directory,
        output_encoding=output_encoding,
        output_view=output_view,
        skip_build=skip_build,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
        cli_visuals=cli_visuals,
        workspace=workspace,
    )
    selected_profiles = _select_profiles(params)
    if not selected_profiles:
        return None

    execution = get_execution_paths(project_path)
    if kind == "build":
        profiles = _resolve_build_execution_profiles(
            cast(Sequence[BuildProfile], selected_profiles),
            params,
            execution,
        )
    else:
        managed_run_targets = (
            {task.id for task in declared_operation_tasks} if kind == "serve" else None
        )
        profiles = _resolve_runtime_execution_profiles(
            profiles=cast(Sequence[ServeProfile | InspectProfile], selected_profiles),
            params=params,
            execution=execution,
            managed_run_targets=managed_run_targets,
        )

    if not profiles:
        return None

    start_execution(execution, project_yaml=project_path, command=kind)

    return ProfileRunRequest(
        command=kind,
        project_path=project_path,
        tasks=declared_tasks,
        artifact_task_configs=declared_artifact_tasks,
        profiles=profiles,
        skip_build=params.skip_build,
    )
