import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Optional, Sequence, TypeAlias

from pydantic import ValidationError

from datapipeline.cli.commands.run_config import resolve_runtime_entries
from datapipeline.config.build_resolution import resolve_build_settings
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import profile_specs
from datapipeline.config.profiles import ServeOutputConfig
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.serve_resolution import resolve_run_profiles
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import OutputResolutionError
from datapipeline.profiles.models import (
    BuildExecutionProfile,
    ExecutionProfile,
    ProfileDataset,
    ProfileRunRequest,
    RuntimeBuildOptions,
    RuntimeKind,
    RuntimeExecutionProfile,
)
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.run_artifacts import write_profile_artifact

logger = logging.getLogger(__name__)

ProfileKind = Literal["serve", "build", "inspect"]

_OUTPUT_MATRIX_HELP = (
    "Valid output combinations:\n"
    "  stdout: format=jsonl, view=flat|raw|values\n"
    "          encoding is not supported\n"
    "  fs:     format=jsonl|csv|pickle, view=flat|raw|values\n"
    "          encoding is supported only for jsonl/csv (default utf-8)\n"
    "          csv supports view=flat|values\n"
)


@dataclass(frozen=True)
class ProfileResolveParams:
    project_path: Path
    run_name: Optional[str]
    force: bool
    build_mode: Optional[str]
    limit: Optional[int]
    keep: Optional[str]
    stage: Optional[int]
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

ProfileResolver: TypeAlias = Callable[[ProfileResolveParams], list[ExecutionProfile]]


def _profile_debug_payload(profile) -> dict[str, object]:
    entry = profile.entry
    payload: dict[str, object] = {
        "label": profile.label,
        "idx": profile.idx,
        "total": profile.total,
        "entry": {
            "name": entry.name,
            "path": str(entry.path) if entry.path else None,
        },
        "stage": profile.stage,
        "limit": profile.limit,
        "throttle_ms": profile.throttle_ms,
        "log_level": {
            "name": profile.log_decision.name,
            "value": profile.log_decision.value,
        },
        "log_output": {
            "outputs": [
                {
                    "transport": target.transport,
                    "scope": target.scope,
                    "destination": (
                        str(target.destination)
                        if target.destination is not None
                        else None
                    ),
                }
                for target in profile.log_output.outputs
            ],
        },
        "visuals": {
            "provider": profile.visuals.visuals,
        },
        "output": {
            "transport": profile.output.transport,
            "format": profile.output.format,
            "view": profile.output.view,
            "encoding": profile.output.encoding,
            "destination": str(profile.output.destination)
            if profile.output.destination
            else None,
        },
    }
    cfg = entry.config
    if cfg is not None:
        payload["run_config"] = cfg.model_dump(
            exclude_unset=True, exclude_none=True
        )
    operation = entry.operation
    payload["operation"] = {
        "id": operation.id,
        "entrypoint": operation.entrypoint,
        "dependencies": list(operation.dependencies),
    }
    return payload


def _profile_artifact_writer(
    profile_kind: RuntimeKind,
    profile_name: str,
    output,
) -> Callable[[Mapping[str, Any]], Path | None]:
    def _write(payload: Mapping[str, Any]) -> Path | None:
        run_paths = getattr(output, "run", None)
        if run_paths is None:
            return None
        return write_profile_artifact(
            paths=run_paths,
            profile_kind=profile_kind,
            profile_name=profile_name,
            payload=payload,
        )

    return _write


def _runtime_execution_profile(
    profile_kind: RuntimeKind,
    payload: dict[str, object],
    profile,
    params: ProfileResolveParams,
    datasets: dict[str, ProfileDataset],
) -> RuntimeExecutionProfile:
    return RuntimeExecutionProfile(
        kind=profile_kind,
        name=profile.label,
        target_id=profile.entry.operation.id,
        visuals=profile.visuals.visuals or "on",
        log_decision=profile.log_decision,
        log_output=profile.log_output,
        runtime=profile.runtime,
        sections=(f"{profile_kind.capitalize()} Profiles",),
        label=profile.label,
        artifact_payload=payload,
        artifact_writer=_profile_artifact_writer(
            profile_kind=profile_kind,
            profile_name=profile.label,
            output=profile.output,
        ),
        dataset=_load_dataset_for_profile(
            cache=datasets,
            project_path=params.project_path,
            stage=profile.stage,
        ),
        limit=profile.limit,
        output=profile.output,
        throttle_ms=profile.throttle_ms,
        stage=profile.stage,
        skip_artifacts=params.skip_build,
        build_options=RuntimeBuildOptions(
            build_mode=profile.build_mode,
            cli_log_level=params.cli_log_level,
            cli_visuals=params.cli_visuals,
            cli_log_outputs=tuple(params.cli_log_outputs or ()),
            workspace=params.workspace,
        ),
    )


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
    if transport == "fs":
        if not directory:
            logger.error("--output-directory is required when --output-transport=fs")
            raise SystemExit(2)
        resolved_directory = resolve_workspace_path(
            directory,
            workspace.root if workspace is not None else None,
        )
        try:
            return ServeOutputConfig(
                transport="fs",
                format=fmt,
                view=view,
                encoding=output_encoding,
                directory=resolved_directory,
            )
        except ValidationError as exc:
            logger.error("Invalid output configuration: %s", exc.errors()[0]["msg"])
            logger.error(_OUTPUT_MATRIX_HELP)
            raise SystemExit(2) from exc
    if directory:
        logger.error("--output-directory is only valid when --output-transport=fs")
        raise SystemExit(2)
    try:
        return ServeOutputConfig(
            transport="stdout",
            format=fmt,
            view=view,
            encoding=output_encoding,
        )
    except ValidationError as exc:
        logger.error("Invalid output configuration: %s", exc.errors()[0]["msg"])
        logger.error(_OUTPUT_MATRIX_HELP)
        raise SystemExit(2) from exc


def _load_dataset_for_profile(
    cache: dict[str, ProfileDataset],
    project_path: Path,
    stage: Optional[int],
) -> ProfileDataset:
    dataset_name = "vectors" if stage is None else "features"
    dataset = cache.get(dataset_name)
    if dataset is None:
        dataset = load_dataset(project_path, dataset_name)
        cache[dataset_name] = dataset
    return dataset


def _resolve_build_profiles(params: ProfileResolveParams) -> list[ExecutionProfile]:
    try:
        _, build_specs, _ = profile_specs(params.project_path)
        available_profiles = list(build_specs)
    except Exception as exc:
        logger.error("Failed to load build profiles: %s", exc)
        raise SystemExit(2) from exc

    declared_profiles = bool(available_profiles)
    if not declared_profiles:
        logger.error("Project does not define build profiles.")
        raise SystemExit(2)

    if params.run_name:
        available_profiles = [
            profile
            for profile in available_profiles
            if profile.name == params.run_name
        ]
        if not available_profiles:
            logger.error("Unknown build profile '%s'", params.run_name)
            raise SystemExit(2)
    elif declared_profiles:
        available_profiles = [
            profile for profile in available_profiles if profile.enabled
        ]

    resolved: list[ExecutionProfile] = []
    for profile in available_profiles:
        try:
            settings = resolve_build_settings(
                project_path=params.project_path,
                workspace=params.workspace,
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
        resolved.append(
            BuildExecutionProfile(
                kind="build",
                name=profile.name,
                target_id=profile.target,
                visuals=settings.visuals or "on",
                log_decision=settings.log_decision,
                log_output=settings.log_output,
                sections=("Build Profiles",),
                label=profile.name,
                build_settings=settings,
            ),
        )
    return resolved


def _resolve_runtime_profiles(
    *,
    kind: RuntimeKind,
    params: ProfileResolveParams,
) -> list[ExecutionProfile]:
    run_entries = resolve_runtime_entries(
        params.project_path,
        params.run_name,
        kind=kind,
    )
    if not run_entries:
        return []

    cli_output_cfg = build_cli_output_config(
        params.output_transport,
        params.output_format,
        params.output_directory,
        params.output_encoding,
        workspace=params.workspace,
        view=params.output_view,
    )
    keep = params.keep if kind == "serve" else None
    stage = params.stage if kind == "serve" else None
    create_run = kind == "serve"
    try:
        runtime_profiles = resolve_run_profiles(
            project_path=params.project_path,
            run_entries=run_entries,
            keep=keep,
            stage=stage,
            limit=params.limit,
            cli_build_mode=params.build_mode,
            cli_output=cli_output_cfg,
            workspace=params.workspace,
            cli_log_level=params.cli_log_level,
            cli_log_outputs=params.cli_log_outputs,
            base_log_level=params.base_log_level,
            cli_visuals=params.cli_visuals,
            create_run=create_run,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc
    except OutputResolutionError as exc:
        logger.error("Invalid output configuration: %s", exc)
        raise SystemExit(2) from exc

    return _runtime_profiles_to_execution(
        kind=kind,
        profiles=runtime_profiles,
        params=params,
    )


def _runtime_profiles_to_execution(
    *,
    kind: RuntimeKind,
    profiles,
    params: ProfileResolveParams,
) -> list[ExecutionProfile]:
    datasets: dict[str, ProfileDataset] = {
        "vectors": load_dataset(params.project_path, "vectors")
    }
    resolved: list[ExecutionProfile] = []
    for profile in profiles:
        payload = _profile_debug_payload(profile)
        resolved.append(
            _runtime_execution_profile(
                profile_kind=kind,
                payload=payload,
                profile=profile,
                params=params,
                datasets=datasets,
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
    stage: Optional[int] = None,
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
    params = ProfileResolveParams(
        project_path=project_path,
        run_name=run_name,
        force=force,
        build_mode=build_mode,
        limit=limit,
        keep=keep,
        stage=stage,
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
    resolvers: dict[ProfileKind, ProfileResolver] = {
        "build": _resolve_build_profiles,
        "serve": lambda current: _resolve_runtime_profiles(kind="serve", params=current),
        "inspect": lambda current: _resolve_runtime_profiles(kind="inspect", params=current),
    }
    profiles = resolvers[kind](params)
    if not profiles:
        return None

    try:
        declared_artifact_tasks, declared_operation_tasks = operation_specs(project_path)
        declared_tasks = list(declared_artifact_tasks) + list(declared_operation_tasks)
    except Exception as exc:
        logger.error("Failed to load task definitions: %s", exc)
        raise SystemExit(2) from exc

    return ProfileRunRequest(
        project_path=project_path,
        tasks=declared_tasks,
        artifact_task_configs=declared_artifact_tasks,
        profiles=profiles,
    )
