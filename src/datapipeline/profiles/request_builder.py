import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from datapipeline.build.config_hash import compute_config_hash
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
    MaterializeProfile,
    MaterializeProfileDefaults,
    Profile,
    ProfileCommand,
    ProfileDefaults,
    ServeOutputConfig,
    ServeProfile,
    normalize_artifact_mode,
)
from datapipeline.config.preview import PreviewStage
from datapipeline.config.resolution import (
    LogOutputTarget,
    resolve_execution_log_outputs,
    resolve_observability_settings,
)
from datapipeline.config.serve_resolution import resolve_runtime_profiles
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.io.output import OutputResolutionError
from datapipeline.profiles.models import (
    BuildJob,
    BuildRunRequest,
    MaterializeRunRequest,
    ProfileRunRequest,
    RuntimeJob,
    RuntimeRunRequest,
    ServeRunPlan,
    TaskProfileKind,
)
from datapipeline.profiles.materialize import resolve_materialize_jobs
from datapipeline.profiles.selection import select_profiles
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.project_paths import tasks_dir
from datapipeline.services.executions import execution_root
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
    command: TaskProfileKind
    project_path: Path
    run_name: str | None
    force: bool
    limit: int | None
    preview: PreviewStage | None
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
    elif transport == "stdout":
        config_kwargs["transport"] = "stdout"
    else:
        logger.error("--output-transport must be 'fs' or 'stdout'")
        raise SystemExit(2)
    if transport != "fs" and directory:
        logger.error("--output-directory is only valid when --output-transport=fs")
        raise SystemExit(2)
    try:
        return ServeOutputConfig.model_validate(config_kwargs)
    except ValidationError as exc:
        logger.error("Invalid output configuration: %s", exc.errors()[0]["msg"])
        logger.error(_OUTPUT_MATRIX_HELP)
        raise SystemExit(2) from exc


def _select_profiles(
    project_path: Path,
    command: ProfileCommand,
    run_name: str | None,
) -> tuple[list[Profile], ProfileDefaults]:
    try:
        profiles, defaults = profile_specs_with_defaults(
            project_path,
            cmd=command,
        )
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to load %s profiles: %s", command, exc)
        raise SystemExit(2) from exc
    if not profiles:
        logger.error("Project does not define %s profiles.", command)
        raise SystemExit(2)
    try:
        selected = select_profiles(
            profiles,
            run_name,
            command,
        )
        return (
            [apply_profile_defaults(profile, defaults) for profile in selected],
            defaults,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc


def _load_pipeline_config_hash(project_path: Path) -> str:
    try:
        return compute_config_hash(project_path, tasks_dir(project_path))
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to load pipeline inputs: %s", exc)
        raise SystemExit(2) from exc


def _verify_pipeline_config_hash(project_path: Path, expected: str) -> None:
    try:
        current = compute_config_hash(project_path, tasks_dir(project_path))
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to verify pipeline inputs: %s", exc)
        raise SystemExit(2) from exc
    if current != expected:
        logger.error("Pipeline inputs changed while profiles were being resolved.")
        raise SystemExit(2)


def _resolve_build_jobs(
    profiles: Sequence[BuildProfile],
    params: ProfileResolveParams,
    execution_dir: Path,
    tasks_by_id: dict[str, ArtifactTask],
) -> list[BuildJob]:
    jobs: list[BuildJob] = []
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
        settings = replace(
            settings,
            observability=replace(
                settings.observability,
                log_output=resolve_execution_log_outputs(
                    settings=settings.observability.log_output,
                    execution_dir=execution_dir,
                    command=params.command,
                    label=profile.name,
                ),
            ),
        )
        jobs.append(BuildJob(task=tasks_by_id[profile.target], settings=settings))
    return jobs


def _resolve_runtime_jobs(
    profiles: Sequence[ServeProfile | InspectProfile],
    params: ProfileResolveParams,
    execution_dir: Path,
    tasks_by_id: dict[str, OperationTask],
) -> list[RuntimeJob]:
    cli_output_cfg = build_cli_output_config(
        params.output_transport,
        params.output_format,
        params.output_directory,
        params.output_encoding,
        workspace=params.workspace,
        view=params.output_view,
    )
    try:
        runtime_profiles = resolve_runtime_profiles(
            project_path=params.project_path,
            profiles=profiles,
            preview=params.preview,
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

    dataset = load_dataset(params.project_path)
    jobs: list[RuntimeJob] = []
    for profile in runtime_profiles:
        jobs.append(
            RuntimeJob(
                name=profile.name,
                task=tasks_by_id[profile.target_id],
                runtime=profile.runtime,
                dataset=dataset,
                output=profile.output,
                observability=replace(
                    profile.observability,
                    log_output=resolve_execution_log_outputs(
                        settings=profile.observability.log_output,
                        execution_dir=execution_dir,
                        command=params.command,
                        label=profile.name,
                    ),
                ),
                limit=profile.limit,
                throttle_ms=profile.throttle_ms,
                preview=profile.preview,
                splits=profile.splits,
            )
        )
    return jobs


def _serve_run_plans(
    jobs: Sequence[RuntimeJob],
) -> tuple[ServeRunPlan, ...]:
    plans_by_run: dict[RunPaths, ServeRunPlan] = {}
    for job in jobs:
        if job.output.run is None:
            continue
        if job.output.run not in plans_by_run:
            plans_by_run[job.output.run] = ServeRunPlan(
                paths=job.output.run,
                preview=job.preview,
            )
    return tuple(plans_by_run.values())


def build_profile_run_request(
    *,
    kind: TaskProfileKind,
    project: str,
    run_name: str | None = None,
    force: bool = False,
    artifact_mode: str | None = None,
    limit: int | None = None,
    preview: PreviewStage | None = None,
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
        limit=limit,
        preview=preview,
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
    loaded_profiles, defaults = _select_profiles(project_path, kind, run_name)
    if not loaded_profiles:
        return None
    selected_profiles: list[BuildProfile | InspectProfile | ServeProfile] = []
    for profile in loaded_profiles:
        if not isinstance(profile, BuildProfile | InspectProfile | ServeProfile):
            raise TypeError("Profile loading returned an unsupported profile type")
        selected_profiles.append(profile)

    config_hash = _load_pipeline_config_hash(project_path)

    try:
        declared_artifact_tasks, declared_operation_tasks = operation_specs(
            project_path
        )
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to load task definitions: %s", exc)
        raise SystemExit(2) from exc
    artifact_tasks_by_id = {task.id: task for task in declared_artifact_tasks}
    operation_tasks_by_id = {task.id: task for task in declared_operation_tasks}
    tasks_by_id = artifact_tasks_by_id | operation_tasks_by_id
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
    if kind == "build":
        build_profiles = [
            profile
            for profile in selected_profiles
            if isinstance(profile, BuildProfile)
        ]
        if len(build_profiles) != len(selected_profiles):
            raise TypeError("Build profile loading returned the wrong profile type")
        build_jobs = _resolve_build_jobs(
            build_profiles,
            params,
            execution_dir,
            artifact_tasks_by_id,
        )
        if not build_jobs:
            return None
        request: ProfileRunRequest = BuildRunRequest(
            project_path=project_path,
            artifact_task_configs=declared_artifact_tasks,
            jobs=build_jobs,
            execution=defaults.execution,
            config_hash=config_hash,
        )
    else:
        runtime_profiles = [
            profile
            for profile in selected_profiles
            if isinstance(profile, ServeProfile | InspectProfile)
        ]
        if len(runtime_profiles) != len(selected_profiles):
            raise TypeError("Runtime profile loading returned the wrong profile type")
        if artifact_mode is not None:
            try:
                resolved_artifact_mode = normalize_artifact_mode(artifact_mode)
            except ValueError as exc:
                logger.error("Invalid artifact mode: %s", exc)
                raise SystemExit(2) from exc
        else:
            profile_modes = {
                profile.artifact_mode or "AUTO" for profile in runtime_profiles
            }
            if len(profile_modes) != 1:
                configured = ", ".join(
                    f"{profile.name}={profile.artifact_mode or 'AUTO'}"
                    for profile in runtime_profiles
                )
                logger.error(
                    "Selected %s profiles disagree on artifact_mode: %s.",
                    kind,
                    configured,
                )
                raise SystemExit(2)
            resolved_artifact_mode = profile_modes.pop()
        try:
            artifact_observability = resolve_observability_settings(
                project_path,
                defaults.observability,
                cli_visuals=cli_visuals,
                cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
                cli_log_level=cli_log_level,
                cli_log_outputs=cli_log_outputs,
                base_log_level=base_log_level,
            )
        except ValueError as exc:
            logger.error("Invalid prerequisite observability: %s", exc)
            raise SystemExit(2) from exc
        artifact_settings = BuildSettings(
            mode=resolved_artifact_mode or "AUTO",
            observability=replace(
                artifact_observability,
                log_output=resolve_execution_log_outputs(
                    artifact_observability.log_output,
                    execution_dir,
                    command=kind,
                    label="artifacts",
                ),
            ),
        )
        runtime_jobs = _resolve_runtime_jobs(
            runtime_profiles,
            params,
            execution_dir,
            operation_tasks_by_id,
        )
        if not runtime_jobs:
            return None
        request = RuntimeRunRequest(
            command=kind,
            project_path=project_path,
            artifact_task_configs=declared_artifact_tasks,
            jobs=runtime_jobs,
            execution=defaults.execution,
            config_hash=config_hash,
            artifact_settings=artifact_settings,
            serve_run_plans=(_serve_run_plans(runtime_jobs) if kind == "serve" else ()),
        )

    _verify_pipeline_config_hash(project_path, config_hash)
    return request


def build_materialize_run_request(
    project: str,
    run_name: str | None,
    overwrite: bool | None,
    output: Path | None,
    artifact_mode: str | None,
    cli_log_level: str | None,
    cli_log_outputs: Sequence[LogOutputTarget],
    base_log_level: str,
    cli_visuals: str | None,
    cli_heartbeat_interval_seconds: float | None,
) -> MaterializeRunRequest | None:
    project_path = Path(project).resolve()
    loaded_profiles, defaults = _select_profiles(
        project_path,
        "materialize",
        run_name,
    )
    materialize_profiles = [
        profile
        for profile in loaded_profiles
        if isinstance(profile, MaterializeProfile)
    ]
    if len(materialize_profiles) != len(loaded_profiles):
        raise TypeError("Materialize profile loading returned the wrong profile type")
    if not isinstance(defaults, MaterializeProfileDefaults):
        raise TypeError("Materialize profile loading returned the wrong defaults type")
    if not materialize_profiles:
        return None

    config_hash = _load_pipeline_config_hash(project_path)
    try:
        artifact_tasks, _ = operation_specs(project_path)
        runtime = bootstrap(project_path)
        jobs = resolve_materialize_jobs(
            profiles=materialize_profiles,
            project_path=project_path,
            overwrite=overwrite,
            cli_output=output,
            cli_visuals=cli_visuals,
            cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
        resolved_artifact_mode = (
            normalize_artifact_mode(artifact_mode) or defaults.artifact_mode or "AUTO"
        )
        artifact_observability = resolve_observability_settings(
            project_path,
            defaults.observability,
            cli_visuals=cli_visuals,
            cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
        )
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Invalid materialize configuration: %s", exc)
        raise SystemExit(2) from exc

    execution_dir = execution_root(project_path)
    artifact_settings = BuildSettings(
        mode=resolved_artifact_mode,
        observability=replace(
            artifact_observability,
            log_output=resolve_execution_log_outputs(
                artifact_observability.log_output,
                execution_dir,
                command="materialize",
                label="artifacts",
            ),
        ),
    )
    request = MaterializeRunRequest(
        project_path=project_path,
        artifact_task_configs=artifact_tasks,
        jobs=jobs,
        execution=defaults.execution,
        config_hash=config_hash,
        artifact_settings=artifact_settings,
        runtime=runtime,
    )
    _verify_pipeline_config_hash(project_path, config_hash)
    return request
