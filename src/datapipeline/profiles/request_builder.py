import logging
from dataclasses import replace
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from datapipeline.artifacts.settings import BuildSettings
from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.config.preview import PreviewStage
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
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.execution.settings import (
    LogOutputTarget,
    resolve_execution_log_outputs,
    resolve_observability_settings,
)
from datapipeline.io.output import OutputResolutionError
from datapipeline.profiles.loader import (
    apply_profile_defaults,
    profile_specs_with_defaults,
)
from datapipeline.profiles.materialize import resolve_materialize_jobs
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
from datapipeline.profiles.runtime_profiles import resolve_runtime_profiles
from datapipeline.services.definitions import ProjectManifest
from datapipeline.services.executions import execution_root
from datapipeline.services.path_policy import (
    resolve_workspace_path,
    sanitize_path_segment,
)
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime
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
    project: ProjectManifest,
    command: ProfileCommand,
    run_name: str | None,
) -> tuple[list[Profile], ProfileDefaults]:
    try:
        profiles, defaults = profile_specs_with_defaults(
            project,
            cmd=command,
        )
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Failed to load %s profiles: %s", command, exc)
        raise SystemExit(2) from exc
    if not profiles:
        logger.error("Project does not define %s profiles.", command)
        raise SystemExit(2)
    if run_name is None:
        selected = [profile for profile in profiles if profile.enabled]
    else:
        normalized_name = run_name.strip()
        if not normalized_name:
            logger.error("%s profile name must not be empty.", command.capitalize())
            raise SystemExit(2)
        selected = [profile for profile in profiles if profile.name == normalized_name]
        if not selected:
            logger.error("Unknown %s profile '%s'", command, normalized_name)
            raise SystemExit(2)
    try:
        return (
            [apply_profile_defaults(profile, defaults) for profile in selected],
            defaults,
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc


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
    project_path = Path(project)
    try:
        definition = load_pipeline(project_path)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        logger.error("Failed to load pipeline inputs: %s", exc)
        raise SystemExit(2) from exc
    project_path = definition.project.path
    loaded_profiles, defaults = _select_profiles(definition.project, kind, run_name)
    if not loaded_profiles:
        return None
    selected_profiles: list[BuildProfile | InspectProfile | ServeProfile] = []
    for profile in loaded_profiles:
        if not isinstance(profile, BuildProfile | InspectProfile | ServeProfile):
            raise TypeError("Profile loading returned an unsupported profile type")
        selected_profiles.append(profile)

    declared_artifact_tasks = definition.artifact_operations
    declared_operation_tasks = definition.runtime_operations
    artifact_tasks_by_id = {task.id: task for task in declared_artifact_tasks}
    operation_tasks_by_id = {task.id: task for task in declared_operation_tasks}
    tasks_by_id = artifact_tasks_by_id | operation_tasks_by_id
    for profile in selected_profiles:
        task = tasks_by_id.get(profile.target)
        if task is None:
            logger.error(
                "%s profile '%s' references unknown operation target '%s'.",
                kind.capitalize(),
                profile.name,
                profile.target,
            )
            raise SystemExit(2)
        if kind == "build" and not isinstance(task, ArtifactTask):
            logger.error(
                "Build profile '%s' must target an artifact operation; '%s' is a "
                "runtime operation.",
                profile.name,
                profile.target,
            )
            raise SystemExit(2)
        if kind != "build" and not isinstance(task, OperationTask):
            logger.error(
                "%s profile '%s' must target a runtime operation; '%s' is an artifact operation.",
                kind.capitalize(),
                profile.name,
                profile.target,
            )
            raise SystemExit(2)

    execution_dir = execution_root(definition.project.artifacts_root)
    if kind == "build":
        build_profiles = [
            profile
            for profile in selected_profiles
            if isinstance(profile, BuildProfile)
        ]
        if len(build_profiles) != len(selected_profiles):
            raise TypeError("Build profile loading returned the wrong profile type")
        build_jobs: list[BuildJob] = []
        for profile in build_profiles:
            try:
                observability = resolve_observability_settings(
                    project_path,
                    profile.observability,
                    cli_visuals=cli_visuals,
                    cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
                    cli_log_level=cli_log_level,
                    cli_log_outputs=cli_log_outputs,
                    base_log_level=base_log_level,
                )
            except ValueError as exc:
                logger.error("Invalid build configuration: %s", exc)
                raise SystemExit(2) from exc
            build_jobs.append(
                BuildJob(
                    task=artifact_tasks_by_id[profile.target].model_copy(deep=True),
                    settings=BuildSettings(
                        mode="FORCE" if force else profile.mode or "AUTO",
                        observability=replace(
                            observability,
                            log_output=resolve_execution_log_outputs(
                                observability.log_output,
                                execution_dir,
                                default_path=(
                                    Path("logs")
                                    / f"build.{sanitize_path_segment(profile.name)}.log"
                                ),
                            ),
                        ),
                    ),
                )
            )
        request: ProfileRunRequest = BuildRunRequest(
            definition=definition,
            jobs=build_jobs,
            execution=defaults.execution,
        )
    else:
        selected_runtime_profiles = [
            profile
            for profile in selected_profiles
            if isinstance(profile, ServeProfile | InspectProfile)
        ]
        if len(selected_runtime_profiles) != len(selected_profiles):
            raise TypeError("Runtime profile loading returned the wrong profile type")
        if artifact_mode is not None:
            try:
                resolved_artifact_mode = normalize_artifact_mode(artifact_mode)
            except ValueError as exc:
                logger.error("Invalid artifact mode: %s", exc)
                raise SystemExit(2) from exc
        else:
            profile_modes = {
                profile.artifact_mode or "AUTO" for profile in selected_runtime_profiles
            }
            if len(profile_modes) != 1:
                configured = ", ".join(
                    f"{profile.name}={profile.artifact_mode or 'AUTO'}"
                    for profile in selected_runtime_profiles
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
                    default_path=Path("logs") / f"{kind}.artifacts.log",
                ),
            ),
        )
        cli_output = build_cli_output_config(
            output_transport,
            output_format,
            output_directory,
            output_encoding,
            workspace=workspace,
            view=output_view,
        )
        try:
            resolved_runtime_profiles = resolve_runtime_profiles(
                definition=definition,
                profiles=selected_runtime_profiles,
                preview=preview,
                limit=limit,
                cli_output=cli_output,
                cli_log_level=cli_log_level,
                cli_log_outputs=cli_log_outputs,
                base_log_level=base_log_level,
                cli_visuals=cli_visuals,
                cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
            )
            runtime_jobs = [
                RuntimeJob(
                    name=profile.name,
                    task=operation_tasks_by_id[profile.target_id].model_copy(deep=True),
                    runtime=compile_runtime(definition),
                    output=profile.output,
                    observability=replace(
                        profile.observability,
                        log_output=resolve_execution_log_outputs(
                            profile.observability.log_output,
                            execution_dir,
                            default_path=(
                                Path("logs")
                                / f"{kind}.{sanitize_path_segment(profile.name)}.log"
                            ),
                        ),
                    ),
                    limit=profile.limit,
                    throttle_ms=profile.throttle_ms,
                    preview=profile.preview,
                    output_splits=profile.output_splits,
                )
                for profile in resolved_runtime_profiles
            ]
        except OutputResolutionError as exc:
            logger.error("Invalid output configuration: %s", exc)
            raise SystemExit(2) from exc
        except ValueError as exc:
            logger.error("%s", exc)
            raise SystemExit(2) from exc
        request = RuntimeRunRequest(
            command=kind,
            definition=definition,
            jobs=runtime_jobs,
            execution=defaults.execution,
            artifact_settings=artifact_settings,
            serve_run_plans=(_serve_run_plans(runtime_jobs) if kind == "serve" else ()),
        )

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
    project_path = Path(project)
    try:
        definition = load_pipeline(project_path)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        logger.error("Failed to load pipeline inputs: %s", exc)
        raise SystemExit(2) from exc
    project_path = definition.project.path
    loaded_profiles, defaults = _select_profiles(
        definition.project,
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

    try:
        execution_dir = execution_root(definition.project.artifacts_root)
        jobs = resolve_materialize_jobs(
            profiles=materialize_profiles,
            project_path=project_path,
            execution_dir=execution_dir,
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
        runtime = compile_runtime(definition)
    except (OSError, TypeError, ValueError) as exc:
        logger.error("Invalid materialize configuration: %s", exc)
        raise SystemExit(2) from exc

    artifact_settings = BuildSettings(
        mode=resolved_artifact_mode,
        observability=replace(
            artifact_observability,
            log_output=resolve_execution_log_outputs(
                artifact_observability.log_output,
                execution_dir,
                default_path=Path("logs") / "materialize.artifacts.log",
            ),
        ),
    )
    request = MaterializeRunRequest(
        definition=definition,
        jobs=jobs,
        execution=defaults.execution,
        artifact_settings=artifact_settings,
        runtime=runtime,
    )
    return request
