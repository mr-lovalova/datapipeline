import logging
from pathlib import Path
from typing import Optional, Sequence

from pydantic import ValidationError

from datapipeline.cli.commands.build import run_build_if_needed
from datapipeline.cli.commands.run_config import (
    resolve_run_entries,
)
from datapipeline.cli.commands.serve_operations import run_serve_operation
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.runner import run_job
from datapipeline.cli.visuals.sections import sections_from_path
from datapipeline.config.serve_resolution import resolve_run_profiles
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.tasks import ServeOutputConfig, artifact_tasks
from datapipeline.io.output import OutputResolutionError
from datapipeline.artifacts.specs import (
    StageDemand,
    artifact_build_order,
    artifact_definitions_with_task_dependencies,
    artifact_keys_for_task_kinds,
    required_artifacts_for,
)
from datapipeline.services.path_policy import resolve_workspace_path
from datapipeline.services.run_entries import RunEntry
from datapipeline.services.run_artifacts import write_serve_profile

logger = logging.getLogger(__name__)

_OUTPUT_MATRIX_HELP = (
    "Valid output combinations:\n"
    "  stdout: format=jsonl, view=flat|raw|values\n"
    "          encoding is not supported\n"
    "  fs:     format=jsonl|csv|pickle, view=flat|raw|values\n"
    "          encoding is supported only for jsonl/csv (default utf-8)\n"
    "          csv supports view=flat|values\n"
)


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
            exclude_unset=True, exclude_none=True)
    operation = entry.operation
    payload["operation"] = {
        "name": operation.effective_name(),
        "entrypoint": operation.entrypoint,
        "dependencies": list(operation.dependencies),
    }
    return payload


def _persist_profile_artifact(profile) -> Path | None:
    run_paths = getattr(profile.output, "run", None)
    if run_paths is None:
        return None
    payload = _profile_debug_payload(profile)
    try:
        return write_serve_profile(
            paths=run_paths,
            task_label=profile.label,
            payload=payload,
        )
    except Exception:
        logger.warning("Failed to persist serve profile artifact", exc_info=True)
        return None


def _log_profile_start_debug(profile,  profile_path: Path | None = None) -> None:
    output = profile.output
    message = (
        f"Run profile start ({profile.idx}/{profile.total}) "
        f"label={profile.label} "
        f"stage={profile.stage} "
        f"limit={profile.limit} "
        f"log_level={profile.log_decision.name} "
        f"output={output.transport}:{output.format}:{output.view}"
    )
    if profile_path is not None:
        message = f"{message} profile={profile_path}"
    emit_execution_message(
        message,
        level=logging.DEBUG,
        logger=logger,
        message_kind="task_config",
    )


def _entry_sections(run_root: Optional[Path], entry: RunEntry) -> tuple[str, ...]:
    # Prefix sections with a phase label for visuals; keep path-based detail.
    path_sections = sections_from_path(run_root, entry.path)
    return ("Run Tasks",) + tuple(path_sections[1:])


def _build_cli_output_config(
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
            logger.error(
                "--output-directory is required when --output-transport=fs")
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


def _resolve_profiles_or_exit(
    
    project_path: Path,
    run_entries,
    keep: Optional[str],
    stage: Optional[int],
    limit: Optional[int],
    cli_output: ServeOutputConfig | None,
    workspace,
    cli_log_level: Optional[str],
    cli_log_outputs: Sequence[LogOutputTarget] | None,
    base_log_level: str,
    cli_visuals: Optional[str],
    create_run: bool,
):
    try:
        return resolve_run_profiles(
            project_path=project_path,
            run_entries=run_entries,
            keep=keep,
            stage=stage,
            limit=limit,
            cli_output=cli_output,
            workspace=workspace,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
            cli_visuals=cli_visuals,
            create_run=create_run,
        )
    except ValueError as exc:
        logger.error("Invalid log output configuration: %s", exc)
        raise SystemExit(2) from exc
    except OutputResolutionError as exc:
        logger.error("Invalid output configuration: %s", exc)
        raise SystemExit(2) from exc


def _dataset_name_for_stage(stage: Optional[int]) -> str:
    return "vectors" if stage is None else "features"


def _load_dataset_for_profile(
    
    cache: dict[str, object],
    project_path: Path,
    stage: Optional[int],
):
    dataset_name = _dataset_name_for_stage(stage)
    dataset = cache.get(dataset_name)
    if dataset is None:
        dataset = load_dataset(project_path, dataset_name)
        cache[dataset_name] = dataset
    return dataset


def ensure_stage_artifacts(
    project_path: Path,
    dataset,
    profiles,
    
    cli_log_level: Optional[str],
    cli_visuals: Optional[str],
    cli_log_outputs: Sequence[LogOutputTarget] | None,
    workspace,
) -> None:
    demands = [StageDemand(profile.stage) for profile in profiles]
    try:
        definitions = artifact_definitions_with_task_dependencies(artifact_tasks(project_path))
    except ValueError as exc:
        logger.error("Invalid artifact task dependencies: %s", exc)
        raise SystemExit(2) from exc
    required = required_artifacts_for(dataset, demands, definitions=definitions)
    operation_dependencies: set[str] = set()
    for profile in profiles:
        operation = profile.entry.operation
        operation_dependencies.update(operation.dependencies)
    if operation_dependencies:
        required |= artifact_keys_for_task_kinds(operation_dependencies, definitions)
    required = set(artifact_build_order(required, definitions=definitions))
    if not required:
        return
    run_build_if_needed(
        project_path,
        cli_log_level=cli_log_level,
        cli_visuals=cli_visuals,
        cli_log_outputs=list(cli_log_outputs or []),
        workspace=workspace,
        required_artifacts=required,
    )


def handle_serve(
    project: str,
    limit: Optional[int],
    keep: Optional[str] = None,
    run_name: Optional[str] = None,
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
    workspace=None,
) -> None:
    project_path = Path(project)
    run_entries, run_root = resolve_run_entries(project_path, run_name)
    if not run_entries:
        logger.info("No enabled serve profiles; skipping serve.")
        return

    cli_output_cfg = _build_cli_output_config(
        output_transport,
        output_format,
        output_directory,
        output_encoding,
        workspace=workspace,
        view=output_view,
    )
    profiles = _resolve_profiles_or_exit(
        project_path=project_path,
        run_entries=run_entries,
        keep=keep,
        stage=stage,
        limit=limit,
        cli_output=cli_output_cfg,
        workspace=workspace,
        cli_log_level=cli_log_level,
        cli_log_outputs=cli_log_outputs,
        base_log_level=base_log_level,
        cli_visuals=cli_visuals,
        create_run=skip_build,
    )

    vector_dataset = load_dataset(project_path, "vectors")
    skip_reason = None
    if skip_build:
        skip_reason = "--skip-build flag provided"

    if not skip_reason:
        ensure_stage_artifacts(
            project_path,
            vector_dataset,
            profiles,
            cli_log_level=cli_log_level,
            cli_visuals=cli_visuals,
            cli_log_outputs=cli_log_outputs,
            workspace=workspace,
        )
        profiles = _resolve_profiles_or_exit(
            project_path=project_path,
            run_entries=run_entries,
            keep=keep,
            stage=stage,
            limit=limit,
            cli_output=cli_output_cfg,
            workspace=workspace,
            cli_log_level=cli_log_level,
            cli_log_outputs=cli_log_outputs,
            base_log_level=base_log_level,
            cli_visuals=cli_visuals,
            create_run=True,
        )

    datasets: dict[str, object] = {}
    datasets["vectors"] = vector_dataset
    for profile in profiles:
        dataset = _load_dataset_for_profile(
            cache=datasets,
            project_path=project_path,
            stage=profile.stage,
        )

        configure_root_logging(
            level=profile.log_decision.value,
            output=profile.log_output,
        )
        profile_path = _persist_profile_artifact(profile)

        def _work(profile=profile, profile_path=profile_path):
            _log_profile_start_debug(profile, profile_path=profile_path)
            operation = profile.entry.operation
            try:
                run_serve_operation(
                    operation=operation,
                    runtime=profile.runtime,
                    dataset=dataset,
                    limit=profile.limit,
                    target=profile.output,
                    throttle_ms=profile.throttle_ms,
                    stage=profile.stage,
                    visuals=profile.visuals.visuals,
                )
            except ValueError as exc:
                logger.error("%s", exc)
                raise SystemExit(2) from exc

        sections = _entry_sections(run_root, profile.entry)
        run_job(
            sections=sections,
            label=profile.label,
            visuals=profile.visuals.visuals or "on",
            level=profile.log_decision.value,
            runtime=profile.runtime,
            work=_work,
            idx=profile.idx,
            total=profile.total,
        )
