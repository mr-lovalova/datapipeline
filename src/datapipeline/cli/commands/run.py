import json
import logging
from pathlib import Path
from typing import Optional

from datapipeline.cli.commands.build import run_build_if_needed
from datapipeline.cli.commands.run_config import (
    RunEntry,
    resolve_run_entries,
)
from datapipeline.cli.commands.serve_pipeline import serve_with_runtime
from datapipeline.cli.visuals.runner import run_job
from datapipeline.cli.visuals.sections import sections_from_path
from datapipeline.config.context import resolve_run_profiles
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.tasks import ServeOutputConfig
from datapipeline.io.output import OutputResolutionError
from datapipeline.pipeline.artifacts import StageDemand, required_artifacts_for
from datapipeline.services.path_policy import resolve_workspace_path

logger = logging.getLogger(__name__)


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
        "visuals": {
            "provider": profile.visuals.visuals,
            "progress": profile.visuals.progress,
        },
        "output": {
            "transport": profile.output.transport,
            "format": profile.output.format,
            "payload": profile.output.payload,
            "destination": str(profile.output.destination)
            if profile.output.destination
            else None,
        },
    }
    cfg = entry.config
    if cfg is not None:
        payload["run_config"] = cfg.model_dump(
            exclude_unset=True, exclude_none=True)
    return payload


def _log_profile_start_debug(profile) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    payload = _profile_debug_payload(profile)
    logger.debug(
        "Run profile start (%s/%s):\n%s",
        profile.idx,
        profile.total,
        json.dumps(payload, indent=2, default=str),
    )


def _entry_sections(run_root: Optional[Path], entry: RunEntry) -> tuple[str, ...]:
    # Prefix sections with a phase label for visuals; keep path-based detail.
    path_sections = sections_from_path(run_root, entry.path)
    return ("Run Tasks",) + tuple(path_sections[1:])


def _build_cli_output_config(
    transport: Optional[str],
    fmt: Optional[str],
    directory: Optional[str],
    payload: Optional[str],
    workspace=None,
) -> tuple[ServeOutputConfig | None, Optional[str]]:
    payload_style = _normalize_payload(payload)

    if transport is None and fmt is None and directory is None:
        return None, payload_style

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
        return (
            ServeOutputConfig(
                transport="fs",
                format=fmt,
                directory=resolved_directory,
                payload=payload_style or "sample",
            ),
            None,
        )
    if directory:
        logger.error("--output-directory is only valid when --output-transport=fs")
        raise SystemExit(2)
    return (
        ServeOutputConfig(
            transport="stdout",
            format=fmt,
            payload=payload_style or "sample",
        ),
        None,
    )


def _normalize_payload(payload: Optional[str]) -> Optional[str]:
    if payload is None:
        return None
    payload_style = payload.lower()
    if payload_style not in {"sample", "vector"}:
        logger.error("--output-payload must be 'sample' or 'vector'")
        raise SystemExit(2)
    return payload_style


def _resolve_profiles(
    *,
    project_path: Path,
    run_entries,
    keep: Optional[str],
    stage: Optional[int],
    limit: Optional[int],
    cli_output: ServeOutputConfig | None,
    cli_payload: Optional[str],
    workspace,
    cli_log_level: Optional[str],
    base_log_level: str,
    cli_visuals: Optional[str],
    cli_progress: Optional[str],
    create_run: bool,
):
    return resolve_run_profiles(
        project_path=project_path,
        run_entries=run_entries,
        keep=keep,
        stage=stage,
        limit=limit,
        cli_output=cli_output,
        cli_payload=cli_payload,
        workspace=workspace,
        cli_log_level=cli_log_level,
        base_log_level=base_log_level,
        cli_visuals=cli_visuals,
        cli_progress=cli_progress,
        create_run=create_run,
    )


def _dataset_name_for_stage(stage: Optional[int]) -> str:
    return "vectors" if stage is None else "features"


def _load_dataset_for_profile(
    *,
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
    *,
    cli_visuals: Optional[str],
    cli_progress: Optional[str],
    workspace,
) -> None:
    demands = [StageDemand(profile.stage) for profile in profiles]
    required = required_artifacts_for(dataset, demands)
    if not required:
        return
    run_build_if_needed(
        project_path,
        cli_visuals=cli_visuals,
        cli_progress=cli_progress,
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
    output_payload: Optional[str] = None,
    output_directory: Optional[str] = None,
    skip_build: bool = False,
    *,
    cli_log_level: Optional[str],
    base_log_level: str,
    cli_visuals: Optional[str] = None,
    cli_progress: Optional[str] = None,
    workspace=None,
) -> None:
    project_path = Path(project)
    run_entries, run_root = resolve_run_entries(project_path, run_name)

    cli_output_cfg, payload_override = _build_cli_output_config(
        output_transport, output_format, output_directory, output_payload, workspace)
    cli_payload = payload_override
    try:
        profiles = _resolve_profiles(
            project_path=project_path,
            run_entries=run_entries,
            keep=keep,
            stage=stage,
            limit=limit,
            cli_output=cli_output_cfg,
            cli_payload=cli_payload,
            workspace=workspace,
            cli_log_level=cli_log_level,
            base_log_level=base_log_level,
            cli_visuals=cli_visuals,
            cli_progress=cli_progress,
            create_run=False,
        )
    except OutputResolutionError as exc:
        logger.error("Invalid output configuration: %s", exc)
        raise SystemExit(2) from exc

    vector_dataset = load_dataset(project_path, "vectors")
    skip_reason = None
    if skip_build:
        skip_reason = "--skip-build flag provided"

    if not skip_reason:
        ensure_stage_artifacts(
            project_path,
            vector_dataset,
            profiles,
            cli_visuals=cli_visuals,
            cli_progress=cli_progress,
            workspace=workspace,
        )
        profiles = _resolve_profiles(
            project_path=project_path,
            run_entries=run_entries,
            keep=keep,
            stage=stage,
            limit=limit,
            cli_output=cli_output_cfg,
            cli_payload=cli_payload,
            workspace=workspace,
            cli_log_level=cli_log_level,
            base_log_level=base_log_level,
            cli_visuals=cli_visuals,
            cli_progress=cli_progress,
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

        root_logger = logging.getLogger()
        if root_logger.level != profile.log_decision.value:
            root_logger.setLevel(profile.log_decision.value)

        def _work(profile=profile):
            _log_profile_start_debug(profile)
            serve_with_runtime(
                profile.runtime,
                dataset,
                limit=profile.limit,
                target=profile.output,
                throttle_ms=profile.throttle_ms,
                stage=profile.stage,
                visuals=profile.visuals.visuals,
            )

        sections = _entry_sections(run_root, profile.entry)
        run_job(
            sections=sections,
            label=profile.label,
            visuals=profile.visuals.visuals or "auto",
            progress_style=profile.visuals.progress or "auto",
            level=profile.log_decision.value,
            runtime=profile.runtime,
            work=_work,
            idx=profile.idx,
            total=profile.total,
        )
