import logging
from pathlib import Path
from typing import Optional

from datapipeline.cli.commands.build import run_build_if_needed
from datapipeline.cli.commands.run_config import (
    RunEntry,
    determine_preview_stage,
    resolve_run_entries,
)
from datapipeline.cli.commands.serve_pipeline import serve_with_runtime
from datapipeline.cli.visuals.runner import run_job
from datapipeline.cli.visuals.sections import sections_from_path
from datapipeline.config.context import resolve_run_profiles
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.run import OutputConfig
from datapipeline.io.output import OutputResolutionError

logger = logging.getLogger(__name__)


def _run_config_value(run_cfg, field: str):
    """Return a run config field only when it was explicitly provided."""
    if run_cfg is None:
        return None
    fields_set = getattr(run_cfg, 'model_fields_set', None)
    if fields_set is not None and field not in fields_set:
        return None
    return getattr(run_cfg, field, None)



def _entry_sections(run_root: Optional[Path], entry: RunEntry) -> tuple[str, ...]:
    return sections_from_path(run_root, entry.path)


def _build_cli_output_config(
    transport: Optional[str],
    fmt: Optional[str],
    path: Optional[str],
) -> OutputConfig | None:
    if transport is None and fmt is None and path is None:
        return None
    if not transport or not fmt:
        logger.error(
            "--out-transport and --out-format must be provided together")
        raise SystemExit(2)
    transport = transport.lower()
    fmt = fmt.lower()
    if transport == "fs":
        if not path:
            logger.error("--out-path is required when --out-transport=fs (directory)")
            raise SystemExit(2)
        return OutputConfig(transport="fs", format=fmt, directory=Path(path))
    if path:
        logger.error("--out-path is only valid when --out-transport=fs")
        raise SystemExit(2)
    return OutputConfig(transport="stdout", format=fmt)


def handle_serve(
    project: str,
    limit: Optional[int],
    include_targets: Optional[bool] = None,
    keep: Optional[str] = None,
    run_name: Optional[str] = None,
    stage: Optional[int] = None,
    out_transport: Optional[str] = None,
    out_format: Optional[str] = None,
    out_path: Optional[str] = None,
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
    skip_reason = None
    if skip_build:
        skip_reason = "--skip-build flag provided"
    else:
        preview_stage, preview_source = determine_preview_stage(stage, run_entries)
        if preview_stage is not None and preview_stage <= 5:
            if preview_source:
                skip_reason = f"stage {preview_stage} preview ({preview_source})"
            else:
                skip_reason = f"stage {preview_stage} preview"

    if skip_reason:
        logger.info("Skipping build (%s).", skip_reason)
    else:
        run_build_if_needed(
            project_path,
            cli_visuals=cli_visuals,
            cli_progress=cli_progress,
            workspace=workspace,
        )

    cli_output_cfg = _build_cli_output_config(
        out_transport, out_format, out_path)
    try:
        profiles = resolve_run_profiles(
            project_path=project_path,
            run_entries=run_entries,
            keep=keep,
            stage=stage,
            limit=limit,
            include_targets=include_targets,
            cli_output=cli_output_cfg,
            workspace=workspace,
            cli_log_level=cli_log_level,
            base_log_level=base_log_level,
            cli_visuals=cli_visuals,
            cli_progress=cli_progress,
        )
    except OutputResolutionError as exc:
        logger.error("Invalid output configuration: %s", exc)
        raise SystemExit(2) from exc

    datasets: dict[str, object] = {}
    for profile in profiles:
        dataset_name = "vectors" if profile.stage is None else "features"
        dataset = datasets.get(dataset_name)
        if dataset is None:
            dataset = load_dataset(project_path, dataset_name)
            datasets[dataset_name] = dataset

        root_logger = logging.getLogger()
        if root_logger.level != profile.log_decision.value:
            root_logger.setLevel(profile.log_decision.value)

        def _work():
            serve_with_runtime(
                profile.runtime,
                dataset,
                limit=profile.limit,
                target=profile.output,
                include_targets=profile.include_targets,
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
