import logging
from pathlib import Path
from typing import Optional, Sequence

from datapipeline.cli.commands.build import run_build_if_needed
from datapipeline.cli.commands.run_config import (
    RunEntry,
    determine_preview_stage,
    iter_runtime_runs,
    resolve_run_entries,
)
from datapipeline.cli.commands.serve_pipeline import serve_with_runtime
from datapipeline.cli.visuals.runner import run_job
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.run import OutputConfig
from datapipeline.config.resolution import (
    cascade,
    resolve_log_level,
    resolve_visuals,
    workspace_output_defaults,
)
from datapipeline.io.output import OutputResolutionError, resolve_output_target

logger = logging.getLogger(__name__)


def _run_config_value(run_cfg, field: str):
    """Return a run config field only when it was explicitly provided."""
    if run_cfg is None:
        return None
    fields_set = getattr(run_cfg, "model_fields_set", None)
    if fields_set is not None and field not in fields_set:
        return None
    return getattr(run_cfg, field, None)


def _execute_runs(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    stage: Optional[int],
    limit: Optional[int],
    cli_output: OutputConfig | None,
    include_targets: Optional[bool],
    keep: Optional[str],
    *,
    cli_log_level: Optional[str],
    base_log_level: str,
    cli_visual_provider: Optional[str],
    cli_progress_style: Optional[str],
    workspace,
) -> None:
    base_level_name = str(base_log_level).upper()
    shared = workspace.config.shared if workspace else None
    serve_defaults = workspace.config.serve if workspace else None
    shared_visual_provider_default = shared.visual_provider if shared else None
    shared_progress_style_default = shared.progress_style if shared else None
    shared_log_level_default = shared.log_level if shared else None
    serve_log_level_default = (
        serve_defaults.log_level if serve_defaults else None
    )
    serve_limit_default = serve_defaults.limit if serve_defaults else None
    serve_stage_default = serve_defaults.stage if serve_defaults else None
    serve_throttle_default = (
        serve_defaults.throttle_ms if serve_defaults else None
    )
    workspace_output_cfg = workspace_output_defaults(workspace)
    datasets: dict[str, object] = {}

    for idx, total_runs, entry_name, runtime in iter_runtime_runs(project_path, run_entries, keep):
        run = getattr(runtime, "run", None)
        resolved_stage = cascade(stage, _run_config_value(
            run, "stage"), serve_stage_default)
        dataset_name = "vectors" if resolved_stage is None else "features"
        dataset = datasets.get(dataset_name)
        if dataset is None:
            dataset = load_dataset(project_path, dataset_name)
            datasets[dataset_name] = dataset

        # resolving argument hierarchy CLI args > run config > defaults
        resolved_limit = cascade(limit, _run_config_value(
            run, "limit"), serve_limit_default)
        resolved_include_targets = cascade(
            include_targets, _run_config_value(run, "include_targets"), False)
        throttle_ms = cascade(
            _run_config_value(run, "throttle_ms"),
            serve_throttle_default,
        )
        log_decision = resolve_log_level(
            cli_log_level,
            _run_config_value(run, "log_level"),
            serve_log_level_default,
            shared_log_level_default,
            fallback=base_level_name,
        )
        resolved_level_value = log_decision.value

        # Visual provider/style resolution: CLI > run.yaml > defaults
        run_visual_provider = _run_config_value(run, "visual_provider")
        run_progress_style = _run_config_value(run, "progress_style")
        visuals = resolve_visuals(
            cli_provider=cli_visual_provider,
            config_provider=run_visual_provider,
            workspace_provider=shared_visual_provider_default,
            cli_style=cli_progress_style,
            config_style=run_progress_style,
            workspace_style=shared_progress_style_default,
        )
        resolved_visual_provider = visuals.provider
        resolved_progress_style = visuals.progress_style

        try:
            runtime_output_cfg = workspace_output_cfg.model_copy() if workspace_output_cfg else None
            target = resolve_output_target(
                cli_output=cli_output,
                config_output=getattr(run, "output", None) if run else None,
                default=runtime_output_cfg,
                base_path=project_path.parent,
            )
        except OutputResolutionError as exc:
            logger.error("Invalid output configuration: %s", exc)
            raise SystemExit(2) from exc

        root_logger = logging.getLogger()
        if root_logger.level != resolved_level_value:
            root_logger.setLevel(resolved_level_value)

        label = entry_name or f"run{idx}"

        def _work():
            serve_with_runtime(
                runtime,
                dataset,
                limit=resolved_limit,
                target=target,
                include_targets=resolved_include_targets,
                throttle_ms=throttle_ms,
                stage=resolved_stage,
                visual_provider=resolved_visual_provider,
            )

        run_job(
            kind="run",
            label=label,
            visuals=resolved_visual_provider or "auto",
            progress_style=resolved_progress_style or "auto",
            level=resolved_level_value,
            runtime=runtime,
            work=_work,
            idx=idx,
            total=total_runs,
        )


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
            logger.error("--out-path is required when --out-transport=fs")
            raise SystemExit(2)
        return OutputConfig(transport="fs", format=fmt, path=Path(path))
    if path:
        logger.error("--out-path is only valid when --out-transport=fs")
        raise SystemExit(2)
    return OutputConfig(transport="stdout", format=fmt, path=None)


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
    cli_visual_provider: Optional[str] = None,
    cli_progress_style: Optional[str] = None,
    workspace=None,
) -> None:
    project_path = Path(project)
    run_entries = resolve_run_entries(project_path, run_name)
    skip_reason = None
    if skip_build:
        skip_reason = "--skip-build flag provided"
    else:
        preview_stage, preview_source = determine_preview_stage(
            stage, run_entries
        )
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
            ensure_level=logging.INFO,
            cli_visual_provider=cli_visual_provider,
            cli_progress_style=cli_progress_style,
            workspace=workspace,
        )

    cli_output_cfg = _build_cli_output_config(
        out_transport, out_format, out_path)
    _execute_runs(
        project_path=project_path,
        run_entries=run_entries,
        stage=stage,
        limit=limit,
        cli_output=cli_output_cfg,
        include_targets=include_targets,
        keep=keep,
        cli_log_level=cli_log_level,
        base_log_level=base_log_level,
        cli_visual_provider=cli_visual_provider,
        cli_progress_style=cli_progress_style,
        workspace=workspace,
    )
