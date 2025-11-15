import logging
from pathlib import Path
from typing import Optional, Sequence, Union

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
from datapipeline.io.output import OutputResolutionError, resolve_output_target

logger = logging.getLogger(__name__)


def _coerce_log_level(
    value: Optional[Union[str, int]],
    *,
    default: int = logging.WARNING,
) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    name = str(value).upper()
    if name not in logging._nameToLevel:
        raise ValueError(f"Unsupported log level: {value}")
    return logging._nameToLevel[name]


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
    # Helper for precedence: CLI > config > default
    def pick(*values):
        for value in values:
            if value is not None:
                return value
        return None

    base_level_name = str(base_log_level).upper()
    shared = workspace.config.shared if workspace else None
    serve_defaults = workspace.config.serve if workspace else None
    shared_visual_provider_default = (
        shared.visual_provider.lower() if shared and shared.visual_provider else None
    )
    shared_progress_style_default = (
        shared.progress_style.lower() if shared and shared.progress_style else None
    )
    shared_log_level_default = (
        shared.log_level.upper() if shared and shared.log_level else None
    )
    serve_log_level_default = (
        serve_defaults.log_level.upper() if serve_defaults and serve_defaults.log_level else None
    )
    serve_limit_default = serve_defaults.limit if serve_defaults else None
    serve_stage_default = serve_defaults.stage if serve_defaults else None
    serve_throttle_default = (
        serve_defaults.throttle_ms if serve_defaults else None
    )
    serve_output_defaults = None
    if serve_defaults and serve_defaults.output_defaults:
        od = serve_defaults.output_defaults
        output_path = Path(od.path) if od.path else None
        if output_path and workspace and not output_path.is_absolute():
            output_path = (workspace.root / output_path).resolve()
        serve_output_defaults = OutputConfig(
            transport=od.transport,
            format=od.format,
            path=output_path,
        )
    base_level_value = _coerce_log_level(base_level_name)
    datasets: dict[str, object] = {}

    for idx, total_runs, entry_name, runtime in iter_runtime_runs(project_path, run_entries, keep):
        run = getattr(runtime, "run", None)
        resolved_stage = pick(stage, _run_config_value(run, "stage"), serve_stage_default)
        dataset_name = "vectors" if resolved_stage is None else "features"
        dataset = datasets.get(dataset_name)
        if dataset is None:
            dataset = load_dataset(project_path, dataset_name)
            datasets[dataset_name] = dataset

        # resolving argument hierarchy CLI args > run config > defaults
        resolved_limit = pick(limit, _run_config_value(run, "limit"), serve_limit_default)
        resolved_include_targets = pick(
            include_targets, _run_config_value(run, "include_targets"), False)
        throttle_ms = pick(
            _run_config_value(run, "throttle_ms"),
            serve_throttle_default,
        )
        resolved_level_name = pick(
            cli_log_level.upper() if cli_log_level else None,
            _run_config_value(run, "log_level"),
            serve_log_level_default,
            shared_log_level_default,
            base_level_name,
        )
        resolved_level_value = _coerce_log_level(
            resolved_level_name, default=base_level_value)

        # Visual provider/style resolution: CLI > run.yaml > defaults
        run_visual_provider = _run_config_value(run, "visual_provider")
        run_progress_style = _run_config_value(run, "progress_style")
        resolved_visual_provider = (
            pick(
                (cli_visual_provider.lower() if cli_visual_provider else None),
                run_visual_provider.lower() if run_visual_provider else None,
                shared_visual_provider_default,
                "auto",
            )
            or "auto"
        )
        resolved_progress_style = (
            pick(
                (cli_progress_style.lower() if cli_progress_style else None),
                run_progress_style.lower() if run_progress_style else None,
                shared_progress_style_default,
                "auto",
            )
            or "auto"
        )

        try:
            runtime_output_cfg = (
                serve_output_defaults.model_copy()
                if serve_output_defaults is not None
                else None
            )
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
