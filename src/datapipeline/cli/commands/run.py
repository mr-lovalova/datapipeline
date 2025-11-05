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
from datapipeline.cli.visuals import visual_sources
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.run import OutputConfig
from datapipeline.io.output import OutputResolutionError, resolve_output_target
from tqdm.contrib.logging import logging_redirect_tqdm

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
) -> None:
    # Helper for precedence: CLI > config > default
    def pick(cli_val, cfg_val, default=None):
        return cli_val if cli_val is not None else (cfg_val if cfg_val is not None else default)

    base_level_name = str(base_log_level).upper()
    base_level_value = _coerce_log_level(base_level_name)
    datasets: dict[str, object] = {}

    for idx, total_runs, entry_name, runtime in iter_runtime_runs(project_path, run_entries, keep):
        run = getattr(runtime, "run", None)
        resolved_stage = pick(stage, getattr(run, "stage", None), None)
        dataset_name = "vectors" if resolved_stage is None else "features"
        dataset = datasets.get(dataset_name)
        if dataset is None:
            dataset = load_dataset(project_path, dataset_name)
            datasets[dataset_name] = dataset

        # resolving argument hierarchy CLI args > run config > defaults
        resolved_limit = pick(limit, getattr(run, "limit", None), None)
        resolved_include_targets = pick(
            include_targets, getattr(run, "include_targets", None), False)
        throttle_ms = getattr(run, "throttle_ms", None)
        resolved_level_name = pick(
            cli_log_level.upper() if cli_log_level else None,
            getattr(run, "log_level", None),
            base_level_name,
        )
        resolved_level_value = _coerce_log_level(
            resolved_level_name, default=base_level_value)

        try:
            target = resolve_output_target(
                cli_output=cli_output,
                config_output=getattr(run, "output", None) if run else None,
                base_path=project_path.parent,
            )
        except OutputResolutionError as exc:
            logger.error("Invalid output configuration: %s", exc)
            raise SystemExit(2) from exc

        root_logger = logging.getLogger()
        if root_logger.level != resolved_level_value:
            root_logger.setLevel(resolved_level_value)

        label = entry_name or f"run{idx}"
        logger.info("Run '%s' (%d/%d)", label, idx, total_runs)

        with visual_sources(runtime, resolved_level_value):
            with logging_redirect_tqdm():
                serve_with_runtime(
                    runtime,
                    dataset,
                    limit=resolved_limit,
                    target=target,
                    include_targets=resolved_include_targets,
                    throttle_ms=throttle_ms,
                    stage=resolved_stage
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
        run_build_if_needed(project_path, ensure_level=logging.INFO)

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
    )
