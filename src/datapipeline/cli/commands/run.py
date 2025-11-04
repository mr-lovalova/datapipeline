import logging
import time
from itertools import islice
from pathlib import Path
from typing import Iterator, List, Optional, Union

from datapipeline.cli.visuals import visual_sources
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.run import OutputConfig, RunConfig, load_named_run_configs
from datapipeline.domain.sample import Sample
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process
from datapipeline.pipeline.split import apply_split_stage
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import (
    OutputResolutionError,
    OutputTarget,
    resolve_output_target,
)
from datapipeline.io.protocols import Writer
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


def _resolve_run_entries(project_path: Path, run_name: Optional[str]) -> List[tuple[Optional[str], Optional[RunConfig]]]:
    try:
        entries = load_named_run_configs(project_path)
    except FileNotFoundError:
        entries = []
    except Exception as exc:
        logger.error("Failed to load run configs: %s", exc)
        raise SystemExit(2) from exc

    if entries:
        if run_name:
            entries = [entry for entry in entries if entry[0] == run_name]
            if not entries:
                logger.error("Unknown run config '%s'", run_name)
                raise SystemExit(2)
    else:
        if run_name:
            logger.error("Project does not define run configs.")
            raise SystemExit(2)
        entries = [(None, None)]
    return entries


def _iter_runtime_runs(
    project_path: Path,
    run_name: Optional[str],
    keep_override: Optional[str],
) -> Iterator[tuple[int, int, Optional[str], Runtime]]:
    run_entries = _resolve_run_entries(project_path, run_name)
    total_runs = len(run_entries)
    for idx, (entry_name, run_cfg) in enumerate(run_entries, start=1):
        runtime = bootstrap(project_path)
        if run_cfg is not None:
            runtime.run = run_cfg
            split_keep = getattr(runtime.split, "keep", None)
            runtime.split_keep = run_cfg.keep or split_keep
        if keep_override:
            runtime.split_keep = keep_override
        yield idx, total_runs, entry_name, runtime


def _limit_items(items: Iterator[object], limit: Optional[int]) -> Iterator[object]:
    if limit is None:
        yield from items
    else:
        yield from islice(items, limit)


def _throttle_vectors(vectors: Iterator[Sample], throttle_ms: Optional[float]) -> Iterator[Sample]:
    if not throttle_ms or throttle_ms <= 0:
        yield from vectors
        return
    delay = throttle_ms / 1000.0
    for item in vectors:
        yield item
        time.sleep(delay)


def _serve(
    items: Iterator[object],
    limit: Optional[int],
    writer: Writer,
) -> int:
    """Iterate, write raw items, return count."""
    count = 0
    try:
        for item in _limit_items(items, limit):
            writer.write(item)
            count += 1
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
    return count


def _report_end(target: OutputTarget, count: int) -> None:
    if target.destination:
        logger.info("Saved %d items to %s", count, target.destination)
        return
    if target.transport == "stdout" and target.format in {"json-lines", "json", "jsonl"}:
        logger.info("(streamed %d items)", count)
    else:
        logger.info("(printed %d items to stdout)", count)


def _serve_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int],
    target: OutputTarget,
    include_targets: bool,
    throttle_ms: Optional[float],
    stage: Optional[int],
) -> None:
    context = PipelineContext(runtime)

    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or []) if include_targets else []
    preview_cfgs = feature_cfgs + target_cfgs

    if not preview_cfgs:
        logger.warning("(no features configured; nothing to serve)")
        return

    if stage is not None and stage <= 5:
        for cfg in preview_cfgs:
            stream = build_vector_pipeline(
                context,
                [cfg],
                dataset.group_by,
                stage=stage,
            )
            feature_target = target.for_feature(cfg.id)
            writer = writer_factory(feature_target)
            count = _serve(stream, limit, writer=writer)
            _report_end(feature_target, count)
        return

    vector_stage = 6 if stage in (6, 7) else None
    vectors = build_vector_pipeline(
        context,
        feature_cfgs,
        dataset.group_by,
        stage=vector_stage,
        target_configs=target_cfgs,
    )

    if stage in (None, 7):
        vectors = post_process(context, vectors)
    if stage is None:
        vectors = apply_split_stage(runtime, vectors)
        vectors = _throttle_vectors(vectors, throttle_ms)

    writer = writer_factory(target)
    result_count = _serve(vectors, limit, writer=writer)
    _report_end(target, result_count)


def _execute_runs(
    project_path: Path,
    stage: Optional[int],
    limit: Optional[int],
    cli_output: OutputConfig | None,
    include_targets: Optional[bool],
    keep: Optional[str],
    run_name: Optional[str],
    *,
    cli_log_level: Optional[str],
    base_log_level: str,
) -> None:
    # Helper for precedence: CLI > config > default
    def pick(cli_val, cfg_val, default=None):
        return cli_val if cli_val is not None else (cfg_val if cfg_val is not None else default)

    base_level_name = str(base_log_level).upper()
    base_level_value = _coerce_log_level(base_level_name)
    datasets = {}

    for idx, total_runs, entry_name, runtime in _iter_runtime_runs(project_path, run_name, keep):
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
                _serve_with_runtime(
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
    *,
    cli_log_level: Optional[str],
    base_log_level: str,
) -> None:
    project_path = Path(project)
    cli_output_cfg = _build_cli_output_config(
        out_transport, out_format, out_path)
    _execute_runs(
        project_path=project_path,
        stage=stage,
        limit=limit,
        cli_output=cli_output_cfg,
        include_targets=include_targets,
        keep=keep,
        run_name=run_name,
        cli_log_level=cli_log_level,
        base_log_level=base_log_level,
    )
