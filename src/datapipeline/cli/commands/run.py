import sys
import time
from itertools import islice
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from datapipeline.cli.visuals import visual_sources
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.run import RunConfig, load_named_run_configs
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process, split_stage
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap
from datapipeline.cli.commands.writers import writer_factory, Writer


def _resolve_run_entries(project_path: Path, run_name: Optional[str]) -> List[Tuple[Optional[str], Optional[RunConfig]]]:
    try:
        entries = load_named_run_configs(project_path)
    except FileNotFoundError:
        entries = []
    except Exception as exc:
        print(f"Error: failed to load run configs: {exc}")
        raise SystemExit(2) from exc

    if entries:
        if run_name:
            entries = [entry for entry in entries if entry[0] == run_name]
            if not entries:
                print(f"Error: unknown run config '{run_name}'")
                raise SystemExit(2)
    else:
        if run_name:
            print("Error: project does not define run configs.")
            raise SystemExit(2)
        entries = [(None, None)]
    return entries


def _iter_runtime_runs(
    project_path: Path,
    run_name: Optional[str],
    keep_override: Optional[str],
) -> Iterator[Tuple[int, int, Optional[str], Runtime]]:
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


def _limit_items(items: Iterator[Tuple[object, object]], limit: Optional[int]) -> Iterator[Tuple[object, object]]:
    if limit is None:
        yield from items
    else:
        yield from islice(items, limit)


def _throttle_vectors(vectors: Iterator[Tuple[object, Vector]], throttle_ms: Optional[float]) -> Iterator[Tuple[object, Vector]]:
    if not throttle_ms or throttle_ms <= 0:
        yield from vectors
        return
    delay = throttle_ms / 1000.0
    for item in vectors:
        yield item
        time.sleep(delay)


def _normalize(key: object, payload: object) -> dict:
    return {
        "key": list(key) if isinstance(key, tuple) else key,
        "values": getattr(payload, "values", payload),
    }


def _serve(
    items: Iterator[Tuple[object, object]],
    limit: Optional[int],
    *,
    writer: Writer,
) -> int:
    """Iterate, normalize, write, return count. Writers do only I/O."""
    count = 0
    try:
        for key, payload in _limit_items(items, limit):
            writer.write(_normalize(key, payload))
            count += 1
    except KeyboardInterrupt:
        pass
    finally:
        writer.close()
    return count


def _report_end(output: Optional[str], count: int, verbosity: int) -> None:
    if verbosity < 1:
        return
    mode = (output or "print").lower()
    if output and output.lower().endswith(".pt"):
        print(f"Saved {count} items to {output}", file=sys.stderr)
    elif output and output.lower().endswith(".csv"):
        print(f"Saved {count} items to {output}", file=sys.stderr)
    elif output and (output.lower().endswith(".jsonl.gz") or output.lower().endswith(".gz")):
        print(f"Saved {count} items to {output}", file=sys.stderr)
    elif mode == "stream":
        print(f"(streamed {count} items)", file=sys.stderr)
    elif mode == "print":
        print(f"(printed {count} items to stdout)", file=sys.stderr)
    else:
        raise ValueError("unreachable: unknown output mode in _report_end")


def _serve_with_runtime(
    runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int],
    output: Optional[str],
    include_targets: bool,
    throttle_ms: Optional[float],
    verbosity: int,
    stage: Optional[int] = None,
) -> None:
    context = PipelineContext(runtime)

    feature_configs = list(dataset.features or [])
    if not feature_configs:
        print("(no features configured; nothing to serve)")
        return

    if stage is not None and stage <= 5:
        preview_configs: List[FeatureRecordConfig] = list(feature_configs)
        preview_configs += list(dataset.targets or [])
        for cfg in preview_configs:
            stream = build_vector_pipeline(
                context,
                [cfg],
                dataset.group_by,
                stage=stage,
            )
            items = ((cfg.id, item) for item in stream)
            writer = writer_factory(output)
            count = _serve(items, limit, writer=writer)
            _report_end(output, count, verbosity)
        return

    configs: List[FeatureRecordConfig] = list(feature_configs)
    if include_targets:
        configs += list(dataset.targets or [])

    vector_stage = 6 if stage in (6, 7) else None
    vectors = build_vector_pipeline(
        context,
        configs,
        dataset.group_by,
        stage=vector_stage,
    )

    if stage in (None, 7):
        vectors = post_process(context, vectors)
    if stage is None:
        vectors = split_stage(runtime, vectors)
        vectors = _throttle_vectors(vectors, throttle_ms)

    writer = writer_factory(output)
    result_count = _serve(vectors, limit, writer=writer)
    _report_end(output, result_count, verbosity)


def _execute_runs(
    project_path: Path,
    stage: Optional[int],
    limit: Optional[int],
    output: Optional[str],
    include_targets: Optional[bool],
    verbosity: Optional[int],
    keep: Optional[str],
    run_name: Optional[str],
) -> None:
    # Helper for precedence: CLI > config > default
    def pick(cli_val, cfg_val, default=None):
        return cli_val if cli_val is not None else (cfg_val if cfg_val is not None else default)

    dataset_name = "vectors" if stage is None else "features"
    dataset = load_dataset(project_path, dataset_name)

    for idx, total_runs, entry_name, runtime in _iter_runtime_runs(project_path, run_name, keep):
        run = getattr(runtime, "run", None)

        # resolving argument hierarchy CLI args > run config > defaults
        resolved_verbosity = pick(
            verbosity, getattr(run, "verbosity", None), 0)
        resolved_limit = pick(limit, getattr(run, "limit", None), None)
        resolved_output = pick(output, getattr(run, "output", None), "print")
        resolved_include_targets = pick(
            include_targets, getattr(run, "include_targets", None), False)
        throttle_ms = getattr(run, "throttle_ms", None)

        if resolved_verbosity >= 1:
            label = entry_name or f"run{idx}"
            print(f"\nRun '{label}' ({idx}/{total_runs})")

        with visual_sources(runtime, resolved_verbosity):
            _serve_with_runtime(
                runtime,
                dataset,
                limit=resolved_limit,
                output=resolved_output,
                include_targets=resolved_include_targets,
                throttle_ms=throttle_ms,
                verbosity=resolved_verbosity,
                stage=stage,
            )


def handle_serve(
    project: str,
    limit: Optional[int],
    output: Optional[str],
    include_targets: Optional[bool] = None,
    keep: Optional[str] = None,
    verbosity: Optional[int] = None,
    run_name: Optional[str] = None,
    stage: Optional[int] = None,
) -> None:
    project_path = Path(project)
    _execute_runs(
        project_path=project_path,
        stage=stage,
        limit=limit,
        output=output,
        include_targets=include_targets,
        verbosity=verbosity,
        keep=keep,
        run_name=run_name,
    )
