import json
import pickle
import sys
import time
from contextlib import nullcontext
from itertools import islice
from pathlib import Path
from typing import Iterator, Optional, Tuple

from tqdm import tqdm
from datapipeline.cli.visual_source import visual_sources
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import (
    build_feature_pipeline,
    build_vector_pipeline,
)
from datapipeline.config.run import load_named_run_configs
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.stages import post_process
from datapipeline.services.bootstrap import bootstrap
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.stages import split_stage


def _print_head(iterable: Iterator[object], limit: int) -> int:
    count = 0
    try:
        for item in iterable:
            tqdm.write(str(item))
            count += 1
            if count >= limit:
                break
    except KeyboardInterrupt:
        pass
    return count


def _run_feature_stage(runtime, dataset: FeatureDatasetConfig, stage: int, limit: int) -> None:
    """Preview a numeric feature/vector stage.

    Stages 0–5 preview the first configured feature.
    Stage 6 assembles merged vectors (no vector transforms).
    Stage 7 applies vector transforms.
    """
    group_by = dataset.group_by
    context = PipelineContext(runtime)

    # Vector stages (merged across all features)
    if stage in (6, 7):
        stream = build_vector_pipeline(context, dataset.features, group_by, stage=6)
        if stage == 7:
            stream = post_process(context, stream)
        printed = _print_head(stream, limit)
        label = "assembled" if stage == 6 else "transformed"
        print(f"({label} {printed} vectors)")
        return

    # Feature stages (per-feature preview; show only the first configured)
    stage_labels = {
        0: ("📦", "source DTO's", "read {n} dto records"),
        1: ("🧪", "domain records", "mapped {n} records"),
        2: ("🍷", "records conditional steps", "poured {n} records"),
        3: ("🧱", "building features", "built {n} feature records"),
        4: ("🔎", "wrap only (partition_by)", "wrapped {n} feature records"),
        5: ("🧰", "feature transforms/sequence", "transformed {n} feature records"),
    }

    if stage not in stage_labels:
        print("❗ Unsupported stage. Use 0–5 for features, 6–7 for vectors.")
        raise SystemExit(2)

    icon, title, summary = stage_labels[stage]

    for cfg in dataset.features + dataset.targets:
        print(f"\n{icon} {title} for {cfg.id}")
        stream = build_feature_pipeline(context, cfg, stage=stage)
        printed = _print_head(stream, limit)
        print(f"({summary.format(n=printed)})")


def handle_prep_stage(project: str, stage: int, limit: Optional[int] = None, visuals: Optional[bool] = None) -> None:
    """Preview a numeric feature stage (0-5) for all configured features."""
    project_path = Path(project)
    dataset = load_dataset(project_path, "features")
    runtime = bootstrap(project_path)
    run_opts = getattr(runtime, "run", None)
    resolved_limit = (
        limit
        if limit is not None
        else (run_opts.limit if run_opts and run_opts.limit is not None else 20)
    )
    if visuals is None:
        visuals = (
            run_opts.visuals
            if run_opts and run_opts.visuals is not None
            else True
        )
    prep_ctx = visual_sources(runtime) if visuals else nullcontext()
    with prep_ctx:
        _run_feature_stage(runtime, dataset, stage, resolved_limit)


def _limit_vectors(vectors: Iterator[Tuple[object, Vector]], limit: Optional[int]) -> Iterator[Tuple[object, Vector]]:
    if limit is None:
        yield from vectors
    else:
        yield from islice(vectors, limit)


def _throttle_vectors(
    vectors: Iterator[Tuple[object, Vector]],
    throttle_ms: Optional[float],
) -> Iterator[Tuple[object, Vector]]:
    if not throttle_ms or throttle_ms <= 0:
        yield from vectors
        return
    delay = throttle_ms / 1000.0
    for item in vectors:
        yield item
        time.sleep(delay)


def _serve_print(vectors: Iterator[Tuple[object, Vector]], limit: Optional[int]) -> None:
    count = 0
    try:
        for group_key, vector in _limit_vectors(vectors, limit):
            print(f"group={group_key}: {vector.values}")
            count += 1
    except KeyboardInterrupt:
        pass
    print(f"(served {count} vectors to stdout)")


def _serve_stream(vectors: Iterator[Tuple[object, Vector]], limit: Optional[int]) -> None:
    count = 0
    try:
        for group_key, vector in _limit_vectors(vectors, limit):
            payload = {"group": list(group_key) if isinstance(group_key, tuple) else group_key,
                       "values": vector.values}
            print(json.dumps(payload, default=str))
            count += 1
    except KeyboardInterrupt:
        pass
    print(f"(streamed {count} vectors)", file=sys.stderr)


def _serve_pt(vectors: Iterator[Tuple[object, Vector]], limit: Optional[int], destination: Path) -> None:
    data = []
    for group_key, vector in _limit_vectors(vectors, limit):
        normalized_key = list(group_key) if isinstance(
            group_key, tuple) else group_key
        data.append((normalized_key, vector.values))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as fh:
        pickle.dump(data, fh)
    print(f"💾 Saved {len(data)} vectors to {destination}")


def _serve_with_runtime(
    runtime,
    dataset: FeatureDatasetConfig,
    *,
    limit: Optional[int],
    output: str,
    include_targets: bool,
    throttle_ms: Optional[float],
    visuals: bool,
) -> None:
    context = PipelineContext(runtime)

    features = list(dataset.features or [])
    if not features:
        print("(no features configured; nothing to serve)")
        return

    configs = list(dataset.features or [])
    if include_targets:
        configs += list(dataset.targets or [])

    serve_ctx = visual_sources(runtime) if visuals else nullcontext()
    with serve_ctx:
        vectors = build_vector_pipeline(context, configs, dataset.group_by)
        # Apply global postprocess transforms first (fills/coverage)
        vectors = post_process(context, vectors)
        # Finally, apply configured split (if any) via a dedicated stage
        vectors = split_stage(runtime, vectors)
        # Throttle emission if configured
        vectors = _throttle_vectors(vectors, throttle_ms)

        if output == "print":
            _serve_print(vectors, limit)
        elif output == "stream":
            _serve_stream(vectors, limit)
        elif output and output.endswith(".pt"):
            _serve_pt(vectors, limit, Path(output))
        else:
            print("❗ Unsupported output format. Use 'print', 'stream', or a .pt file path.")
            raise SystemExit(2)


def handle_serve(
    project: str,
    limit: Optional[int],
    output: Optional[str],
    include_targets: Optional[bool] = None,
    keep: Optional[str] = None,
    visuals: Optional[bool] = None,
    run_name: Optional[str] = None,
) -> None:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    try:
        run_entries = load_named_run_configs(project_path)
    except FileNotFoundError:
        run_entries = []
    except Exception as exc:
        print(f"❗ Failed to load run configs: {exc}")
        raise SystemExit(2) from exc

    if run_entries:
        if run_name:
            run_entries = [entry for entry in run_entries if entry[0] == run_name]
            if not run_entries:
                print(f"❗ Unknown run config: {run_name}")
                raise SystemExit(2)
    else:
        if run_name:
            print("❗ Project does not define run configs.")
            raise SystemExit(2)
        run_entries = [(None, None)]

    total_runs = len(run_entries)
    for idx, (entry_name, run_cfg) in enumerate(run_entries, start=1):
        runtime = bootstrap(project_path)
        if run_cfg is not None:
            runtime.run = run_cfg
            split_keep = getattr(runtime.split, "keep", None)
            runtime.split_keep = run_cfg.keep or split_keep
        run_opts = runtime.run

        resolved_output = (
            output
            if output is not None
            else (run_opts.output if run_opts and run_opts.output else "print")
        )
        resolved_limit = (
            limit if limit is not None else (run_opts.limit if run_opts else None)
        )
        resolved_include_targets = (
            include_targets
            if include_targets is not None
            else (run_opts.include_targets if run_opts else False)
        )
        throttle_ms = run_opts.throttle_ms if run_opts else None
        resolved_visuals = (
            visuals
            if visuals is not None
            else (
                run_opts.visuals
                if run_opts and run_opts.visuals is not None
                else False
            )
        )

        if keep:
            runtime.split_keep = keep

        if total_runs > 1:
            label = entry_name or f"run{idx}"
            print(f"\n🥃 Run '{label}' ({idx}/{total_runs})")

        _serve_with_runtime(
            runtime,
            dataset,
            limit=resolved_limit,
            output=resolved_output,
            include_targets=resolved_include_targets,
            throttle_ms=throttle_ms,
            visuals=resolved_visuals,
        )
