import json
import pickle
import sys
from itertools import islice
from pathlib import Path
from typing import Iterator, Optional, Tuple

from tqdm import tqdm
from datapipeline.cli.visual_source import visual_sources
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import (
    build_feature_pipeline,
    build_pipeline
)
from datapipeline.services.bootstrap import bootstrap
from datapipeline.domain.vector import Vector


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


def _run_feature_stage(dataset: FeatureDatasetConfig, stage: int, limit: int) -> None:
    """Preview a numeric feature/vector stage.

    Stages 0–5 preview the first configured feature.
    Stage 6 assembles merged vectors (no vector transforms).
    Stage 7 applies vector transforms.
    """
    group_by = dataset.group_by

    # Vector stages (merged across all features)
    if stage in (6, 7):
        stream = build_pipeline(dataset.features, group_by, stage=stage)
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
        stream = build_feature_pipeline(cfg, stage=stage)
        printed = _print_head(stream, limit)
        print(f"({summary.format(n=printed)})")


def handle_prep_stage(project: str, stage: int, limit: int = 20) -> None:
    """Preview a numeric feature stage (0-5) for all configured features."""
    project_path = Path(project)
    dataset = load_dataset(project_path, "features")
    bootstrap(project_path)
    with visual_sources():
        _run_feature_stage(dataset, stage, limit)


def _limit_vectors(vectors: Iterator[Tuple[object, Vector]], limit: Optional[int]) -> Iterator[Tuple[object, Vector]]:
    if limit is None:
        yield from vectors
    else:
        yield from islice(vectors, limit)


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


def handle_serve(project: str, limit: Optional[int], output: str, include_targets: bool = False) -> None:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    features = list(dataset.features or [])
    if not features:
        print("(no features configured; nothing to serve)")
        return

    configs = list(dataset.features or [])
    if include_targets:
        configs += list(dataset.targets or [])
    vectors = build_pipeline(
        configs,
        dataset.group_by,
        dataset.vector_transforms,
    )

    if output == "print":
        _serve_print(vectors, limit)
    elif output == "stream":
        _serve_stream(vectors, limit)
    elif output.endswith(".pt"):
        _serve_pt(vectors, limit, Path(output))
    else:
        print("❗ Unsupported output format. Use 'print', 'stream', or a .pt file path.")
        raise SystemExit(2)
