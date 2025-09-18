from __future__ import annotations

import json
import pickle
import sys
from itertools import islice
from pathlib import Path
from typing import Iterator, Optional, Tuple

from tqdm import tqdm

from datapipeline.cli.openers import open_canonical_stream_visual
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, RecordDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import (
    build_feature_pipeline,
    build_record_pipeline,
    build_vector_pipeline,
)
from datapipeline.services.bootstrap import bootstrap
from datapipeline.streams.canonical import open_canonical_stream
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


def _run_records(dataset: RecordDatasetConfig, limit: int) -> None:
    for cfg in dataset.features:
        print(f"\n🍷 pouring records for {cfg.feature_id}")
        records = build_record_pipeline(cfg, open_canonical_stream_visual)
        printed = _print_head(records, limit)
        print(f"(poured {printed} records)")


def _run_features(dataset: FeatureDatasetConfig, limit: int) -> None:
    group_by = dataset.group_by
    for cfg in dataset.features:
        feature_id = getattr(cfg, "feature_id", "?")
        print(f"\n🛠️ building features for {feature_id}")
        features = build_feature_pipeline(
            cfg, group_by, open_canonical_stream_visual)
        printed = _print_head(features, limit)
        tqdm.write(f"(built {printed} feature records)")


def _run_vectors(dataset: FeatureDatasetConfig, limit: int) -> None:
    print("\n🥄 stirring vectors")
    vectors = build_vector_pipeline(
        dataset.features, dataset.group_by, open_canonical_stream_visual)
    printed = _print_head(vectors, limit)
    print(f"(stirred {printed} vectors)")


def handle_prep(action: str, project: str, limit: int = 20) -> None:
    stage_lookup = {"pour": "records", "build": "features", "stir": "vectors"}
    if action not in stage_lookup:
        raise ValueError(f"Unknown prep action: {action}")

    project_path = Path(project)
    dataset = load_dataset(project_path, stage_lookup[action])
    bootstrap(project_path)

    features = list(dataset.features or [])
    if not features:
        print("(no features configured; nothing to prep)")
        return

    if action == "pour":
        _run_records(dataset, limit)
    elif action == "build":
        _run_features(dataset, limit)
    else:
        _run_vectors(dataset, limit)


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


def handle_serve(project: str, limit: Optional[int], output: str) -> None:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    features = list(dataset.features or [])
    if not features:
        print("(no features configured; nothing to serve)")
        return

    vectors = build_vector_pipeline(
        dataset.features, dataset.group_by, open_canonical_stream)

    if output == "print":
        _serve_print(vectors, limit)
    elif output == "stream":
        _serve_stream(vectors, limit)
    elif output.endswith(".pt"):
        _serve_pt(vectors, limit, Path(output))
    else:
        print("❗ Unsupported output format. Use 'print', 'stream', or a .pt file path.")
        raise SystemExit(2)
