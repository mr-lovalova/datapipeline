import json
from pathlib import Path
from typing import Dict, Iterator, Tuple

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.analysis.vector.snapshot import snapshot_from_collector
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.tasks import StatsTask
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent


def _merge_sample_values(sample) -> dict:
    merged = dict(sample.features.values)
    if sample.targets:
        merged.update(sample.targets.values)
    return merged


def _iter_merged_vectors(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    *,
    apply_postprocess: bool,
) -> Iterator[tuple[object, dict]]:
    context = PipelineContext(runtime)
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or [])

    context.window_bounds(rectangular_required=True)
    vectors = build_vector_pipeline(
        context,
        feature_cfgs,
        dataset.group_by,
        target_configs=target_cfgs,
        rectangular=True,
    )
    if apply_postprocess:
        vectors = post_process(context, vectors)

    for sample in vectors:
        yield sample.key, _merge_sample_values(sample)


def materialize_vector_stats(
    runtime: Runtime,
    task_cfg: StatsTask,
) -> Tuple[str, Dict[str, object]] | None:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    expected_feature_ids = [cfg.id for cfg in (dataset.features or [])]
    schema_entries = PipelineContext(runtime).load_schema(payload="features")
    schema_meta = {
        entry["id"]: entry
        for entry in (schema_entries or [])
        if isinstance(entry.get("id"), str)
    }

    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition="base",
        schema_meta=schema_meta,
        threshold=None,
        show_matrix=False,
    )
    apply_postprocess = task_cfg.mode == "final"
    for key, merged in _iter_merged_vectors(
        runtime,
        dataset,
        apply_postprocess=apply_postprocess,
    ):
        collector.update(key, merged)

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    ensure_parent(destination)
    with destination.open("w", encoding="utf-8") as fh:
        json.dump(snapshot_from_collector(collector), fh, indent=2)

    meta: Dict[str, object] = {
        "mode": task_cfg.mode,
        "vectors": collector.total_vectors,
        "features": len(collector.discovered_features),
        "partitions": len(collector.discovered_partitions),
    }
    return str(relative_path), meta
