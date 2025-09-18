from pathlib import Path
from typing import Optional

from datapipeline.services.bootstrap import bootstrap
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.cli.openers import open_canonical_stream_visual
from datapipeline.analysis.vector_analyzer import VectorStatsCollector


def analyze(project: str, limit: Optional[int] = None) -> None:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    expected_feature_ids = [cfg.feature_id for cfg in (dataset.features or [])]
    if not expected_feature_ids:
        print("(no features configured; nothing to analyze)")
        return

    collector = VectorStatsCollector(expected_feature_ids)

    count = 0
    for group_key, vector in build_vector_pipeline(
        dataset.features, dataset.group_by, open_canonical_stream_visual
    ):
        collector.update(group_key, vector.values)
        count += 1
        if limit and count >= limit:
            break

    collector.print_report()
