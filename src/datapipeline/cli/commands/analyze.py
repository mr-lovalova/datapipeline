from pathlib import Path

from datapipeline.services.bootstrap import bootstrap
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.cli.openers import open_canonical_stream_visual
from datapipeline.analysis.vector_analyzer import VectorStatsCollector


def analyze(
    project: str,
    threshold: float = 0.95,
    show_matrix: bool = False,
    matrix_rows: int = 20,
    matrix_cols: int = 10,
    matrix_output: str | None = None,
    matrix_format: str = "csv",
) -> None:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    expected_feature_ids = [cfg.feature_id for cfg in (dataset.features or [])]

    match_partition = "base"
    if not expected_feature_ids:
        print("(no features configured; nothing to analyze)")
        return

    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition=match_partition,
        threshold=threshold,
        show_matrix=show_matrix,
        matrix_rows=matrix_rows,
        matrix_cols=matrix_cols,
        matrix_output=matrix_output,
        matrix_format=matrix_format,
    )

    for group_key, vector in build_vector_pipeline(
        dataset.features,
        dataset.group_by,
        open_canonical_stream_visual,
        None,
    ):
        collector.update(group_key, vector.values)

    collector.print_report()
