import json

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.analysis.vector.snapshot import (
    collector_from_snapshot,
    snapshot_from_collector,
)


def test_vector_analyzer_summary_is_serializable(capsys):
    collector = VectorStatsCollector(
        expected_feature_ids=["speed", "temp"],
        threshold=0.8,
    )

    collector.update("g0", {"speed__stationA": None})
    collector.update("g1", {"temp": 7.0})

    summary = collector.print_report()

    json.dumps(summary)

    assert summary["total_vectors"] == 2
    assert set(summary["below_features"]) == {"speed", "temp"}
    assert set(summary["below_partitions"]) == {"speed__stationA", "temp"}

    speed_stats = next(item for item in summary["feature_stats"] if item["id"] == "speed")
    assert speed_stats["present"] == 1  # seen once, missing once
    assert speed_stats["missing"] == 1
    assert speed_stats["nulls"] == 1
    temp_stats = next(item for item in summary["feature_stats"] if item["id"] == "temp")
    assert temp_stats["present"] == 1
    assert temp_stats["missing"] == 1

    captured = capsys.readouterr()
    assert "Vector Quality Report" in captured.out


def test_vector_analyzer_snapshot_round_trip():
    collector = VectorStatsCollector(
        expected_feature_ids=["speed", "temp"],
        threshold=0.8,
    )
    collector.update("g0", {"speed": [1.0, None], "temp": 2.0})
    collector.update("g1", {"speed": [None, None], "temp": None})

    snapshot = snapshot_from_collector(collector)
    restored = collector_from_snapshot(
        snapshot,
        threshold=0.95,
        show_matrix=False,
        matrix_rows=20,
        matrix_cols=10,
        matrix_output=None,
        matrix_format="html",
    )

    assert restored.total_vectors == collector.total_vectors
    assert restored.seen_counts["speed"] == collector.seen_counts["speed"]
    assert restored.null_counts_partitions["temp"] == collector.null_counts_partitions["temp"]
    assert "g0" in restored.group_feature_status
