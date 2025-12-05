import json
import logging

from datapipeline.analysis.vector.collector import VectorStatsCollector


def test_vector_analyzer_summary_is_serializable(caplog):
    collector = VectorStatsCollector(
        expected_feature_ids=["speed", "temp"],
        threshold=0.8,
    )

    collector.update("g0", {"speed__stationA": None})
    collector.update("g1", {"temp": 7.0})

    with caplog.at_level(logging.INFO, logger="datapipeline.analysis.vector.report"):
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

    assert any("Vector Quality Report" in record.message for record in caplog.records)
