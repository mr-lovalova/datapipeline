import json
import logging

from datapipeline.analysis.vector.collector import VectorStatsCollector


def test_vector_analyzer_summary_is_serializable(caplog):
    collector = VectorStatsCollector(expected_feature_ids=["speed"])

    collector.update("2024-01-01T00:00", {"speed__stationA": 1.0})
    collector.update("2024-01-01T01:00", {"speed__stationA": 2.0})

    with caplog.at_level(logging.INFO, logger="datapipeline.analysis.vector.report"):
        summary = collector.print_report()

    json.dumps(summary)

    assert any("Vector Quality Report" in record.message
               for record in caplog.records)
