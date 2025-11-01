import json

from datapipeline.analysis.vector.collector import VectorStatsCollector


def test_vector_analyzer_summary_is_serializable(capfd):
    collector = VectorStatsCollector(expected_feature_ids=["speed"])

    collector.update("2024-01-01T00:00", {"speed__stationA": 1.0})
    collector.update("2024-01-01T01:00", {"speed__stationA": 2.0})

    summary = collector.print_report()

    json.dumps(summary)

    captured = capfd.readouterr()
    assert "Vector Quality Report" in captured.out
