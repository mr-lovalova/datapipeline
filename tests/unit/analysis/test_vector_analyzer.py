import csv
import json

import pytest

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.analysis.vector import report as report_module
from datapipeline.analysis.vector.matrix import export_matrix_data
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


def test_vector_report_characterizes_matrix_side_effects(monkeypatch, tmp_path):
    collector = VectorStatsCollector(
        expected_feature_ids=["speed", "temp"],
        threshold=0.8,
        show_matrix=True,
        matrix_output=str(tmp_path / "matrix.csv"),
        matrix_format="csv",
    )
    collector.update("g0", {"speed__station:A": 1.0})
    collector.update("g1", {"temp__station:B": None})

    rendered = []
    exported = []
    monkeypatch.setattr(
        report_module,
        "render_matrix",
        lambda collector, **kwargs: rendered.append(kwargs),
    )
    monkeypatch.setattr(
        report_module,
        "export_matrix_data",
        lambda collector: exported.append(collector.matrix_output),
    )

    summary = collector.print_report()

    assert set(summary["below_features"]) == {"speed", "temp"}
    assert set(summary["below_partitions"]) == {
        "speed__station:A",
        "temp__station:B",
    }
    assert set(summary["below_suffixes"]) == {"station:A", "station:B"}
    assert set(summary["below_partition_values"]) == {"A", "B"}
    assert set(rendered[0]["features"]) == {"speed", "temp"}
    assert rendered[1]["partitions"] is True
    assert set(rendered[1]["features"]) == {
        "speed__station:A",
        "temp__station:B",
    }
    assert exported == [collector.matrix_output]


def test_export_matrix_data_writes_csv_rows(tmp_path):
    path = tmp_path / "matrix.csv"
    collector = VectorStatsCollector(
        expected_feature_ids=["speed", "temp"],
        matrix_output=str(path),
        matrix_format="csv",
    )
    collector.update("g0", {"speed__station:A": 1.0})
    collector.update("g1", {"temp__station:B": None})

    assert export_matrix_data(collector) == path

    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    assert {
        (row["kind"], row["identifier"], row["group_key"], row["status"])
        for row in rows
    } == {
        ("feature", "speed", "g0", "present"),
        ("feature", "temp", "g0", "absent"),
        ("feature", "temp", "g1", "null"),
        ("feature", "speed", "g1", "absent"),
        ("partition", "speed__station:A", "g0", "present"),
        ("partition", "speed__station:A", "g1", "absent"),
        ("partition", "temp__station:B", "g1", "null"),
    }


def test_export_matrix_data_writes_html_shell(tmp_path):
    path = tmp_path / "matrix.html"
    collector = VectorStatsCollector(
        expected_feature_ids=["speed"],
        matrix_output=str(path),
        matrix_format="html",
    )
    collector.update("g0", {"speed__station:A": [1.0, None]})

    assert export_matrix_data(collector) == path

    html = path.read_text(encoding="utf-8")
    assert "<h1>Availability Matrix</h1>" in html
    assert "<h2>Feature Availability</h2>" in html
    assert "<h2>Partition Availability</h2>" in html
    assert "setupMatrix('feature'" in html
    assert "setupMatrix('partition'" in html


def test_vector_analyzer_snapshot_round_trip():
    collector = VectorStatsCollector(
        expected_feature_ids=["speed", "temp"],
        threshold=0.8,
    )
    collector.update("g0", {"speed": [1.0, None], "temp": 2.0})
    collector.update("g1", {"speed": [None, None], "temp": None})

    snapshot = snapshot_from_collector(collector)
    assert "schema_meta" not in snapshot
    assert "expected_features" not in snapshot
    assert "discovered_features" not in snapshot
    assert "discovered_partitions" not in snapshot
    restored = collector_from_snapshot(
        snapshot,
        expected_feature_ids=["speed", "temp"],
        schema_meta={},
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


def test_vector_analyzer_snapshot_rejects_unknown_schema_version():
    with pytest.raises(ValueError, match="Unsupported vector stats snapshot schema version"):
        collector_from_snapshot(
            {
                "schema_version": 999,
                "group_feature_status": {},
                "group_partition_status": {},
                "group_feature_sub": {},
                "group_partition_sub": {},
            },
            expected_feature_ids=[],
            schema_meta={},
            threshold=0.95,
            show_matrix=False,
            matrix_rows=20,
            matrix_cols=10,
            matrix_output=None,
            matrix_format="html",
        )
