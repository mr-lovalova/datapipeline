import pytest

from datapipeline.io.csv_projection import CsvTableProjector


def test_csv_projection_rejects_new_columns_after_header() -> None:
    rows = iter(
        [
            {"key": "k1", "feature.temp": 1.0},
            {"key": "k2", "feature.temp": 2.0, "feature.new": 9.0},
        ]
    )
    projector = CsvTableProjector(lambda _item: next(rows))
    projector.project(object())

    with pytest.raises(
        ValueError, match="CSV row contains fields not present in header"
    ):
        projector.project(object())


def test_csv_projection_preserves_header_order_when_rows_reorder_fields() -> None:
    rows = iter(
        [
            {"second": 2, "first": 1},
            {"first": 3, "second": 4},
        ]
    )
    projector = CsvTableProjector(lambda _item: next(rows))

    first = projector.project(object())
    second = projector.project(object())

    assert first.header == ["second", "first"]
    assert first.values == [2, 1]
    assert second.header == ["second", "first"]
    assert second.values == [4, 3]
