import csv

import pytest

from datapipeline.io.writers.csv_writer import CsvFileWriter


def test_csv_writer_uses_projected_header(tmp_path) -> None:
    rows = iter(
        [
            {"key": "k1", "feature.temp": 1.0, "feature.wind.0": 2.0},
            {"key": "k2", "feature.temp": 3.0, "feature.wind.0": 4.0},
        ]
    )

    class _Serializer:
        def __call__(self, _item):
            return next(rows)

    dest = tmp_path / "out.csv"
    writer = CsvFileWriter(dest, serializer=_Serializer())
    writer.write(object())
    writer.write(object())
    writer.close()

    with open(dest, newline="", encoding="utf-8") as fh:
        parsed = list(csv.DictReader(fh))

    assert parsed[0]["key"] == "k1"
    assert parsed[0]["feature.temp"] == "1.0"
    assert parsed[1]["key"] == "k2"


def test_csv_writer_raises_on_new_columns_after_header(tmp_path) -> None:
    rows = iter(
        [
            {"key": "k1", "feature.temp": 1.0},
            {"key": "k2", "feature.temp": 2.0, "feature.new": 9.0},
        ]
    )

    class _Serializer:
        def __call__(self, _item):
            return next(rows)

    dest = tmp_path / "out.csv"
    writer = CsvFileWriter(dest, serializer=_Serializer())
    writer.write(object())

    with pytest.raises(ValueError, match="CSV row contains fields not present in header"):
        writer.write(object())
    writer.close()


def test_csv_writer_honors_configured_encoding(tmp_path) -> None:
    rows = iter([{"key": "k1", "feature.temp": 1.0}])

    class _Serializer:
        def __call__(self, _item):
            return next(rows)

    dest = tmp_path / "out.csv"
    writer = CsvFileWriter(dest, serializer=_Serializer(), encoding="utf-8-sig")
    writer.write(object())
    writer.close()

    raw = dest.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
