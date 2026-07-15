import csv

from datapipeline.io.writers.csv_writer import CsvFileWriter


def test_csv_writer_writes_flattened_rows(tmp_path) -> None:
    dest = tmp_path / "out.csv"
    writer = CsvFileWriter(dest)
    writer.write({"key": "k1", "feature": {"temp": 1.0, "wind": [2.0]}})
    writer.write({"key": "k2", "feature": {"temp": 3.0, "wind": [4.0]}})
    writer.close()

    with open(dest, newline="", encoding="utf-8") as fh:
        parsed = list(csv.DictReader(fh))

    assert parsed[0]["key"] == "k1"
    assert parsed[0]["feature.temp"] == "1.0"
    assert parsed[1]["key"] == "k2"


def test_csv_writer_honors_configured_encoding(tmp_path) -> None:
    dest = tmp_path / "out.csv"
    writer = CsvFileWriter(dest, encoding="utf-8-sig")
    writer.write({"key": "k1", "feature": {"temp": 1.0}})
    writer.close()

    raw = dest.read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")
