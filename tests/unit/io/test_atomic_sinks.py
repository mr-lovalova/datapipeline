import gzip

import pytest

from datapipeline.io.sinks.files import (
    AtomicBinaryFileSink,
    AtomicTextFileSink,
    GzipBinarySink,
)


def test_atomic_text_sink_commits_and_removes_temporary_file(tmp_path) -> None:
    destination = tmp_path / "output.txt"
    sink = AtomicTextFileSink(destination)

    sink.write_text("hello")
    assert not destination.exists()

    sink.close()

    assert destination.read_text(encoding="utf-8") == "hello"
    assert list(tmp_path.iterdir()) == [destination]


def test_atomic_text_sink_rejects_invalid_encoding_before_creating_temp_file(
    tmp_path,
) -> None:
    with pytest.raises(LookupError):
        AtomicTextFileSink(tmp_path / "output.txt", encoding="not-a-codec")

    assert list(tmp_path.iterdir()) == []


def test_atomic_binary_sink_abort_removes_temporary_file(tmp_path) -> None:
    destination = tmp_path / "output.bin"
    sink = AtomicBinaryFileSink(destination)

    sink.write_bytes(b"discarded")
    sink.abort()

    assert list(tmp_path.iterdir()) == []


def test_gzip_sink_commits_readable_file(tmp_path) -> None:
    destination = tmp_path / "output.jsonl.gz"
    sink = GzipBinarySink(destination)

    sink.write_bytes(b'{"value": 1}\n')
    sink.close()

    with gzip.open(destination, "rb") as stream:
        assert stream.read() == b'{"value": 1}\n'
    assert list(tmp_path.iterdir()) == [destination]
