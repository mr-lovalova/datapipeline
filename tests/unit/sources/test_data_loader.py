import gzip
from urllib.parse import parse_qsl, urlparse

import pytest

import datapipeline.sources.adapters.fs as fs_adapter
import datapipeline.sources.adapters.http as http_adapter
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.factory import build_loader
from datapipeline.sources.ports import SourceResource, SourceTransport


def _write_gzip(path, text: str) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="") as stream:
        stream.write(text)


def test_fs_path_selects_file_or_glob_transport(tmp_path) -> None:
    (tmp_path / "rows.jsonl").write_text("", encoding="utf-8")
    file_loader = build_loader("fs", "jsonl", path=str(tmp_path / "rows.jsonl"))
    glob_loader = build_loader("fs", "jsonl", path=str(tmp_path / "*.jsonl"))

    assert isinstance(file_loader.transport, FsFileTransport)
    assert isinstance(glob_loader.transport, FsGlobTransport)


def test_fs_file_loader_decompresses_gzip_explicitly(tmp_path) -> None:
    path = tmp_path / "rows.data"
    _write_gzip(path, '{"value":1}\n{"value":2}\n')

    loader = build_loader(
        "fs",
        "jsonl",
        path=str(path),
        compression="gzip",
    )

    assert list(loader.load()) == [{"value": 1}, {"value": 2}]
    assert loader.transport.compression == "gzip"


def test_fs_glob_loader_decompresses_each_gzip_file(tmp_path) -> None:
    _write_gzip(tmp_path / "01.data", '{"value":1}\n')
    _write_gzip(tmp_path / "02.data", '{"value":2}\n')

    loader = build_loader(
        "fs",
        "jsonl",
        path=str(tmp_path / "*.data"),
        compression="gzip",
    )

    assert list(loader.load()) == [{"value": 1}, {"value": 2}]
    assert loader.transport.compression == "gzip"


def test_fs_csv_loader_decompresses_gzip(tmp_path) -> None:
    path = tmp_path / "rows.data"
    _write_gzip(path, "value;label\n1;one\n")

    loader = build_loader(
        "fs",
        "csv",
        path=str(path),
        compression="gzip",
    )

    assert list(loader.load()) == [{"value": "1", "label": "one"}]


def test_fs_loader_does_not_infer_compression_from_suffix(tmp_path) -> None:
    path = tmp_path / "rows.jsonl.gz"
    _write_gzip(path, '{"value":1}\n')

    loader = build_loader("fs", "jsonl", path=str(path))

    with pytest.raises(UnicodeDecodeError):
        list(loader.load())


def test_explicit_empty_encoding_is_not_replaced_with_default(tmp_path) -> None:
    path = tmp_path / "rows.jsonl"
    path.write_text('{"value":1}\n', encoding="utf-8")
    loader = build_loader("fs", "jsonl", path=str(path), encoding="")

    with pytest.raises(LookupError):
        list(loader.load())


def test_fs_gzip_loader_closes_file_after_partial_read(tmp_path, monkeypatch) -> None:
    path = tmp_path / "rows.data"
    _write_gzip(path, '{"value":1}\n{"value":2}\n')

    class TrackedStream:
        def __init__(self):
            self.stream = gzip.open(path, "rb")
            self.closed = False

        def read(self, size):
            return self.stream.read(size)

        def close(self):
            self.closed = True
            self.stream.close()

    tracked = TrackedStream()
    monkeypatch.setattr(fs_adapter.gzip, "open", lambda *args, **kwargs: tracked)
    rows = build_loader(
        "fs",
        "jsonl",
        path=str(path),
        compression="gzip",
    ).load()

    assert next(rows) == {"value": 1}
    rows.close()

    assert tracked.closed


def test_fs_glob_requires_at_least_one_file(tmp_path) -> None:
    pattern = str(tmp_path / "*.jsonl")

    with pytest.raises(FileNotFoundError, match="Source glob matched no files"):
        build_loader("fs", "jsonl", path=pattern)


def test_fs_loader_rejects_removed_glob_option(tmp_path) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'glob'"):
        build_loader(
            "fs",
            "jsonl",
            path=str(tmp_path / "*.jsonl"),
            glob=True,
        )


def test_loader_rejects_misspelled_option(tmp_path) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'delimeter'"):
        build_loader(
            "fs",
            "csv",
            path=str(tmp_path / "rows.csv"),
            delimeter=",",
        )


def test_loader_rejects_gzip_for_json(tmp_path) -> None:
    with pytest.raises(
        ValueError,
        match="gzip compression is supported only for csv and jsonl formats",
    ):
        build_loader(
            "fs",
            "json",
            path=str(tmp_path / "rows.gz"),
            compression="gzip",
        )


def test_loader_rejects_pickle_format(tmp_path) -> None:
    with pytest.raises(ValueError, match="unsupported format for IO loader: pickle"):
        build_loader(
            "fs",
            "pickle",
            path=str(tmp_path / "rows.pkl"),
        )


def test_loader_rejects_compression_for_http() -> None:
    with pytest.raises(ValueError, match="compression is supported only for fs"):
        build_loader(
            "http",
            "jsonl",
            url="https://example.test/rows.jsonl.gz",
            compression="gzip",
        )


def test_http_transport_merges_existing_and_configured_query_parameters() -> None:
    transport = HttpTransport(
        "https://example.test/rows?existing=1",
        params={"symbol": ["AAPL", "MSFT"], "empty": ""},
    )

    query = parse_qsl(urlparse(transport._build_url()).query, keep_blank_values=True)

    assert query == [
        ("existing", "1"),
        ("symbol", "AAPL"),
        ("symbol", "MSFT"),
        ("empty", ""),
    ]


def test_http_transport_does_not_drop_parameters_when_encoding_fails(
    monkeypatch,
) -> None:
    class InvalidParameter:
        def __str__(self) -> str:
            raise RuntimeError("invalid parameter")

    opened = False

    def open_url(request, timeout):
        nonlocal opened
        opened = True
        raise AssertionError("request must not be opened")

    monkeypatch.setattr(http_adapter, "urlopen", open_url)
    transport = HttpTransport(
        "https://example.test/rows",
        params={"symbol": InvalidParameter()},
    )

    with pytest.raises(RuntimeError, match="invalid parameter"):
        list(transport.resources())

    assert not opened


def test_data_loader_tracks_current_resource_uri(tmp_path):
    appl = tmp_path / "APPL.jsonl"
    msft = tmp_path / "MSFT.jsonl"
    appl.write_text('{"symbol":"AAPL"}\n', encoding="utf-8")
    msft.write_text('{"symbol":"MSFT"}\n', encoding="utf-8")

    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )

    rows = loader.load()

    assert next(rows) == {"symbol": "AAPL"}
    assert loader.current_resource_uri == str(appl)
    assert next(rows) == {"symbol": "MSFT"}
    assert loader.current_resource_uri == str(msft)
    assert list(rows) == []
    assert loader.current_resource_uri is None


def test_data_loader_closes_resource_after_partial_read() -> None:
    closed = False

    def chunks():
        nonlocal closed
        try:
            yield b'{"value": 1}\n'
            yield b'{"value": 2}\n'
        finally:
            closed = True

    class Transport(SourceTransport):
        def resources(self):
            yield SourceResource(uri="rows", stream=chunks())

    rows = DataLoader(Transport(), JsonLinesDecoder()).load()

    assert next(rows) == {"value": 1}
    rows.close()
    assert closed
