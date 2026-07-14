from urllib.parse import parse_qsl, urlparse

import pytest

import datapipeline.sources.adapters.http as http_adapter
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import Decoder, JsonLinesDecoder
from datapipeline.sources.factory import build_loader
from datapipeline.sources.ports import SourceResource, SourceTransport


def test_fs_path_selects_file_or_glob_transport(tmp_path) -> None:
    file_loader = build_loader("fs", "jsonl", path=str(tmp_path / "rows.jsonl"))
    glob_loader = build_loader("fs", "jsonl", path=str(tmp_path / "*.jsonl"))

    assert isinstance(file_loader.transport, FsFileTransport)
    assert isinstance(glob_loader.transport, FsGlobTransport)


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


def test_data_loader_count_propagates_decoder_errors() -> None:
    class BrokenDecoder(Decoder):
        def decode(self, chunks):
            yield from ()

        def count(self, chunks):
            raise ValueError("invalid payload")

    class Transport(SourceTransport):
        def resources(self):
            yield SourceResource(uri="broken", stream=iter((b"bad",)))

    loader = DataLoader(Transport(), BrokenDecoder())

    with pytest.raises(ValueError, match="invalid payload"):
        loader.count()


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
