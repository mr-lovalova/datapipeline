import pytest

from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.factory import build_loader


def test_fs_path_selects_file_or_glob_transport(tmp_path) -> None:
    file_loader = build_loader("fs", "jsonl", path=str(tmp_path / "rows.jsonl"))
    glob_loader = build_loader("fs", "jsonl", path=str(tmp_path / "*.jsonl"))

    assert isinstance(file_loader.transport, FsFileTransport)
    assert isinstance(glob_loader.transport, FsGlobTransport)


def test_fs_loader_rejects_removed_glob_option(tmp_path) -> None:
    with pytest.raises(ValueError, match="no longer accepts 'glob'"):
        build_loader(
            "fs",
            "jsonl",
            path=str(tmp_path / "*.jsonl"),
            glob=True,
        )


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
