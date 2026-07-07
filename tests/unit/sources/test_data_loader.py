from datapipeline.sources.adapters.fs import FsGlobTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder


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
