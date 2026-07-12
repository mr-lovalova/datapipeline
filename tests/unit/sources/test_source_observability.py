"""Source progress metadata and resource tracking."""

from dataclasses import dataclass

from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.observability import (
    describe_loader,
    loader_current_label,
    loader_current_resource_id,
    loader_progress_sequence,
    source_summary,
)


@dataclass
class _Loader:
    transport: object


class _Source:
    def __init__(self, transport: object) -> None:
        self.loader = _Loader(transport)


def test_source_summary_describes_http_transport() -> None:
    source = _Source(HttpTransport("https://example.test/data/demo.jsonl"))

    assert (
        source_summary(source)
        == "transport=http.fetch host=example.test resource=demo.jsonl"
    )
    description = describe_loader(source.loader)
    assert loader_current_label(source.loader, description) == "@example.test"


def test_glob_progress_tracks_current_file_and_resource_sequence(tmp_path) -> None:
    (tmp_path / "APPL.jsonl").write_text('{"n":1}\n', encoding="utf-8")
    (tmp_path / "MSFT.jsonl").write_text('{"n":2}\n', encoding="utf-8")
    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )
    description = describe_loader(loader)

    sequence = loader_progress_sequence(loader, description)
    assert sequence is not None
    assert [entry.label for entry in sequence] == ['"APPL.jsonl"', '"MSFT.jsonl"']
    assert [entry.source_resource_id for entry in sequence] == [
        str(tmp_path / "APPL.jsonl"),
        str(tmp_path / "MSFT.jsonl"),
    ]

    loader._current_resource_uri = str(tmp_path / "MSFT.jsonl")
    assert loader_current_label(loader, description) == '"MSFT.jsonl"'
    assert loader_current_resource_id(loader) == str(tmp_path / "MSFT.jsonl")


def test_single_file_glob_uses_the_file_name_in_progress(tmp_path) -> None:
    path = tmp_path / "only.jsonl"
    path.write_text('{"n":1}\n', encoding="utf-8")
    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )

    sequence = loader_progress_sequence(loader)

    assert sequence is not None
    assert [entry.label for entry in sequence] == ['"only.jsonl"']


def test_file_loader_current_label_uses_file_name() -> None:
    loader = _Loader(FsFileTransport("/tmp/demo.csv"))

    assert loader_current_label(loader, describe_loader(loader)) == '"demo.csv"'
