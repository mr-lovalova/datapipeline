from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import CsvDecoder, JsonLinesDecoder
from datapipeline.sources.models.source import Source
from datapipeline.sources.observability import (
    describe_loader,
    source_summary,
)
from datapipeline.sources.synthetic.time.loader import make_time_loader
from datapipeline.parsers.identity import IdentityParser


class _SourceLookalike:
    def __init__(self, loader: DataLoader) -> None:
        self.loader = loader

    def stream(self):
        return iter(())


def test_source_summary_describes_http_transport() -> None:
    loader = DataLoader(
        HttpTransport("https://example.test/data/demo.jsonl"),
        JsonLinesDecoder(),
    )
    source = Source(loader, IdentityParser())

    assert (
        source_summary(source)
        == "transport=http.fetch host=example.test resource=demo.jsonl"
    )
    description = describe_loader(loader)
    assert description.current_label == "@example.test"
    assert description.current_resource_id is None
    assert description.progress_sequence is None
    assert description.unit == "items"


def test_glob_progress_tracks_current_file_and_resource_sequence(tmp_path) -> None:
    (tmp_path / "APPL.jsonl").write_text('{"n":1}\n', encoding="utf-8")
    (tmp_path / "MSFT.jsonl").write_text('{"n":2}\n', encoding="utf-8")
    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )
    description = describe_loader(loader)

    sequence = description.progress_sequence
    assert sequence is not None
    assert [entry.label for entry in sequence] == ['"APPL.jsonl"', '"MSFT.jsonl"']
    assert [entry.source_resource_id for entry in sequence] == [
        str(tmp_path / "APPL.jsonl"),
        str(tmp_path / "MSFT.jsonl"),
    ]

    rows = loader.load()
    assert next(rows) == {"n": 1}
    assert description.current_label == '"APPL.jsonl"'
    assert description.current_resource_id == str(tmp_path / "APPL.jsonl")
    assert next(rows) == {"n": 2}
    assert description.current_label == '"MSFT.jsonl"'
    assert description.current_resource_id == str(tmp_path / "MSFT.jsonl")
    rows.close()


def test_single_file_glob_uses_the_file_name_in_progress(tmp_path) -> None:
    path = tmp_path / "only.jsonl"
    path.write_text('{"n":1}\n', encoding="utf-8")
    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )

    sequence = describe_loader(loader).progress_sequence

    assert sequence is not None
    assert [entry.label for entry in sequence] == ['"only.jsonl"']


def test_file_loader_uses_file_name_and_csv_rows() -> None:
    loader = DataLoader(FsFileTransport("/tmp/demo.csv"), CsvDecoder())
    description = describe_loader(loader)

    assert description.current_label == '"demo.csv"'
    assert description.unit == "rows"
    assert source_summary(Source(loader, IdentityParser())) == (
        "transport=fs.file file=demo.csv"
    )


def test_synthetic_loader_uses_tick_progress_without_a_resource() -> None:
    loader = make_time_loader(
        "2024-01-01T00:00:00Z",
        "2024-01-01T01:00:00Z",
        "1h",
    )
    description = describe_loader(loader)

    assert description.unit == "ticks"
    assert description.current_label is None
    assert description.current_resource_id is None
    assert description.progress_sequence is None
    assert source_summary(Source(loader, IdentityParser())) is None


def test_source_summary_does_not_infer_a_source_from_a_loader_attribute() -> None:
    loader = DataLoader(
        HttpTransport("https://example.test/data/demo.jsonl"),
        JsonLinesDecoder(),
    )

    assert source_summary(_SourceLookalike(loader)) is None
