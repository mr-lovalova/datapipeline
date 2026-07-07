import logging
from types import SimpleNamespace

from datapipeline.cli.visuals.streams import observe_source
from datapipeline.cli.visuals.streams_basic import visual_sources
from datapipeline.cli.visuals.streams_basic import VisualSourceProxy
from datapipeline.sources.adapters.fs import FsFileTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader


class _DummyGenerator(DataGenerator):
    def generate(self):
        for idx in range(2):
            yield {"id": idx}

    def count(self):
        return 2

    def info_lines(self):
        return [
            "synthetic.generate: start=2024-01-01T00:00:00Z end=2024-01-01T01:00:00Z freq=30m"
        ]


class _SyntheticSource:
    def __init__(self) -> None:
        self.loader = SyntheticLoader(_DummyGenerator())

    def stream(self):
        yield from self.loader.load()


class _LoaderSource:
    def __init__(self, loader) -> None:
        self.loader = loader

    def stream(self):
        yield from self.loader.load()


class _DerivedSource:
    def stream(self):
        yield from ()


class _StreamRegistry:
    def __init__(self) -> None:
        self._items: dict[str, object] = {}

    def items(self):
        return self._items.items()

    def register(self, stream_id: str, stream_source: object) -> None:
        self._items[stream_id] = stream_source


def test_visual_source_proxy_logs_source_details_without_completion(caplog):
    proxy = VisualSourceProxy(_SyntheticSource(), "time.ticks.linear")

    with caplog.at_level(
        logging.INFO, logger="datapipeline.cli.visuals.streams_basic"
    ):
        rows = list(proxy.stream())

    assert rows == [{"id": 0}, {"id": 1}]
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "synthetic.generate: start=2024-01-01T00:00:00Z" in msg
        for msg in messages
    )
    assert all("stream complete items=" not in msg for msg in messages)
    assert all("✔" not in msg for msg in messages)


def test_visual_source_proxy_does_not_invent_file_source_details(
    tmp_path, caplog
) -> None:
    path = tmp_path / "prices.jsonl"
    path.write_text('{"id":1}\n{"id":2}\n', encoding="utf-8")
    source = _LoaderSource(
        DataLoader(
            FsFileTransport(str(path)),
            JsonLinesDecoder(),
        )
    )
    proxy = VisualSourceProxy(source, "equity.price")

    with caplog.at_level(
        logging.INFO, logger="datapipeline.cli.visuals.streams_basic"
    ):
        rows = list(proxy.stream())

    assert rows == [{"id": 1}, {"id": 2}]
    messages = [record.getMessage() for record in caplog.records]
    assert messages == []


def test_basic_visual_sources_observe_dynamic_sources() -> None:
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )

    source = _SyntheticSource()
    with visual_sources(runtime, logging.INFO):
        observed = observe_source(source, "time.ticks.linear")

    assert isinstance(observed, VisualSourceProxy)
    assert observed.loader is source.loader


def test_basic_visual_sources_leave_derived_sources_unwrapped() -> None:
    source = _DerivedSource()
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )
    runtime.registries.stream_sources.register("derived", source)

    with visual_sources(runtime, logging.INFO):
        wrapped = runtime.registries.stream_sources._items["derived"]
        observed = observe_source(source, "derived")

    assert wrapped is source
    assert observed is source
