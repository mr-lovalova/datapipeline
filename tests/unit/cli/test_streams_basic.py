import logging
from types import SimpleNamespace

from datapipeline.cli.visuals.streams import observe_source
from datapipeline.cli.visuals.streams_basic import visual_sources
from datapipeline.cli.visuals.streams_basic import VisualSourceProxy
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


def test_visual_source_proxy_logs_ascii_stream_completion(caplog):
    proxy = VisualSourceProxy(_SyntheticSource(), "time.ticks.linear")

    with caplog.at_level(
        logging.INFO, logger="datapipeline.cli.visuals.streams_basic"
    ):
        list(proxy.stream())

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Stream complete items=2" in msg
        for msg in messages
    )
    assert all("✔" not in msg for msg in messages)


def test_basic_visual_sources_observe_dynamic_sources() -> None:
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )

    with visual_sources(runtime, logging.INFO):
        observed = observe_source(_SyntheticSource(), "time.ticks.linear")

    assert isinstance(observed, VisualSourceProxy)


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
