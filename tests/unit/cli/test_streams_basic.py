import logging

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


def test_visual_source_proxy_logs_ascii_stream_completion(caplog):
    proxy = VisualSourceProxy(_SyntheticSource(), "time.ticks.linear")

    with caplog.at_level(
        logging.INFO, logger="datapipeline.cli.visuals.streams_basic"
    ):
        list(proxy.stream())

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "Stream complete items=2 unit=tick" in msg
        for msg in messages
    )
    assert all("✔" not in msg for msg in messages)
