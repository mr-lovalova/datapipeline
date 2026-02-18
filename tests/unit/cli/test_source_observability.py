from dataclasses import dataclass

import pytest

from datapipeline.cli.visuals.source_observability import SourceObservabilityAdapter
from datapipeline.sources.transports import FsFileTransport


@dataclass
class _DummyLoader:
    transport: FsFileTransport

    def count(self):
        return 7


@dataclass
class _FailingCountLoader:
    transport: FsFileTransport

    def count(self):
        raise RuntimeError("boom")


class _SourceWithLoader:
    def __init__(self):
        self.loader = _DummyLoader(transport=FsFileTransport("/tmp/demo.csv"))


class _SourceWithFailingLoaderCount:
    def __init__(self):
        self.loader = _FailingCountLoader(
            transport=FsFileTransport("/tmp/demo.csv"))


class _SourceWithoutLoader:
    pass


class _ComposedLoader:
    def __init__(self):
        self._spec = type("Spec", (), {"inputs": ("a=left", "b=right")})()
        self.transport = None

    def count(self):
        return None


class _ComposedSourceWithLoader:
    def __init__(self):
        self.loader = _ComposedLoader()



def test_source_observability_adapter_exposes_count_labels_and_details():
    adapter = SourceObservabilityAdapter(_SourceWithLoader(), "demo")

    assert adapter.count() == 7
    assert adapter.preparing_label() == "[demo] Preparing data stream"
    assert adapter.format_label() == "[demo] _DummyLoader"
    assert adapter.current_label() == '"demo.csv"'
    assert adapter.info_lines() == ["fs.file streaming demo.csv"]



def test_source_observability_adapter_tolerates_count_failures():
    adapter = SourceObservabilityAdapter(_SourceWithFailingLoaderCount(), "demo")
    assert adapter.count() is None



def test_source_observability_adapter_requires_loader():
    with pytest.raises(TypeError, match="must expose a loader"):
        SourceObservabilityAdapter(_SourceWithoutLoader(), "missing")



def test_source_observability_adapter_logs_composed_details(monkeypatch):
    captured: list[tuple[str, list[str] | None]] = []

    def _capture(alias, details):
        captured.append((alias, details))

    monkeypatch.setattr(
        "datapipeline.cli.visuals.source_observability.log_combined_stream",
        _capture,
    )

    adapter = SourceObservabilityAdapter(_ComposedSourceWithLoader(), "combined")
    adapter.log_composed_details()

    assert captured == [("combined", ["a=left", "b=right"])]
