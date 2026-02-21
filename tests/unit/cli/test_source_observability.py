from dataclasses import dataclass
import logging

import pytest

from datapipeline.cli.visuals.execution_context import (
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_visual_log_level,
)
from datapipeline.cli.visuals.source_observability import SourceObservabilityAdapter
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader
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


class _DummyGenerator(DataGenerator):
    def generate(self):
        yield from ()

    def info_lines(self) -> list[str]:
        return ["synthetic.generate: _DummyGenerator"]


class _SyntheticSourceWithLoader:
    def __init__(self):
        self.loader = SyntheticLoader(_DummyGenerator())


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
    set_current_dag_depth(0)
    adapter = SourceObservabilityAdapter(_SourceWithLoader(), "demo")

    assert adapter.count() == 7
    assert adapter.format_label() == "[demo] _DummyLoader"
    assert adapter.current_label() == '"demo.csv"'
    assert adapter.info_lines() == ["fs.file: demo.csv"]



def test_source_observability_adapter_tolerates_count_failures():
    adapter = SourceObservabilityAdapter(_SourceWithFailingLoaderCount(), "demo")
    assert adapter.count() is None



def test_source_observability_adapter_requires_loader():
    with pytest.raises(TypeError, match="must expose a loader"):
        SourceObservabilityAdapter(_SourceWithoutLoader(), "missing")


def test_source_observability_adapter_includes_synthetic_info_line():
    adapter = SourceObservabilityAdapter(_SyntheticSourceWithLoader(), "ticks")
    assert adapter.info_lines() == ["synthetic.generate: _DummyGenerator"]



def test_source_observability_adapter_logs_composed_details(monkeypatch):
    captured: list[tuple[str, list[str] | None, str]] = []

    def _capture(stream_id, details, indent=""):
        captured.append((stream_id, details, indent))

    monkeypatch.setattr(
        "datapipeline.cli.visuals.source_observability.log_combined_stream",
        _capture,
    )

    set_current_dag_depth(2)
    adapter = SourceObservabilityAdapter(_ComposedSourceWithLoader(), "combined")
    adapter.log_composed_details()

    assert captured == [("combined", ["a=left", "b=right"], "  ")]



def test_source_observability_adapter_skips_composed_details_for_non_composed(monkeypatch):
    captured: list[tuple[str, list[str] | None, str]] = []

    def _capture(stream_id, details, indent=""):
        captured.append((stream_id, details, indent))

    monkeypatch.setattr(
        "datapipeline.cli.visuals.source_observability.log_combined_stream",
        _capture,
    )

    adapter = SourceObservabilityAdapter(_SourceWithLoader(), "demo")
    adapter.log_composed_details()

    assert captured == []


def test_source_observability_adapter_formats_with_current_dag_indent():
    set_current_dag_depth(2)
    adapter = SourceObservabilityAdapter(_SourceWithLoader(), "demo")
    assert adapter.current_indent() == "  "
    assert adapter.current_indent(logging.DEBUG) == "    "
    assert adapter.format_label() == "  [demo] _DummyLoader"
    assert adapter.format_label(level=logging.DEBUG) == "    [demo] _DummyLoader"


def test_source_observability_adapter_uses_full_depth_in_debug_session():
    set_current_dag_depth(2)
    token = set_current_visual_log_level(logging.DEBUG)
    try:
        adapter = SourceObservabilityAdapter(_SourceWithLoader(), "demo")
        assert adapter.current_indent(logging.INFO) == "    "
        assert adapter.current_indent(logging.DEBUG) == "    "
    finally:
        reset_current_visual_log_level(token)
