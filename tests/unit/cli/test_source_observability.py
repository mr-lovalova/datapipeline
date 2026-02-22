from dataclasses import dataclass
import logging

import pytest

from datapipeline.cli.visuals.execution_context import (
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_visual_log_level,
)
from datapipeline.cli.visuals.source_observability import SourceObservabilityAdapter
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader
from datapipeline.sources.transports import FsFileTransport
from datapipeline.sources.transports import FsGlobTransport


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


class _SourceWithGlobLoader:
    def __init__(self):
        self.loader = _DummyLoader(
            transport=FsGlobTransport(
                "/definitely/not/real/*.jsonl",
            )
        )
        self.loader.transport._files = [  # type: ignore[attr-defined]
            "/tmp/APPL.jsonl",
            "/tmp/MSFT.jsonl",
        ]


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


class _SourceWithForeachFsLoader:
    def __init__(self):
        self.loader = ForeachLoader(
            foreach={"path": ["/tmp/APPL.jsonl", "/tmp/MSFT.jsonl"]},
            loader={
                "entrypoint": "core.io",
                "args": {"transport": "fs", "path": "${path}"},
            },
        )



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

    assert captured == [("combined", ["a=left", "b=right"], "    ")]



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
    assert adapter.current_indent() == "    "
    assert adapter.current_indent(logging.DEBUG) == "    "
    assert adapter.format_label() == "    [demo] _DummyLoader"
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


def test_source_observability_adapter_initial_label_uses_first_glob_file():
    adapter = SourceObservabilityAdapter(_SourceWithGlobLoader(), "equity.ohlcv")
    assert adapter.initial_label() == '"APPL.jsonl"'


def test_source_observability_adapter_foreach_fs_emits_glob_summary_line():
    adapter = SourceObservabilityAdapter(_SourceWithForeachFsLoader(), "equity.ohlcv")
    assert adapter.info_lines() == [
        "fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)"
    ]


def test_source_observability_adapter_foreach_initial_label_uses_first_file():
    adapter = SourceObservabilityAdapter(_SourceWithForeachFsLoader(), "equity.ohlcv")
    assert adapter.initial_label() == 'Loading "APPL.jsonl"'


def test_source_observability_adapter_foreach_progress_sequence_tracks_each_file():
    adapter = SourceObservabilityAdapter(_SourceWithForeachFsLoader(), "equity.ohlcv")
    sequence = adapter.progress_sequence()
    assert sequence is not None
    assert [entry.label for entry in sequence] == [
        'Loading "APPL.jsonl"',
        'Loading "MSFT.jsonl"',
    ]


def test_source_observability_adapter_foreach_progress_sequence_counts_each_file(tmp_path):
    appl = tmp_path / "APPL.jsonl"
    msft = tmp_path / "MSFT.jsonl"
    appl.write_text('{"n":1}\n{"n":2}\n', encoding="utf-8")
    msft.write_text('{"n":3}\n', encoding="utf-8")
    loader = ForeachLoader(
        foreach={"path": [str(appl), str(msft)]},
        loader={
            "entrypoint": "core.io",
            "args": {
                "transport": "fs",
                "format": "jsonl",
                "path": "${path}",
            },
        },
    )
    source = type("S", (), {"loader": loader})()
    adapter = SourceObservabilityAdapter(source, "equity.ohlcv")
    sequence = adapter.progress_sequence()
    assert sequence is not None
    assert [entry.total for entry in sequence] == [2, 1]


def test_source_observability_adapter_glob_progress_sequence_tracks_each_file(tmp_path):
    appl = tmp_path / "APPL.jsonl"
    msft = tmp_path / "MSFT.jsonl"
    appl.write_text('{"n":1}\n{"n":2}\n', encoding="utf-8")
    msft.write_text('{"n":3}\n', encoding="utf-8")
    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )
    source = type("S", (), {"loader": loader})()
    adapter = SourceObservabilityAdapter(source, "equity.ohlcv")

    sequence = adapter.progress_sequence()
    assert sequence is not None
    assert [entry.label for entry in sequence] == ['"APPL.jsonl"', '"MSFT.jsonl"']
    assert [entry.total for entry in sequence] == [2, 1]
