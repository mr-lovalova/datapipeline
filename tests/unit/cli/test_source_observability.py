from dataclasses import dataclass
from pathlib import Path

import pytest

from datapipeline.cli.visuals.execution_context import (
    set_current_dag_depth,
)
from datapipeline.cli.visuals.common import compute_glob_root, relative_label
from datapipeline.cli.visuals.source_observability import SourceObservabilityAdapter
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader
from datapipeline.sources.adapters.fs import FsFileTransport
from datapipeline.sources.adapters.fs import FsGlobTransport


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


class _ManualLoader:
    def __init__(self):
        self._spec = type("Spec", (), {"inputs": ("a=left", "b=right")})()
        self.transport = None

    def count(self):
        return None

    def progress_visible(self):
        return False


class _ManualSourceWithLoader:
    def __init__(self):
        self.loader = _ManualLoader()


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



def test_source_observability_adapter_includes_input_details_in_info_lines():
    adapter = SourceObservabilityAdapter(_ManualSourceWithLoader(), "combined")

    assert adapter.info_lines() == [
        "Inputs: a=left, b=right",
    ]


def test_source_observability_adapter_uses_loader_progress_visibility():
    adapter = SourceObservabilityAdapter(_ManualSourceWithLoader(), "combined")

    assert adapter.progress_visible() is False


def test_source_observability_adapter_formats_with_current_dag_indent():
    set_current_dag_depth(2)
    adapter = SourceObservabilityAdapter(_SourceWithLoader(), "demo")
    assert adapter.current_indent() == "    "
    assert adapter.format_label() == "    [demo] _DummyLoader"


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


def test_compute_glob_root_returns_none_for_mixed_path_styles() -> None:
    assert compute_glob_root(["/tmp/APPL.jsonl", "relative/MSFT.jsonl"]) is None


def test_relative_label_falls_back_to_name_outside_root(tmp_path) -> None:
    assert (
        relative_label(str(tmp_path / "outside.csv"), Path("/other/root"))
        == "outside.csv"
    )
