from collections.abc import Iterator
from typing import Any

import datapipeline.pipelines.shared.record_nodes as record_nodes
from datapipeline.pipelines.shared.record_nodes import open_records
from datapipeline.sources.adapters.fs import FsGlobTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder


class _LoaderStream:
    def __init__(self, loader: DataLoader) -> None:
        self.loader = loader

    def stream(self) -> Iterator[Any]:
        return self.loader.load()


class _PlainStream:
    def stream(self) -> Iterator[int]:
        yield 1
        yield 2


def test_open_records_reports_each_source_resource_without_counting(
    tmp_path,
    monkeypatch,
) -> None:
    for index in range(3):
        (tmp_path / f"part-{index}.jsonl").write_text(
            f'{{"part": {index}}}\n',
            encoding="utf-8",
        )
    loader = DataLoader(
        transport=FsGlobTransport(str(tmp_path / "*.jsonl")),
        decoder=JsonLinesDecoder(),
    )

    def fail_count() -> None:
        raise AssertionError("count must not be called")

    monkeypatch.setattr(loader, "count", fail_count)
    snapshots = []
    monkeypatch.setattr(record_nodes, "report_node_progress", snapshots.append)

    records = list(open_records(_LoaderStream(loader)))

    assert records == [{"part": 0}, {"part": 1}, {"part": 2}]
    resources = [
        snapshot.resource for snapshot in snapshots if snapshot.resource is not None
    ]
    assert [
        (resource.index, resource.total, resource.label) for resource in resources
    ] == [
        (1, 3, '"part-0.jsonl"'),
        (2, 3, '"part-1.jsonl"'),
        (3, 3, '"part-2.jsonl"'),
    ]
    assert [snapshot.completed for snapshot in snapshots] == [0, 1, 2]
    assert all(snapshot.phase == "streaming from" for snapshot in snapshots)
    assert all(snapshot.unit == "items" for snapshot in snapshots)


def test_open_records_leaves_non_loader_streams_on_automatic_progress(
    monkeypatch,
) -> None:
    snapshots = []
    monkeypatch.setattr(record_nodes, "report_node_progress", snapshots.append)

    assert list(open_records(_PlainStream())) == [1, 2]
    assert snapshots == []
