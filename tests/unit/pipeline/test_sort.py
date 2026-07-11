from dataclasses import dataclass

import pytest

import datapipeline.pipelines.shared.sort as sort_module
from datapipeline.pipelines.shared.sort import batch_sort


@dataclass
class SortItem:
    value: int


@dataclass
class StableSortItem:
    value: int
    position: int


class UnpickleableSortItem:
    def __init__(self, value: int) -> None:
        self.value = value
        self.callback = lambda: value


def test_batch_sort_orders_records_across_batches() -> None:
    records = [SortItem(3), SortItem(1), SortItem(4), SortItem(2)]

    ordered = list(batch_sort(records, batch_size=2, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2, 3, 4]


def test_batch_sort_preserves_input_order_for_equal_keys_across_batches() -> None:
    records = [
        StableSortItem(value=0, position=0),
        StableSortItem(value=1, position=1),
        StableSortItem(value=1, position=2),
    ]

    ordered = list(batch_sort(records, batch_size=2, key=lambda item: item.value))

    assert [item.position for item in ordered] == [0, 1, 2]


def test_batch_sort_orders_records_across_merge_passes(monkeypatch) -> None:
    monkeypatch.setattr(sort_module, "_MAX_OPEN_RUNS", 2)
    records = [SortItem(5), SortItem(1), SortItem(4), SortItem(2), SortItem(3)]

    ordered = list(batch_sort(records, batch_size=1, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2, 3, 4, 5]


def test_batch_sort_reports_buffered_and_merged_work(monkeypatch) -> None:
    monkeypatch.setattr(sort_module, "_MAX_OPEN_RUNS", 2)
    monkeypatch.setattr(sort_module, "_MERGE_PROGRESS_INTERVAL", 1)
    snapshots = []
    monkeypatch.setattr(sort_module, "report_node_progress", snapshots.append)

    ordered = list(
        batch_sort(
            [SortItem(3), SortItem(1), SortItem(2)],
            batch_size=1,
            key=lambda item: item.value,
        )
    )

    assert [item.value for item in ordered] == [1, 2, 3]
    assert any(
        snapshot.phase == "spilling"
        and snapshot.completed == 3
        and snapshot.detail == "3 spill runs"
        for snapshot in snapshots
    )
    assert any(
        snapshot.phase == "merging"
        and snapshot.completed == 3
        and snapshot.total == 3
        and snapshot.detail == "pass 1"
        for snapshot in snapshots
    )
    assert snapshots[-1].phase == "emitting"
    assert snapshots[-1].completed == 0
    assert snapshots[-1].total == 3


def test_batch_sort_preserves_input_order_for_equal_keys_across_merge_passes(
    monkeypatch,
) -> None:
    monkeypatch.setattr(sort_module, "_MAX_OPEN_RUNS", 2)
    records = [StableSortItem(value=1, position=index) for index in range(5)]

    ordered = list(batch_sort(records, batch_size=1, key=lambda item: item.value))

    assert [item.position for item in ordered] == list(range(5))


def test_batch_sort_keeps_single_batch_in_memory() -> None:
    records = [UnpickleableSortItem(2), UnpickleableSortItem(1)]

    ordered = list(batch_sort(records, batch_size=10, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2]


def test_batch_sort_does_not_create_spill_dir_for_single_batch(tmp_path) -> None:
    records = [SortItem(2), SortItem(1)]
    spill_dir = tmp_path / "sort"

    ordered = list(
        batch_sort(
            records,
            batch_size=10,
            key=lambda item: item.value,
            spill_dir=spill_dir,
        )
    )

    assert [item.value for item in ordered] == [1, 2]
    assert not spill_dir.exists()


def test_batch_sort_uses_configured_spill_parent(tmp_path) -> None:
    spill_parent = tmp_path / "sort"
    records = [SortItem(3), SortItem(1), SortItem(2)]

    ordered = list(
        batch_sort(
            records,
            batch_size=1,
            key=lambda item: item.value,
            spill_dir=spill_parent,
        )
    )

    assert [item.value for item in ordered] == [1, 2, 3]
    assert spill_parent.exists()
    assert list(spill_parent.iterdir()) == []


def test_batch_sort_requires_pickleable_records_when_spilling() -> None:
    records = [UnpickleableSortItem(2), UnpickleableSortItem(1)]

    with pytest.raises(TypeError, match="pickle-serializable"):
        list(batch_sort(records, batch_size=1, key=lambda item: item.value))


def test_batch_sort_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        list(batch_sort([SortItem(1)], batch_size=0, key=lambda item: item.value))
