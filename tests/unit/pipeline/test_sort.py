from dataclasses import dataclass

import pytest

import datapipeline.pipelines.shared.sort as sort_module
from datapipeline.pipelines.shared.sort import batch_sort


@dataclass
class SortItem:
    value: int


class UnpickleableSortItem:
    def __init__(self, value: int) -> None:
        self.value = value
        self.callback = lambda: value


def test_batch_sort_orders_records_across_batches() -> None:
    records = [SortItem(3), SortItem(1), SortItem(4), SortItem(2)]

    ordered = list(batch_sort(records, batch_size=2, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2, 3, 4]


def test_batch_sort_orders_records_across_merge_passes(monkeypatch) -> None:
    monkeypatch.setattr(sort_module, "_MAX_OPEN_RUNS", 2)
    records = [SortItem(5), SortItem(1), SortItem(4), SortItem(2), SortItem(3)]

    ordered = list(batch_sort(records, batch_size=1, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2, 3, 4, 5]


def test_batch_sort_keeps_single_batch_in_memory() -> None:
    records = [UnpickleableSortItem(2), UnpickleableSortItem(1)]

    ordered = list(batch_sort(records, batch_size=10, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2]


def test_batch_sort_does_not_create_spill_dir_for_single_batch(tmp_path) -> None:
    records = [SortItem(2), SortItem(1)]
    calls = {"count": 0}

    def spill_dir():
        calls["count"] += 1
        return tmp_path / "sort"

    ordered = list(
        batch_sort(
            records,
            batch_size=10,
            key=lambda item: item.value,
            spill_dir=spill_dir,
        )
    )

    assert [item.value for item in ordered] == [1, 2]
    assert calls["count"] == 0
    assert not (tmp_path / "sort").exists()


def test_batch_sort_emits_single_batch_summary(monkeypatch) -> None:
    timestamps = iter([10.0, 10.25])
    monkeypatch.setattr(sort_module.time, "perf_counter", lambda: next(timestamps))
    messages: list[str] = []
    monkeypatch.setattr(sort_module, "emit_node_progress", messages.append)
    records = [SortItem(2), SortItem(1)]

    ordered = list(
        batch_sort(
            records,
            batch_size=10,
            key=lambda item: item.value,
            progress_stage="Ordering test",
        )
    )

    assert [item.value for item in ordered] == [1, 2]
    assert messages == [
        "Ordering test complete records=2 batch_size=10 spilled=false elapsed=0.250s"
    ]


def test_batch_sort_emits_empty_summary(monkeypatch) -> None:
    timestamps = iter([10.0, 10.125])
    monkeypatch.setattr(sort_module.time, "perf_counter", lambda: next(timestamps))
    messages: list[str] = []
    monkeypatch.setattr(sort_module, "emit_node_progress", messages.append)

    ordered = list(
        batch_sort(
            [],
            batch_size=10,
            key=lambda item: item.value,
            progress_stage="Ordering empty",
        )
    )

    assert ordered == []
    assert messages == [
        "Ordering empty complete records=0 batch_size=10 spilled=false elapsed=0.125s"
    ]


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


def test_batch_sort_emits_spill_progress(monkeypatch) -> None:
    monkeypatch.setattr(sort_module, "_PROGRESS_ITEM_INTERVAL", 1)
    timestamps = iter([0.0, 61.0, 122.0, 183.0, 244.0, 305.0, 366.0, 427.0])
    monkeypatch.setattr(sort_module.time, "perf_counter", lambda: next(timestamps))
    messages: list[str] = []
    monkeypatch.setattr(sort_module, "emit_node_progress", messages.append)
    records = [SortItem(3), SortItem(1), SortItem(2)]

    ordered = list(batch_sort(records, batch_size=1, key=lambda item: item.value))

    assert [item.value for item in ordered] == [1, 2, 3]
    assert any("sort read batches=1 records=1" in msg for msg in messages)
    assert any("sort emit records=1" in msg for msg in messages)
    assert messages[-1] == (
        "sort complete records=3 batch_size=1 spilled=true "
        "spill_runs=3 merge_passes=0 elapsed=427.000s"
    )
