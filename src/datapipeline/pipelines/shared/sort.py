import heapq
import pickle
import tempfile
import time
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager
from itertools import count
from pathlib import Path
from typing import TypeVar

from datapipeline.dag.runner import emit_node_progress

T = TypeVar("T")
_EMPTY = object()
_MAX_OPEN_RUNS = 64
_PROGRESS_INTERVAL_SECONDS = 60.0
_PROGRESS_ITEM_INTERVAL = 10_000
SpillDir = Path | Callable[[], Path] | None


class _SortProgress:
    def __init__(self, stage: str, start_time: float) -> None:
        self.stage = stage
        self.records_read = 0
        self.batches_read = 0
        self.runs_written = 0
        self.records_emitted = 0
        self._next_log_at = start_time + _PROGRESS_INTERVAL_SECONDS

    def batch_written(self, batch_size: int) -> None:
        self.batches_read += 1
        self.records_read += batch_size
        self.runs_written += 1
        self._emit_if_due(
            (
                f"{self.stage} read batches={self.batches_read} "
                f"records={self.records_read} spill_runs={self.runs_written}"
            )
        )

    def merge_pass_started(self, pass_id: int, run_count: int) -> None:
        emit_node_progress(
            f"{self.stage} merge pass={pass_id} status=started runs={run_count}"
        )

    def merge_pass_finished(self, pass_id: int, run_count: int) -> None:
        emit_node_progress(
            f"{self.stage} merge pass={pass_id} status=finished runs={run_count}"
        )

    def record_emitted(self) -> None:
        self.records_emitted += 1
        if self.records_emitted % _PROGRESS_ITEM_INTERVAL:
            return
        self._emit_if_due(
            f"{self.stage} emit records={self.records_emitted}"
        )

    def _emit_if_due(self, message: str) -> None:
        now = time.perf_counter()
        if now < self._next_log_at:
            return
        emit_node_progress(message)
        self._next_log_at = now + _PROGRESS_INTERVAL_SECONDS


def read_batches(
    iterable: Iterable[T],
    batch_size: int,
    key: Callable[[T], object],
) -> Iterator[list[T]]:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield sorted(batch, key=key)
            batch = []
    if batch:
        yield sorted(batch, key=key)


def batch_sort(
    iterable: Iterable[T],
    batch_size: int,
    key: Callable[[T], object],
    spill_dir: SpillDir = None,
    progress_stage: str = "sort",
) -> Iterator[T]:
    """Sort an iterable by chunking, spilling runs to disk, then merging."""
    start_time = time.perf_counter()
    batches = read_batches(iterable, batch_size, key)

    first_batch = next(batches, _EMPTY)
    if first_batch is _EMPTY:
        _emit_sort_summary(
            progress_stage,
            records=0,
            batch_size=batch_size,
            spilled=False,
            spill_runs=0,
            merge_passes=0,
            start_time=start_time,
        )
        return

    second_batch = next(batches, _EMPTY)
    if second_batch is _EMPTY:
        yield from first_batch
        _emit_sort_summary(
            progress_stage,
            records=len(first_batch),
            batch_size=batch_size,
            spilled=False,
            spill_runs=0,
            merge_passes=0,
            start_time=start_time,
        )
        return

    with _temporary_directory(spill_dir) as tmp:
        temp_dir = Path(tmp)
        progress = _SortProgress(progress_stage, start_time)
        run_paths: list[Path] = []
        run_paths.append(_write_run(temp_dir, 0, first_batch))
        progress.batch_written(len(first_batch))
        run_paths.append(_write_run(temp_dir, 1, second_batch))
        progress.batch_written(len(second_batch))

        for batch in batches:
            run_paths.append(_write_run(temp_dir, len(run_paths), batch))
            progress.batch_written(len(batch))

        pass_id = 0
        while len(run_paths) > _MAX_OPEN_RUNS:
            pass_id += 1
            progress.merge_pass_started(pass_id, len(run_paths))
            run_paths = _merge_pass(temp_dir, pass_id, run_paths, key)
            progress.merge_pass_finished(pass_id, len(run_paths))

        for item in _merge_runs(run_paths, key):
            progress.record_emitted()
            yield item

        _emit_sort_summary(
            progress_stage,
            records=progress.records_read,
            batch_size=batch_size,
            spilled=True,
            spill_runs=progress.runs_written,
            merge_passes=pass_id,
            start_time=start_time,
        )


def _emit_sort_summary(
    stage: str,
    *,
    records: int,
    batch_size: int,
    spilled: bool,
    spill_runs: int,
    merge_passes: int,
    start_time: float,
) -> None:
    elapsed = time.perf_counter() - start_time
    if spilled:
        emit_node_progress(
            (
                f"{stage} complete records={records} batch_size={batch_size} "
                f"spilled=true spill_runs={spill_runs} "
                f"merge_passes={merge_passes} elapsed={elapsed:.3f}s"
            )
        )
        return
    emit_node_progress(
        (
            f"{stage} complete records={records} batch_size={batch_size} "
            f"spilled=false elapsed={elapsed:.3f}s"
        )
    )


def _temporary_directory(spill_dir: SpillDir) -> AbstractContextManager[str]:
    if spill_dir is None:
        return tempfile.TemporaryDirectory(prefix="datapipeline-sort-")
    root = spill_dir() if callable(spill_dir) else Path(spill_dir)
    root.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix="datapipeline-sort-", dir=root)


def _write_run(temp_dir: Path, run_id: int, items: Iterable[T]) -> Path:
    path = temp_dir / f"run-{run_id}.pickle"
    try:
        with path.open("wb") as fh:
            for item in items:
                pickle.dump(item, fh, protocol=pickle.HIGHEST_PROTOCOL)
    except (pickle.PickleError, TypeError, AttributeError) as exc:
        raise TypeError(
            "batch_sort requires pickle-serializable records when input exceeds batch_size"
        ) from exc
    return path


def _merge_pass(
    temp_dir: Path,
    pass_id: int,
    run_paths: Sequence[Path],
    key: Callable[[T], object],
) -> list[Path]:
    merged_paths: list[Path] = []
    for start in range(0, len(run_paths), _MAX_OPEN_RUNS):
        group = run_paths[start : start + _MAX_OPEN_RUNS]
        merged_path = temp_dir / f"merged-{pass_id}-{len(merged_paths)}.pickle"
        _copy_run(_merge_runs(group, key), merged_path)
        merged_paths.append(merged_path)
        for path in group:
            path.unlink()
    return merged_paths


def _copy_run(items: Iterable[T], path: Path) -> None:
    with path.open("wb") as fh:
        for item in items:
            pickle.dump(item, fh, protocol=pickle.HIGHEST_PROTOCOL)


def _merge_runs(run_paths: Sequence[Path], key: Callable[[T], object]) -> Iterator[T]:
    heap: list[tuple[object, int, T, Iterator[T]]] = []
    readers: list[Generator[T, None, None]] = []
    seq = count()

    try:
        for path in run_paths:
            reader = _read_run(path)
            readers.append(reader)
            first = next(reader, _EMPTY)
            if first is _EMPTY:
                continue
            heapq.heappush(heap, (key(first), next(seq), first, reader))

        while heap:
            _, _, item, reader = heapq.heappop(heap)
            yield item
            next_item = next(reader, _EMPTY)
            if next_item is not _EMPTY:
                heapq.heappush(heap, (key(next_item), next(seq), next_item, reader))
    finally:
        for reader in readers:
            reader.close()


def _read_run(path: Path) -> Generator[T, None, None]:
    with path.open("rb") as fh:
        while True:
            try:
                yield pickle.load(fh)
            except EOFError:
                return
