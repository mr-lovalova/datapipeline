import heapq
import pickle
import tempfile
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any, TypeVar

from datapipeline.dag.events import ProgressSnapshot
from datapipeline.dag.runner import report_node_progress

T = TypeVar("T")
_MAX_OPEN_RUNS = 64
_MERGE_PROGRESS_INTERVAL = 100_000


def _sorted_batches(
    iterable: Iterable[T],
    batch_size: int,
    key: Callable[[T], Any],
) -> Iterator[list[T]]:
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    batch: list[T] = []
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
    key: Callable[[T], Any],
    spill_dir: Path | None = None,
) -> Iterator[T]:
    """Stably sort records, spilling pickle runs when one batch is insufficient."""
    report_node_progress(ProgressSnapshot(completed=0, phase="reading"))
    batches = _sorted_batches(iterable, batch_size, key)

    try:
        first_batch = next(batches)
    except StopIteration:
        return
    report_node_progress(ProgressSnapshot(completed=len(first_batch), phase="reading"))

    try:
        second_batch = next(batches)
    except StopIteration:
        report_node_progress(
            ProgressSnapshot(
                completed=0,
                total=len(first_batch),
                phase="emitting",
            )
        )
        yield from first_batch
        return

    if spill_dir is not None:
        spill_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="datapipeline-sort-",
        dir=spill_dir,
    ) as tmp:
        temp_dir = Path(tmp)
        run_paths = [
            _write_run(temp_dir, 0, first_batch),
            _write_run(temp_dir, 1, second_batch),
        ]
        input_items = len(first_batch) + len(second_batch)
        report_node_progress(
            ProgressSnapshot(
                completed=input_items,
                phase="spilling",
                detail=f"{len(run_paths)} spill runs",
            )
        )

        for batch in batches:
            run_paths.append(_write_run(temp_dir, len(run_paths), batch))
            input_items += len(batch)
            report_node_progress(
                ProgressSnapshot(
                    completed=input_items,
                    phase="spilling",
                    detail=f"{len(run_paths)} spill runs",
                )
            )

        pass_id = 0
        while len(run_paths) > _MAX_OPEN_RUNS:
            pass_id += 1
            run_paths = _merge_pass(
                temp_dir,
                pass_id,
                run_paths,
                key,
                input_items,
            )

        report_node_progress(
            ProgressSnapshot(
                completed=0,
                total=input_items,
                phase="emitting",
            )
        )
        yield from _merge_runs(run_paths, key)


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
    key: Callable[[T], Any],
    input_items: int,
) -> list[Path]:
    merged_paths: list[Path] = []
    merged_items = 0
    pending_progress = 0
    report_node_progress(
        ProgressSnapshot(
            completed=0,
            total=input_items,
            phase="merging",
            detail=f"pass {pass_id}",
        )
    )
    for start in range(0, len(run_paths), _MAX_OPEN_RUNS):
        group = run_paths[start : start + _MAX_OPEN_RUNS]
        merged_path = temp_dir / f"merged-{pass_id}-{len(merged_paths)}.pickle"
        with merged_path.open("wb") as fh:
            for item in _merge_runs(group, key):
                pickle.dump(item, fh, protocol=pickle.HIGHEST_PROTOCOL)
                merged_items += 1
                pending_progress += 1
                if pending_progress == _MERGE_PROGRESS_INTERVAL:
                    report_node_progress(
                        ProgressSnapshot(
                            completed=merged_items,
                            total=input_items,
                            phase="merging",
                            detail=f"pass {pass_id}",
                        )
                    )
                    pending_progress = 0
        merged_paths.append(merged_path)
        for path in group:
            path.unlink()
    if pending_progress:
        report_node_progress(
            ProgressSnapshot(
                completed=merged_items,
                total=input_items,
                phase="merging",
                detail=f"pass {pass_id}",
            )
        )
    return merged_paths


def _merge_runs(run_paths: Sequence[Path], key: Callable[[T], Any]) -> Iterator[T]:
    heap: list[tuple[Any, int, T, Iterator[T]]] = []
    readers: list[Generator[T, None, None]] = []

    try:
        # Each run is a contiguous input slice, so its index is the stable tie-breaker.
        for run_index, path in enumerate(run_paths):
            reader: Generator[T, None, None] = _read_run(path)
            readers.append(reader)
            try:
                first = next(reader)
            except StopIteration:
                continue
            heapq.heappush(heap, (key(first), run_index, first, reader))

        while heap:
            _, run_index, item, heap_reader = heapq.heappop(heap)
            yield item
            try:
                next_item = next(heap_reader)
            except StopIteration:
                continue
            heapq.heappush(
                heap,
                (key(next_item), run_index, next_item, heap_reader),
            )
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
