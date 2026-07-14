import heapq
import pickle
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any, TypeVar

from datapipeline.execution.events import ProgressSnapshot
from datapipeline.execution.runner import report_node_progress
from datapipeline.services.temp_cleanup import sort_spill_directory

T = TypeVar("T")
_BufferedItem = tuple[Any, bytes]
_MAX_OPEN_RUNS = 64
_MERGE_PROGRESS_INTERVAL = 100_000


def _sorted_runs(
    iterable: Iterable[T],
    buffer_bytes: int,
    key: Callable[[T], Any],
) -> Iterator[tuple[list[_BufferedItem], bool]]:
    batch: list[_BufferedItem] = []
    batch_bytes = 0
    for item in iterable:
        sort_key = key(item)
        try:
            payload = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
        except (pickle.PickleError, TypeError, AttributeError) as exc:
            raise TypeError("batch_sort requires pickle-serializable items") from exc
        payload_bytes = len(payload)
        if batch and batch_bytes + payload_bytes > buffer_bytes:
            batch.sort(key=lambda entry: entry[0])
            yield batch, False
            batch = []
            batch_bytes = 0
        batch.append((sort_key, payload))
        batch_bytes += payload_bytes
    if batch:
        batch.sort(key=lambda entry: entry[0])
        yield batch, True


def batch_sort(
    iterable: Iterable[T],
    buffer_bytes: int,
    key: Callable[[T], Any],
    spill_dir: Path | None = None,
) -> Generator[T, None, None]:
    """Stably sort serialized values, spilling runs when the buffer is exceeded."""
    if buffer_bytes < 1:
        raise ValueError("buffer_bytes must be at least 1")
    report_node_progress(ProgressSnapshot(completed=0, phase="reading"))
    runs = _sorted_runs(iterable, buffer_bytes, key)

    try:
        first_run, final = next(runs)
    except StopIteration:
        return
    report_node_progress(ProgressSnapshot(completed=len(first_run), phase="reading"))

    if final:
        report_node_progress(
            ProgressSnapshot(
                completed=0,
                total=len(first_run),
                phase="emitting",
            )
        )
        for _, payload in first_run:
            yield pickle.loads(payload)
        return

    with sort_spill_directory(spill_dir) as temp_dir:
        run_paths = [_write_serialized_run(temp_dir, 0, first_run)]
        input_items = len(first_run)
        first_run.clear()
        report_node_progress(
            ProgressSnapshot(
                completed=input_items,
                phase="spilling",
                detail=f"{len(run_paths)} spill runs",
            )
        )

        for run, _ in runs:
            run_paths.append(_write_serialized_run(temp_dir, len(run_paths), run))
            input_items += len(run)
            run.clear()
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


def _write_serialized_run(
    temp_dir: Path,
    run_id: int,
    items: Iterable[_BufferedItem],
) -> Path:
    path = temp_dir / f"run-{run_id}.pickle"
    with path.open("wb") as fh:
        for _, payload in items:
            fh.write(payload)
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
