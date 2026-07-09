import heapq
import pickle
import tempfile
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")
_EMPTY = object()
_MAX_OPEN_RUNS = 64


def _sorted_batches(
    iterable: Iterable[T],
    batch_size: int,
    key: Callable[[T], object],
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
    key: Callable[[T], object],
    spill_dir: Path | None = None,
) -> Iterator[T]:
    """Stably sort records, spilling pickle runs when one batch is insufficient."""
    batches = _sorted_batches(iterable, batch_size, key)

    first_batch = next(batches, _EMPTY)
    if first_batch is _EMPTY:
        return

    second_batch = next(batches, _EMPTY)
    if second_batch is _EMPTY:
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

        for batch in batches:
            run_paths.append(_write_run(temp_dir, len(run_paths), batch))

        pass_id = 0
        while len(run_paths) > _MAX_OPEN_RUNS:
            pass_id += 1
            run_paths = _merge_pass(temp_dir, pass_id, run_paths, key)

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

    try:
        # Each run is a contiguous input slice, so its index is the stable tie-breaker.
        for run_index, path in enumerate(run_paths):
            reader = _read_run(path)
            readers.append(reader)
            first = next(reader, _EMPTY)
            if first is _EMPTY:
                continue
            heapq.heappush(heap, (key(first), run_index, first, reader))

        while heap:
            _, run_index, item, reader = heapq.heappop(heap)
            yield item
            next_item = next(reader, _EMPTY)
            if next_item is not _EMPTY:
                heapq.heappush(heap, (key(next_item), run_index, next_item, reader))
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
