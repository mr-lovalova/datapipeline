from typing import Iterable, Iterator, Callable, TypeVar
import heapq
from itertools import count


T = TypeVar("T")


def read_batches(iterable: Iterable[T], batch_size: int, key: Callable[[T], object]) -> Iterator[list[T]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield sorted(batch, key=key)
            batch = []
    if batch:
        yield sorted(batch, key=key)


def batch_sort(iterable: Iterable[T], batch_size: int, key: Callable[[T], object]) -> Iterator[T]:
    """Sort an iterable by chunking then merging to reduce peak memory usage."""
    batches = read_batches(iterable, batch_size, key)

    heap: list[tuple[object, int, T, Iterator[T]]] = []
    seq = count()

    for batch in batches:
        it = iter(batch)
        first = next(it, None)
        if first is None:
            continue
        heapq.heappush(heap, (key(first), next(seq), first, it))

    while heap:
        _, _, item, it = heapq.heappop(heap)
        yield item
        nxt = next(it, None)
        if nxt is not None:
            heapq.heappush(heap, (key(nxt), next(seq), nxt, it))
