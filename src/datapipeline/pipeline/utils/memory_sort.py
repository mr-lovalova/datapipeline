from typing import Iterable, Iterator, Callable, TypeVar
import heapq


def apply_pipeline(stream, stages):
    for stage in stages:
        stream = stage(stream)
    return stream


T = TypeVar("T")


def read_batches(iterable: Iterable[T], batch_size: int, key: Callable[[T], any]) -> Iterator[list[T]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield sorted(batch, key=key)
            batch = []
    if batch:
        yield sorted(batch, key=key)


def memory_sorted(iterable: Iterable[T], batch_size: int, key: Callable[[T], any]) -> Iterator[T]:
    sorted_batches = read_batches(iterable, batch_size, key)
    return heapq.merge(*sorted_batches, key=key)
