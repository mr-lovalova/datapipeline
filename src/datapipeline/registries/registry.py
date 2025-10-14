from typing import Dict, Generic, Iterator, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class Registry(Generic[K, V]):

    def __init__(self) -> None:
        self._store: Dict[K, V] = {}

    def register(self, key: K, value: V) -> None:
        self._store[key] = value

    def get(self, key: K) -> V:
        return self._store[key]

    def clear(self) -> None:
        self._store.clear()

    def items(self) -> Iterator[tuple[K, V]]:
        return iter(self._store.items())

    def keys(self) -> Iterator[K]:
        return iter(self._store.keys())

    def values(self) -> Iterator[V]:
        return iter(self._store.values())
