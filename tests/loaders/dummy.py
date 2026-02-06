from typing import Iterator, Any

from datapipeline.sources.models.loader import BaseDataLoader


class DummyLoader(BaseDataLoader):
    def __init__(self, *, value: Any, rows: int = 1):
        self._value = value
        self._rows = int(rows)

    def load(self) -> Iterator[Any]:
        for i in range(self._rows):
            yield {"value": self._value, "idx": i}
