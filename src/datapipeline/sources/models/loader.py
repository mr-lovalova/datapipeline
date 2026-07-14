from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Literal

from .generator import DataGenerator


SourceProgressUnit = Literal["items", "records", "rows", "ticks"]


class BaseDataLoader(ABC):
    @abstractmethod
    def load(self) -> Iterator[Any]: ...

    @property
    def current_resource_uri(self) -> str | None:
        return None

    @property
    def progress_unit(self) -> SourceProgressUnit:
        return "records"

    def __iter__(self) -> Iterator[Any]:
        return self.load()


class SyntheticLoader(BaseDataLoader):
    """Expose an in-process generator through the source loader contract."""

    def __init__(self, generator: DataGenerator) -> None:
        self.generator = generator

    def load(self) -> Iterator[Any]:
        return self.generator.generate()

    @property
    def progress_unit(self) -> SourceProgressUnit:
        return "ticks"
