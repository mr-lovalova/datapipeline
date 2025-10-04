from abc import ABC, abstractmethod
from typing import Iterator, Any, Optional
from .generator import DataGenerator


class RawDataLoader(ABC):
    @abstractmethod
    def load(self) -> Iterator[Any]:
        pass

    def count(self) -> Optional[int]:
        return None

    def __iter__(self) -> Iterator[Any]:
        return self.load()


class SyntheticLoader(RawDataLoader):
    """Adapter that turns a `DataGenerator` into a `RawDataLoader`.

    Keeps the `load()` contract used by the pipeline, while making the
    generative intent explicit and separate from I/O loaders.
    """

    def __init__(self, generator: DataGenerator):
        self.generator = generator

    def load(self) -> Iterator[Any]:
        return self.generator.generate()

    def count(self) -> Optional[int]:
        try:
            return self.generator.count()
        except Exception:
            return None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} generator={self.generator.__class__.__name__}>"


## Deprecated loaders (FileLoader, UrlLoader) have been removed.
