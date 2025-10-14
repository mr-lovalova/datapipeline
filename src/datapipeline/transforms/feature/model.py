from typing import Iterator
from datapipeline.domain.feature import FeatureRecord
from abc import abstractmethod, ABC


class FeatureTransform(ABC):
    @abstractmethod
    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        pass

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)
