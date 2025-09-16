from datapipeline.domain.record import Record
from typing import Dict
from typing import Union

from dataclasses import dataclass


@dataclass
class Vector:
    values: Dict[str, Union[float, list[float]]]

    def __len__(self) -> int:
        return len(self.values)

    def shape(self) -> tuple[int, int | None]:
        first_value = next(iter(self.values.values()), None)
        if isinstance(first_value, list):
            return (len(self.values), len(first_value))
        return (1, len(self.values))

    def keys(self):
        return self.values.keys()

    def __getitem__(self, key: str) -> Union[float, list[float]]:
        return self.values[key]


@dataclass
class Sample:
    features: Vector
    targets: Vector


def vectorize_record_group(values: Dict[str, list[Record]]) -> Vector:
    structured: Dict[str, Union[float, list[float]]] = {}

    for key, records in values.items():
        if len(records) == 1:
            structured[key] = records[0].value
        else:
            # Preserve time order, assuming already sorted
            structured[key] = [r.value for r in records]

    return Vector(values=structured)
