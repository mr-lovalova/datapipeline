from typing import Dict, Union, Any

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


def vectorize_record_group(values: Dict[str, list[Any]]) -> Vector:
    structured: Dict[str, Union[float, list[float]]] = {}

    for key, items in values.items():
        if len(items) == 1:
            structured[key] = items[0]
        else:
            structured[key] = list(items)

    return Vector(values=structured)
