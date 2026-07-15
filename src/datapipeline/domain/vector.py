from dataclasses import dataclass
from typing import Any


@dataclass
class Vector:
    values: dict[str, Any]

    def __len__(self) -> int:
        return len(self.values)

    def shape(self) -> tuple[int, int | None]:
        first_value = next(iter(self.values.values()), None)
        if isinstance(first_value, list):
            return (len(self.values), len(first_value))
        return (1, len(self.values))

    def keys(self):
        return self.values.keys()

    def __getitem__(self, key: str) -> Any:
        return self.values[key]
