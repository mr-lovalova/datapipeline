from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class SeriesRecord:
    id: str
    time: datetime
    value: Any
    entity_key: tuple = ()


@dataclass
class SeriesSequence:
    id: str
    time: datetime
    values: list[Any]
    entity_key: tuple = ()
