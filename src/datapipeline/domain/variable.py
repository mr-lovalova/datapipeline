from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class VariableRecord:
    id: str
    time: datetime
    value: Any
    entity_key: tuple = ()


@dataclass
class VariableSequence:
    id: str
    time: datetime
    values: list[Any]
    entity_key: tuple = ()
