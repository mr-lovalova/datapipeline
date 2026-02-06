from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Record:
    pass


@dataclass
class TemporalRecord(Record):
    """Canonical time-series payload used throughout the pipeline."""

    time: datetime

    def __post_init__(self) -> None:
        if self.time.tzinfo is None:
            raise ValueError("time must be timezone-aware")
        self.time = self.time.astimezone(timezone.utc)

    def _identity_fields(self) -> dict:
        """Return a mapping of domain fields excluding 'time'."""
        data = {
            key: value
            for key, value in self.__dict__.items()
            if not key.startswith("_")
        }
        data.pop("time", None)
        return data

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, TemporalRecord):
            return NotImplemented
        return (
            self.time == other.time
            and self._identity_fields() == other._identity_fields()
        )
