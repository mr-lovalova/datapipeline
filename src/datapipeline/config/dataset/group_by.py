from typing import Any, List, Literal

from pydantic import BaseModel, Field, field_validator

from datapipeline.config.dataset.normalize import floor_time_to_resolution


class TimeKey_old(BaseModel):
    """Configuration for a time-based grouping key."""

    type: Literal["time"] = "time"
    field: str
    resolution: str = Field(..., pattern=r"^\d+(m|min|h)$")

    def normalize(self, val: Any) -> Any:
        return floor_time_to_resolution(val, self.resolution)

class TimeKey(BaseModel):
    """Configuration for a time-based grouping key."""

    type: Literal["time"] = "time"
    field: str
    resolution: str = Field(..., pattern=r"^\d+(m|min|h)$")

    def normalize(self, val: Any) -> Any:
        return floor_time_to_resolution(val, self.resolution)


class GroupBy(BaseModel):
    """Grouping configuration for time-series datasets."""

    keys: List[TimeKey]

    @field_validator("keys")
    def _ensure_time_keys(cls, value: List[TimeKey]) -> List[TimeKey]:
        if not value:
            raise ValueError("At least one time key must be provided")
        return value
