import math
from typing import Annotated, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


HASH_SPLIT_GROUP_KEY = "group"
HASH_SPLIT_FEATURE_PREFIX = "feature:"


class BaseSplitConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')


Ratio = Annotated[float, Field(ge=0.0, le=1.0)]


class HashSplitConfig(BaseSplitConfig):
    mode: Literal["hash"] = Field(default="hash")
    ratios: Optional[Dict[str, Ratio]] = None
    seed: int = 42
    key: str = HASH_SPLIT_GROUP_KEY

    @field_validator("key")
    @classmethod
    def _valid_key(cls, value: str) -> str:
        if value == HASH_SPLIT_GROUP_KEY:
            return value
        if value.startswith(HASH_SPLIT_FEATURE_PREFIX):
            feature_id = value.removeprefix(HASH_SPLIT_FEATURE_PREFIX)
            if feature_id:
                return value
            raise ValueError("hash split key must include a feature id")
        raise ValueError("hash split key must be 'group' or 'feature:<id>'")

    @model_validator(mode="after")
    def _ratios_sum_to_one(self):
        if self.ratios is None:
            return self  # allow None
        s = sum(self.ratios.values())
        if not math.isclose(s, 1.0, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError(f"'ratios' must sum to 1.0 (got {s})")
        return self


class TimeSplitConfig(BaseSplitConfig):
    mode: Literal["time"] = Field(default="time")
    boundaries: Optional[List[str]] = None
    labels: Optional[List[str]] = None


SplitConfig = Union[HashSplitConfig, TimeSplitConfig]
