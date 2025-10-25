from typing import Dict, List, Literal, Optional, Union, Annotated
from pydantic import BaseModel, Field, ConfigDict, model_validator
import math


class BaseSplitConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')


Ratio = Annotated[float, Field(ge=0.0, le=1.0)]


class HashSplitConfig(BaseSplitConfig):
    mode: Literal["hash"] = Field(default="hash")
    ratios: Optional[Dict[str, Ratio]] = None
    seed: int = 42
    key: str = "group"  # "group" or "feature:<id>"

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
