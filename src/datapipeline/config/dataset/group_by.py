from pydantic import BaseModel, Field
from typing import Annotated, Union, List, Any, Literal
from datapipeline.config.dataset.normalize import floor_time_to_resolution


class GroupKey(BaseModel):
    type: str
    field: str

    def normalize(self, val: Any) -> Any:
        return val


class TimeKey(GroupKey):
    type: Literal["time"]
    resolution: str = Field(..., pattern=r"^\d+(min|h)$")

    def normalize(self, val: Any) -> Any:
        return floor_time_to_resolution(val, self.resolution)


class CategoricalKey(GroupKey):
    type: Literal["category"]


GroupKeyUnion = Annotated[Union[TimeKey,
                                CategoricalKey], Field(discriminator="type")]


class GroupBy(BaseModel):
    keys: List[GroupKeyUnion]
