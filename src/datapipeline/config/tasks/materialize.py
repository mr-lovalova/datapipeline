from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from datapipeline.config.model_utils import normalize_required_text

from .base import OperationTask


class MaterializeStreamOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    stream: str
    as_stream_id: str | None = Field(default=None, alias="as")
    force: StrictBool = False

    @field_validator("stream", mode="before")
    @classmethod
    def _normalize_stream(cls, value):
        return normalize_required_text(value, field_name="options.stream")

    @field_validator("as_stream_id", mode="before")
    @classmethod
    def _normalize_as_stream_id(cls, value):
        if value is None:
            return None
        return str(value).strip() or None


class MaterializeStreamTask(OperationTask):
    entrypoint: Literal["core.runtime.materialize_stream"] = Field(
        default="core.runtime.materialize_stream"
    )
    options: MaterializeStreamOptions


__all__ = ["MaterializeStreamOptions", "MaterializeStreamTask"]
