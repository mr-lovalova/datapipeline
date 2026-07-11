from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, Field, StrictBool, field_validator

from datapipeline.config.model_utils import normalize_required_text
from datapipeline.config.observability import ObservabilityConfig

from .base import Profile


class MaterializeProfile(Profile):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    cmd: Literal["materialize"]
    stream: str
    output: Path
    as_stream_id: str | None = Field(default=None, alias="as")
    overwrite: StrictBool = False
    observability: ObservabilityConfig | None = Field(default=None)

    @field_validator("stream", mode="before")
    @classmethod
    def _normalize_stream(cls, value: object) -> str:
        return normalize_required_text(value, field_name="stream")

    @field_validator("output", mode="before")
    @classmethod
    def _normalize_output(cls, value: object) -> Path:
        output = normalize_required_text(value, field_name="output")
        if Path(output).suffix != ".jsonl":
            raise ValueError("output must use a .jsonl path")
        return Path(output)

    @field_validator("as_stream_id", mode="before")
    @classmethod
    def _normalize_as_stream_id(cls, value: object) -> str | None:
        if value is None:
            return None
        return normalize_required_text(value, field_name="as")
