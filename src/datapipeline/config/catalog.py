from typing import Annotated, Any

from pydantic import (
    BeforeValidator,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    StringConstraints,
    field_validator,
    model_validator,
)

from datapipeline.transforms.spec import (
    TransformSpec,
    parse_transform_spec,
    serialize_transform_spec,
)


_ConfiguredTransform = Annotated[
    TransformSpec,
    BeforeValidator(parse_transform_spec),
    PlainSerializer(
        serialize_transform_spec,
        return_type=dict[str, dict[str, Any]],
    ),
]


_EntryPoint = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class EPArgs(BaseModel):
    entrypoint: _EntryPoint
    args: dict[str, Any] = Field(default_factory=dict)


_SourceInputPath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class SourceInputsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    files: list[_SourceInputPath] = Field(min_length=1)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    parser: EPArgs
    loader: EPArgs
    inputs: SourceInputsConfig | None = None


class IngestFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str


class IngestConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    from_: IngestFromConfig = Field(alias="from")
    map: EPArgs
    cadence: Any | None = None
    partition_by: str | list[str] | None = Field(default=None)
    feature_id_by: str | list[str] | None = Field(default=None)
    ordered_by: list[str] | None = Field(default=None)
    record: list[_ConfiguredTransform] | None = Field(default=None)


class StreamRefConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream: str

    @field_validator("stream")
    @classmethod
    def validate_stream(cls, stream: str) -> str:
        stream = stream.strip()
        if not stream:
            raise ValueError("from.stream must not be empty")
        if "@" in stream or ":" in stream:
            raise ValueError("from.stream must reference a canonical stream id")
        return stream


class AlignFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    align: list[str] = Field(min_length=2)

    @field_validator("align")
    @classmethod
    def validate_streams(cls, streams: list[str]) -> list[str]:
        normalized = [stream.strip() for stream in streams]
        if any(not stream for stream in normalized):
            raise ValueError("from.align stream ids must not be empty")
        if any("@" in stream or ":" in stream for stream in normalized):
            raise ValueError("from.align must reference canonical stream ids")
        if len(set(normalized)) != len(normalized):
            raise ValueError("from.align must not contain duplicate stream ids")
        return normalized


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    from_: StreamRefConfig | AlignFromConfig = Field(alias="from")
    map: EPArgs | None = None
    cadence: Any | None = None
    partition_by: str | list[str] | None = Field(default=None)
    feature_id_by: str | list[str] | None = Field(default=None)
    ordered_by: list[str] | None = Field(default=None)
    stream: list[_ConfiguredTransform] | None = Field(default=None)
    # Optional debug-only transforms (applied after stream transforms)
    debug: list[_ConfiguredTransform] | None = Field(default=None)

    @property
    def aligns_streams(self) -> bool:
        return isinstance(self.from_, AlignFromConfig)

    def input_streams(self) -> tuple[str, ...]:
        if isinstance(self.from_, AlignFromConfig):
            return tuple(self.from_.align)
        return (self.from_.stream,)

    @model_validator(mode="after")
    def _validate_stream(self):
        if self.aligns_streams and self.partition_by is not None:
            raise ValueError("aligned streams inherit partition_by from their inputs")
        if self.aligns_streams and self.map is None:
            raise ValueError("aligned streams require map.entrypoint")
        return self


class StreamsConfig(BaseModel):
    raw: dict[str, SourceConfig] = Field(default_factory=dict)
    ingests: dict[str, IngestConfig] = Field(default_factory=dict)
    streams: dict[str, StreamConfig] = Field(default_factory=dict)
