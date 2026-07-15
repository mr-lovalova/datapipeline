from typing import Annotated, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

from datapipeline.config.sources import EntryPointConfig, SourceConfig
from datapipeline.config.transforms import PreprocessConfig, TransformConfig


_StreamId = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$",
    ),
]

_FieldName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class SourceRefConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: _StreamId


class StreamRefConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream: _StreamId


class AlignFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    align: tuple[_StreamId, ...] = Field(min_length=2)

    @field_validator("align")
    @classmethod
    def validate_streams(cls, streams: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(streams)) != len(streams):
            raise ValueError("from.align must not contain duplicate stream ids")
        return streams


class _StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: _StreamId
    transforms: list[TransformConfig] = Field(default_factory=list)


class SourceStreamConfig(_StreamConfig):
    from_: SourceRefConfig = Field(alias="from")
    map: EntryPointConfig
    preprocess: list[PreprocessConfig] = Field(default_factory=list)
    partition_by: tuple[_FieldName, ...] = ()
    ordered_by: tuple[_FieldName, ...] | None = None

    @field_validator("partition_by")
    @classmethod
    def validate_partition_by(cls, fields: tuple[str, ...]) -> tuple[str, ...]:
        if len(fields) != len(set(fields)):
            raise ValueError("partition_by must not contain duplicate fields")
        if "time" in fields:
            raise ValueError("partition_by must not contain the reserved field 'time'")
        return fields

    def input_streams(self) -> tuple[str, ...]:
        return ()


class DerivedStreamConfig(_StreamConfig):
    from_: StreamRefConfig = Field(alias="from")
    transforms: list[TransformConfig] = Field(min_length=1)

    def input_streams(self) -> tuple[str, ...]:
        return (self.from_.stream,)


class AlignedStreamConfig(_StreamConfig):
    from_: AlignFromConfig = Field(alias="from")
    combine: EntryPointConfig

    def input_streams(self) -> tuple[str, ...]:
        return self.from_.align


StreamConfig: TypeAlias = SourceStreamConfig | DerivedStreamConfig | AlignedStreamConfig


class StreamsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    streams: dict[str, StreamConfig] = Field(default_factory=dict)

    @field_validator("sources")
    @classmethod
    def validate_source_keys(
        cls,
        sources: dict[str, SourceConfig],
    ) -> dict[str, SourceConfig]:
        for source_id, source in sources.items():
            if source_id != source.id:
                raise ValueError(
                    f"Source registry key {source_id!r} does not match "
                    f"source id {source.id!r}"
                )
        return sources

    @field_validator("streams")
    @classmethod
    def validate_stream_keys(
        cls,
        streams: dict[str, StreamConfig],
    ) -> dict[str, StreamConfig]:
        for stream_id, stream in streams.items():
            if stream_id != stream.id:
                raise ValueError(
                    f"Stream registry key {stream_id!r} does not match "
                    f"stream id {stream.id!r}"
                )
        return streams
