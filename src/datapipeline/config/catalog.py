from typing import Annotated, Any, Self, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from datapipeline.config.transforms import (
    RecordTransformConfig,
    StreamTransformConfig,
)


_EntryPoint = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]

_FieldName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]

_CanonicalId = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[^@:]+$",
    ),
]


class EntryPointConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    model_config = ConfigDict(extra="forbid")

    id: _CanonicalId
    parser: EntryPointConfig
    loader: EntryPointConfig
    inputs: SourceInputsConfig | None = None


class IngestFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: _CanonicalId


class IngestConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: _CanonicalId
    from_: IngestFromConfig = Field(alias="from")
    map: EntryPointConfig
    partition_by: tuple[_FieldName, ...] = ()
    feature_id_by: tuple[_FieldName, ...] | None = None
    ordered_by: tuple[_FieldName, ...] | None = None
    record: list[RecordTransformConfig] = Field(default_factory=list)

    @field_validator("partition_by")
    @classmethod
    def validate_partition_by(cls, fields: tuple[str, ...]) -> tuple[str, ...]:
        if len(fields) != len(set(fields)):
            raise ValueError("partition_by must not contain duplicate fields")
        if "time" in fields:
            raise ValueError("partition_by must not contain the reserved field 'time'")
        return fields

    @field_validator("feature_id_by")
    @classmethod
    def validate_feature_id_by(
        cls,
        fields: tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if fields is not None and len(fields) != len(set(fields)):
            raise ValueError("feature_id_by must not contain duplicate fields")
        return fields


class StreamRefConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream: _CanonicalId


class AlignFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    align: tuple[_CanonicalId, ...] = Field(min_length=2)

    @field_validator("align")
    @classmethod
    def validate_streams(cls, streams: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(streams)) != len(streams):
            raise ValueError("from.align must not contain duplicate stream ids")
        return streams


class _StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: _CanonicalId
    feature_id_by: tuple[_FieldName, ...] | None = None
    ordered_by: tuple[_FieldName, ...] | None = None
    stream: list[StreamTransformConfig] = Field(default_factory=list)

    @field_validator("feature_id_by")
    @classmethod
    def validate_feature_id_by(
        cls,
        fields: tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if fields is not None and len(fields) != len(set(fields)):
            raise ValueError("feature_id_by must not contain duplicate fields")
        return fields


class DerivedStreamConfig(_StreamConfig):
    from_: StreamRefConfig = Field(alias="from")
    map: EntryPointConfig | None = None
    partition_by: tuple[_FieldName, ...] | None = None

    @field_validator("partition_by")
    @classmethod
    def validate_partition_by(
        cls,
        fields: tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if fields is None:
            return None
        if len(fields) != len(set(fields)):
            raise ValueError("partition_by must not contain duplicate fields")
        if "time" in fields:
            raise ValueError("partition_by must not contain the reserved field 'time'")
        return fields

    @model_validator(mode="after")
    def validate_identity_overrides(self) -> Self:
        if "partition_by" in self.model_fields_set and self.partition_by is None:
            raise ValueError(
                "partition_by must be a list when set; omit it to inherit"
            )
        if "feature_id_by" in self.model_fields_set and self.feature_id_by is None:
            raise ValueError(
                "feature_id_by must be a list when set; omit it to inherit"
            )
        return self

    def input_streams(self) -> tuple[str, ...]:
        return (self.from_.stream,)


class AlignedStreamConfig(_StreamConfig):
    from_: AlignFromConfig = Field(alias="from")
    combine: EntryPointConfig

    def input_streams(self) -> tuple[str, ...]:
        return self.from_.align


StreamConfig: TypeAlias = DerivedStreamConfig | AlignedStreamConfig


class StreamsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    ingests: dict[str, IngestConfig] = Field(default_factory=dict)
    streams: dict[str, StreamConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_registry_keys(self) -> Self:
        for source_id, source in self.sources.items():
            if source_id != source.id:
                raise ValueError(
                    f"Source registry key {source_id!r} does not match "
                    f"source id {source.id!r}"
                )
        for ingest_id, ingest in self.ingests.items():
            if ingest_id != ingest.id:
                raise ValueError(
                    f"Ingest registry key {ingest_id!r} does not match "
                    f"ingest id {ingest.id!r}"
                )
        for stream_id, stream in self.streams.items():
            if stream_id != stream.id:
                raise ValueError(
                    f"Stream registry key {stream_id!r} does not match "
                    f"stream id {stream.id!r}"
                )
        return self
