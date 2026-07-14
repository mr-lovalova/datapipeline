from collections.abc import Mapping
from typing import Annotated, Any, Literal, Self, TypeAlias

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

_CsvDelimiter = Annotated[
    str,
    StringConstraints(min_length=1, max_length=1),
]


class _DecodedSourceArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["csv", "json", "jsonl", "pickle"]
    encoding: _SourceInputPath = "utf-8"
    delimiter: _CsvDelimiter = ";"
    error_prefixes: tuple[str, ...] = ()
    array_field: _SourceInputPath | None = None

    @model_validator(mode="after")
    def validate_format_options(self) -> Self:
        if self.format == "pickle" and "encoding" in self.model_fields_set:
            raise ValueError("encoding is not valid for the pickle format")
        if self.format != "csv" and "delimiter" in self.model_fields_set:
            raise ValueError("delimiter is only valid for the csv format")
        if self.format != "csv" and "error_prefixes" in self.model_fields_set:
            raise ValueError("error_prefixes is only valid for the csv format")
        if self.format != "json" and "array_field" in self.model_fields_set:
            raise ValueError("array_field is only valid for the json format")
        return self


class FsSourceArgs(_DecodedSourceArgs):
    transport: Literal["fs"]
    path: _SourceInputPath


class HttpSourceArgs(_DecodedSourceArgs):
    transport: Literal["http"]
    format: Literal["csv", "json", "jsonl"]
    url: _SourceInputPath
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: (
        Annotated[
            float,
            Field(strict=True, gt=0, allow_inf_nan=False),
        ]
        | None
    ) = None
    count_by_fetch: bool = Field(default=False, strict=True)


CoreIoSourceArgs: TypeAlias = Annotated[
    FsSourceArgs | HttpSourceArgs,
    Field(discriminator="transport"),
]


class CoreIoLoaderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entrypoint: Literal["core.io"]
    args: CoreIoSourceArgs


class SourceInputsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    files: tuple[_SourceInputPath, ...] = Field(min_length=1)

    @field_validator("files")
    @classmethod
    def normalize_files(cls, files: tuple[str, ...]) -> tuple[str, ...]:
        if len(files) != len(set(files)):
            raise ValueError("source input files must not contain duplicates")
        return tuple(sorted(files))


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: _CanonicalId
    parser: EntryPointConfig
    loader: CoreIoLoaderConfig | EntryPointConfig
    inputs: SourceInputsConfig | None = None

    @field_validator("loader", mode="before")
    @classmethod
    def validate_core_io_loader(cls, loader: Any) -> Any:
        if isinstance(loader, CoreIoLoaderConfig):
            return loader
        if isinstance(loader, EntryPointConfig):
            if loader.entrypoint == "core.io":
                return CoreIoLoaderConfig.model_validate(loader.model_dump())
            return loader
        if isinstance(loader, Mapping):
            entrypoint = loader.get("entrypoint")
            if isinstance(entrypoint, str) and entrypoint.strip() == "core.io":
                data = dict(loader)
                data["entrypoint"] = "core.io"
                return CoreIoLoaderConfig.model_validate(data)
        return loader


class IngestFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: _CanonicalId


class IngestConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: _CanonicalId
    from_: IngestFromConfig = Field(alias="from")
    map: EntryPointConfig
    partition_by: tuple[_FieldName, ...] = ()
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
    ordered_by: tuple[_FieldName, ...] | None = None
    stream: list[StreamTransformConfig] = Field(default_factory=list)


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
    def validate_partition_override(self) -> Self:
        if "partition_by" in self.model_fields_set and self.partition_by is None:
            raise ValueError("partition_by must be a list when set; omit it to inherit")
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
