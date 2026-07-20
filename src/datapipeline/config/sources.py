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

from datapipeline.io.compression import Compression


_EntryPoint = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]

_SourceId = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        pattern=r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$",
    ),
]

_SourceInputPath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]

_CsvDelimiter = Annotated[
    str,
    StringConstraints(min_length=1, max_length=1),
]


class EntryPointConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entrypoint: _EntryPoint
    args: dict[str, Any] = Field(default_factory=dict)


class _DecodedSourceArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["csv", "json", "jsonl"]
    encoding: _SourceInputPath = "utf-8"
    delimiter: _CsvDelimiter = ";"
    error_prefixes: tuple[str, ...] = ()
    array_field: _SourceInputPath | None = None

    @model_validator(mode="after")
    def validate_format_options(self) -> Self:
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
    compression: Compression | None = None

    @model_validator(mode="after")
    def validate_compression(self) -> Self:
        if self.compression == "gzip" and self.format not in {"csv", "jsonl"}:
            raise ValueError(
                "gzip compression is supported only for csv and jsonl formats"
            )
        return self


class FsParquetSourceArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transport: Literal["fs"]
    format: Literal["parquet"]
    path: _SourceInputPath


class HttpSourceArgs(_DecodedSourceArgs):
    transport: Literal["http"]
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


_FsSourceArgs: TypeAlias = Annotated[
    FsSourceArgs | FsParquetSourceArgs,
    Field(discriminator="format"),
]

_CoreIoSourceArgs: TypeAlias = Annotated[
    _FsSourceArgs | HttpSourceArgs,
    Field(discriminator="transport"),
]


class CoreIoLoaderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entrypoint: Literal["core.io"]
    args: _CoreIoSourceArgs


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

    id: _SourceId
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
