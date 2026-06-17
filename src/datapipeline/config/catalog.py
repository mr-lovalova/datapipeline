from typing import Any, Literal

from pydantic import BaseModel, Field, ConfigDict, model_validator


class EPArgs(BaseModel):
    entrypoint: str
    args: dict[str, Any] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    parser: EPArgs
    loader: EPArgs


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
    ordered_by: list[str] | None = Field(default=None)
    sort_batch_size: int = Field(default=100_000)
    record: list[dict[str, Any]] | None = Field(default=None)


class StreamFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream: str | None = None
    join: dict[str, str] | None = None
    streams: dict[str, str] | None = None
    primary: str | None = None
    on: str | list[str] = Field(default="time")
    mode: Literal["inner", "left"] = Field(default="inner")

    @model_validator(mode="after")
    def _validate_from(self):
        join_option_fields = {"primary", "on", "mode"}
        from_kinds = [
            self.stream is not None,
            self.join is not None,
            self.streams is not None,
        ]
        if sum(from_kinds) != 1:
            raise ValueError(
                "from must define exactly one of 'stream', 'join', or 'streams'"
            )
        if self.stream is not None:
            if join_option_fields & self.model_fields_set:
                raise ValueError("from.stream cannot define join options")
            return self
        if self.join is not None:
            if not self.primary:
                raise ValueError("from.join requires 'primary'")
            self.join = _normalize_join_refs(self.join)
            self.on = _normalize_join_fields(self.on)
            if self.primary not in self.join:
                raise ValueError(
                    "from.primary must reference one of the joined aliases"
                )
            return self
        if join_option_fields & self.model_fields_set:
            raise ValueError("from.streams cannot define join options")
        self.streams = _normalize_stream_refs(self.streams or {})
        return self


def _normalize_join_refs(refs: dict[str, str]) -> dict[str, str]:
    if not refs:
        raise ValueError("from.join must not be empty")
    aliases: set[str] = set()
    normalized: dict[str, str] = {}
    for alias, ref in refs.items():
        alias_text = str(alias).strip()
        ref_text = str(ref).strip()
        if not alias_text or not ref_text:
            raise ValueError(
                "from.join aliases and stream ids must not be empty"
            )
        if "@" in ref_text:
            raise ValueError("from.join may not include '@stage'")
        if ":" in ref_text:
            raise ValueError(
                "from.join must reference canonical stream ids only"
            )
        if alias_text in aliases:
            raise ValueError(f"from.join contains duplicate alias '{alias_text}'")
        aliases.add(alias_text)
        normalized[alias_text] = ref_text
    return normalized


def _normalize_stream_refs(refs: dict[str, str]) -> dict[str, str]:
    if not refs:
        raise ValueError("from.streams must not be empty")
    aliases: set[str] = set()
    normalized: dict[str, str] = {}
    for alias, ref in refs.items():
        alias_text = str(alias).strip()
        ref_text = str(ref).strip()
        if not alias_text or not ref_text:
            raise ValueError(
                "from.streams aliases and stream ids must not be empty"
            )
        if "@" in ref_text:
            raise ValueError("from.streams may not include '@stage'")
        if ":" in ref_text:
            raise ValueError(
                "from.streams must reference canonical stream ids only"
            )
        if alias_text in aliases:
            raise ValueError(f"from.streams contains duplicate alias '{alias_text}'")
        aliases.add(alias_text)
        normalized[alias_text] = ref_text
    return normalized


def _normalize_join_fields(fields: str | list[str]) -> str | list[str]:
    if isinstance(fields, str):
        field = fields.strip()
        if not field:
            raise ValueError("from.on must not be empty")
        return field
    normalized = [str(field).strip() for field in fields]
    if not normalized or any(not field for field in normalized):
        raise ValueError("from.on must not contain empty fields")
    return normalized


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    from_: StreamFromConfig = Field(alias="from")
    map: EPArgs | None = None
    cadence: Any | None = None
    partition_by: str | list[str] | None = Field(default=None)
    ordered_by: list[str] | None = Field(default=None)
    sort_batch_size: int = Field(default=100_000)
    record: list[dict[str, Any]] | None = Field(default=None)
    stream: list[dict[str, Any]] | None = Field(default=None)
    # Optional debug-only transforms (applied after stream transforms)
    debug: list[dict[str, Any]] | None = Field(default=None)

    @property
    def joins_streams(self) -> bool:
        return self.from_.join is not None

    @property
    def maps_streams(self) -> bool:
        return self.from_.streams is not None

    def input_refs(self) -> dict[str, str]:
        if self.from_.join is not None:
            return dict(self.from_.join)
        if self.from_.streams is not None:
            return dict(self.from_.streams)
        if self.from_.stream is not None:
            return {"stream": self.from_.stream}
        raise ValueError("stream does not define input refs")

    @model_validator(mode="after")
    def _validate_stream(self):
        if self.joins_streams and self.partition_by is not None:
            raise ValueError("joined streams inherit partition_by from from.primary")
        if self.record is not None:
            raise ValueError("streams cannot define record transforms")
        if (self.joins_streams or self.maps_streams) and (
            not self.map or not self.map.entrypoint
        ):
            raise ValueError("stream map.entrypoint is required")
        driver = self.map.args.get("driver") if self.map else None
        if (
            self.maps_streams
            and driver is not None
            and str(driver).strip() not in self.input_refs()
        ):
            raise ValueError(
                "map.args.driver must reference one of the declared input aliases"
            )
        return self


class StreamsConfig(BaseModel):
    raw: dict[str, SourceConfig] = Field(default_factory=dict)
    ingests: dict[str, IngestConfig] = Field(default_factory=dict)
    streams: dict[str, StreamConfig] = Field(default_factory=dict)
