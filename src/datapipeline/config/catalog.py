from typing import Any, Literal

from pydantic import BaseModel, Field, ConfigDict, model_validator


class EPArgs(BaseModel):
    entrypoint: str
    args: dict[str, Any] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    parser: EPArgs
    loader: EPArgs


class StreamFromConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str | None = None
    join: dict[str, str] | None = None
    streams: dict[str, str] | None = None
    primary: str | None = None
    on: str = Field(default="time")
    mode: Literal["inner", "left"] = Field(default="inner")
    broadcast: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_from(self):
        join_option_fields = {"primary", "on", "mode", "broadcast"}
        from_kinds = [
            self.source is not None,
            self.join is not None,
            self.streams is not None,
        ]
        if sum(from_kinds) != 1:
            raise ValueError(
                "from must define exactly one of 'source', 'join', or 'streams'"
            )
        if self.source is not None:
            if join_option_fields & self.model_fields_set:
                raise ValueError("from.source cannot define join options")
            return self
        if self.join is not None:
            if not self.primary:
                raise ValueError("from.join requires 'primary'")
            self.join = _normalize_input_refs("join", self.join)
            if self.primary not in self.join:
                raise ValueError(
                    "from.primary must reference one of the joined aliases"
                )
            unknown_broadcast = [
                alias for alias in self.broadcast if alias not in self.join
            ]
            if unknown_broadcast:
                raise ValueError(
                    "from.broadcast contains unknown aliases: "
                    + ", ".join(unknown_broadcast)
                )
            return self
        if join_option_fields & self.model_fields_set:
            raise ValueError("from.streams cannot define join options")
        self.streams = _normalize_input_refs("streams", self.streams or {})
        return self


def _normalize_input_refs(label: str, refs: dict[str, str]) -> dict[str, str]:
    if not refs:
        raise ValueError(f"from.{label} must not be empty")
    aliases: set[str] = set()
    normalized: dict[str, str] = {}
    for alias, ref in refs.items():
        alias_text = str(alias).strip()
        ref_text = str(ref).strip()
        if not alias_text or not ref_text:
            raise ValueError(
                f"from.{label} aliases and stream ids must not be empty"
            )
        if "@" in ref_text:
            raise ValueError(f"from.{label} may not include '@stage'")
        if ":" in ref_text:
            raise ValueError(
                f"from.{label} must reference canonical stream ids only"
            )
        if alias_text in aliases:
            raise ValueError(f"from.{label} contains duplicate alias '{alias_text}'")
        aliases.add(alias_text)
        normalized[alias_text] = ref_text
    return normalized


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    from_: StreamFromConfig = Field(alias="from")
    map: EPArgs | None = None
    cadence: Any | None = None
    partition_by: str | list[str] | None = Field(default=None)
    sort_batch_size: int = Field(default=100_000)
    record: list[dict[str, Any]] | None = Field(default=None)
    stream: list[dict[str, Any]] | None = Field(default=None)
    # Optional debug-only transforms (applied after stream transforms)
    debug: list[dict[str, Any]] | None = Field(default=None)

    @property
    def reads_source(self) -> bool:
        return self.from_.source is not None

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
        raise ValueError("from.source streams do not define input refs")

    @model_validator(mode="after")
    def _validate_stream(self):
        if self.joins_streams and self.partition_by is not None:
            raise ValueError("joined streams inherit partition_by from from.primary")
        if not self.reads_source and (not self.map or not self.map.entrypoint):
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
    streams: dict[str, StreamConfig] = Field(default_factory=dict)
