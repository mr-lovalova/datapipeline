from typing import Dict, Optional, Any, List, Mapping, Union, Literal
from pydantic import BaseModel, Field, ConfigDict, model_validator


class EPArgs(BaseModel):
    entrypoint: str
    args: Dict[str, Any] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra='ignore')
    parser: EPArgs
    loader: EPArgs


class JoinConfig(BaseModel):
    primary: str
    on: str = Field(default="time")
    mode: Literal["inner", "left"] = Field(default="inner")
    broadcast: List[str] = Field(default_factory=list)


class ContractConfig(BaseModel):
    """Unified contract model with explicit kind.

    - kind = 'ingest': exactly one raw source via source alias
    - kind = 'joined': framework aligns canonical streams by time/partition
    - kind = 'manual': mapper owns all input stream iteration/alignment
    """
    kind: Literal['ingest', 'joined', 'manual']
    id: str

    # Ingest-only
    source: Optional[str] = Field(default=None)

    # Joined/manual-only: list of "[alias=]stream_id" (streams only)
    inputs: Optional[List[str]] = Field(default=None)
    join: Optional[JoinConfig] = Field(default=None)

    mapper: Optional[EPArgs] = None
    partition_by: Optional[Union[str, List[str]]] = Field(default=None)
    sort_batch_size: int = Field(default=100_000)
    record: Optional[List[Mapping[str, Any]]] = Field(default=None)
    stream: Optional[List[Mapping[str, Any]]] = Field(default=None)
    # Optional debug-only transforms (applied after stream transforms)
    debug: Optional[List[Mapping[str, Any]]] = Field(default=None)

    @staticmethod
    def parse_input_spec(spec: str) -> tuple[str, str]:
        alias: Optional[str] = None
        text = spec.strip()
        if "=" in text:
            alias, text = text.split("=", 1)
            alias = alias.strip()
        ref = text.strip()
        return (alias or ref), ref

    @model_validator(mode='after')
    def _validate_mode(self):
        if self.kind == 'ingest':
            if not self.source:
                raise ValueError("ingest contract requires 'source'")
            if self.inputs:
                raise ValueError("ingest contract cannot define 'inputs'")
            if self.join is not None:
                raise ValueError("ingest contract cannot define 'join'")
        elif self.kind in {'joined', 'manual'}:
            if not self.inputs or not isinstance(self.inputs, list):
                raise ValueError(
                    f"{self.kind} contract requires 'inputs' (list of stream ids)"
                )
            if self.source:
                raise ValueError(f"{self.kind} contract cannot define 'source'")
            if not self.mapper or not self.mapper.entrypoint:
                raise ValueError(f"{self.kind} contract requires mapper.entrypoint")
            if self.kind == "joined" and self.partition_by is not None:
                raise ValueError("joined contract inherits partition_by from join.primary")
            if self.kind == "joined" and self.join is None:
                raise ValueError("joined contract requires 'join'")
            if self.kind == "manual" and self.join is not None:
                raise ValueError("manual contract cannot define 'join'")
            # Enforce simple grammar: alias=stream_id or stream_id, no stages/prefixes
            aliases: set[str] = set()
            for item in self.inputs:
                if '@' in item:
                    raise ValueError(
                        f"{self.kind} inputs may not include '@stage'"
                    )
                alias, ref = self.parse_input_spec(item)
                if not alias or not ref:
                    raise ValueError(f"{self.kind} inputs must not be empty")
                if ':' in ref:
                    raise ValueError(
                        f"{self.kind} inputs must reference canonical stream ids only"
                    )
                if alias in aliases:
                    raise ValueError(
                        f"{self.kind} inputs contain duplicate alias '{alias}'"
                    )
                aliases.add(alias)
            driver = None
            if self.mapper and isinstance(self.mapper.args, dict):
                driver = self.mapper.args.get("driver")
            if (
                self.kind == "manual"
                and driver is not None
                and str(driver).strip() not in aliases
            ):
                raise ValueError(
                    "manual mapper.args.driver must reference one of the declared input aliases"
                )
            if self.kind == "joined" and self.join is not None:
                if self.join.primary not in aliases:
                    raise ValueError(
                        "joined join.primary must reference one of the declared input aliases"
                    )
                unknown_broadcast = [
                    alias for alias in self.join.broadcast if alias not in aliases
                ]
                if unknown_broadcast:
                    raise ValueError(
                        "joined join.broadcast contains unknown aliases: "
                        + ", ".join(unknown_broadcast)
                    )
        return self


class StreamsConfig(BaseModel):
    raw: Dict[str, SourceConfig] = Field(default_factory=dict)
    contracts: Dict[str, ContractConfig] = Field(default_factory=dict)
