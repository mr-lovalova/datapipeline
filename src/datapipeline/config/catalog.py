from typing import Dict, Optional, Any, List, Mapping, Union, Literal
from pydantic import BaseModel, Field, ConfigDict


class EPArgs(BaseModel):
    entrypoint: str
    args: Dict[str, Any] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra='ignore')
    parser: EPArgs
    loader: EPArgs


class ContractConfig(BaseModel):
    source_id: str
    stream_id: str
    mapper: Optional[EPArgs] = None
    partition_by: Optional[Union[str, List[str]]] = Field(default=None)
    sort_batch_size: int = Field(default=100_000)
    record: Optional[List[Mapping[str, Any]]] = Field(default=None)
    stream: Optional[List[Mapping[str, Any]]] = Field(default=None)
    # Optional debug-only transforms (applied after stream transforms)
    debug: Optional[List[Mapping[str, Any]]] = Field(default=None)


class StreamsConfig(BaseModel):
    raw: Dict[str, SourceConfig] = Field(default_factory=dict)
    contracts: Dict[str, ContractConfig] = Field(default_factory=dict)
