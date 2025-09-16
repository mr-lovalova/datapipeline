from typing import Dict, Optional, Any
from pydantic import BaseModel, Field


class EPArgs(BaseModel):
    entrypoint: str
    args: Dict[str, Any] = Field(default_factory=dict)


class RawSourceSpec(BaseModel):
    parser: EPArgs
    loader: EPArgs


class CanonicalSpec(BaseModel):
    source: str
    mapper: Optional[EPArgs] = None


class StreamsConfig(BaseModel):
    raw: Dict[str, RawSourceSpec] = Field(default_factory=dict)
    canonical: Dict[str, CanonicalSpec] = Field(default_factory=dict)
