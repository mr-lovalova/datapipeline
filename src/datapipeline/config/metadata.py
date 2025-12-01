from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class Window(BaseModel):
    """Typed representation of dataset window bounds."""

    start: Optional[datetime] = None
    end: Optional[datetime] = None
    mode: Optional[str] = None


class VectorMetadata(BaseModel):
    """Lightweight typed model for schema.metadata.json.

    Only window/counts/entries are modeled explicitly; all other fields are
    accepted via extra='allow' for forwards-compatibility.
    """

    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    generated_at: datetime
    window: Optional[Window] = None
    meta: Dict[str, Any] | None = None
    features: List[Dict[str, Any]] = Field(default_factory=list)
    targets: List[Dict[str, Any]] = Field(default_factory=list)
    counts: Dict[str, int] = Field(default_factory=dict)

    # Window is the single source of truth; no legacy fallbacks.
