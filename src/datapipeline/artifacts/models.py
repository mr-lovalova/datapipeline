from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Window(BaseModel):
    """Typed representation of artifact window bounds."""

    start: datetime | None = None
    end: datetime | None = None
    mode: str | None = None
    size: int | None = Field(
        default=None,
        description="Count of cadence buckets from start to end when known.",
    )


class VectorSchemaEntry(BaseModel):
    """One feature or target entry in build/schema.json."""

    model_config = ConfigDict(extra="ignore")

    id: str | None = None
    kind: str | None = None
    cadence: dict[str, Any] | None = None


class VectorSchemaArtifact(BaseModel):
    """Typed contract for build/schema.json."""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = 1
    window: Window | None = None
    meta: dict[str, Any] | None = None
    features: list[VectorSchemaEntry] = Field(default_factory=list)
    targets: list[VectorSchemaEntry] = Field(default_factory=list)

    def entries_for_payload(self, payload: str) -> list[dict[str, Any]]:
        entries = self.targets if payload == "targets" else self.features
        return [entry.model_dump(mode="json", exclude_none=True) for entry in entries]


class VectorMetadata(BaseModel):
    """Typed contract for build/metadata.json."""

    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    generated_at: datetime | None = None
    window: Window | None = None
    meta: dict[str, Any] | None = None
    features: list[dict[str, Any]] = Field(default_factory=list)
    targets: list[dict[str, Any]] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)
