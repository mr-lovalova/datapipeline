from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SchemaPayload = Literal["features", "targets"]


class Window(BaseModel):
    """Typed representation of artifact window bounds."""

    start: datetime | None = None
    end: datetime | None = None
    mode: str | None = None
    size: int | None = Field(
        default=None,
        description="Count of cadence buckets from start to end when known.",
    )


class SampleDomainEntry(BaseModel):
    key: list[Any] = Field(default_factory=list)
    start: datetime | None = None
    end: datetime | None = None


class SampleMetadata(BaseModel):
    cadence: str
    keys: list[str] = Field(default_factory=list)
    domain: list[SampleDomainEntry] = Field(default_factory=list)


class VectorSchemaCadence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    target: int = Field(strict=True, gt=0)


class VectorSchemaEntry(BaseModel):
    """One feature or target entry in build/schema.json."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(min_length=1)
    kind: Literal["scalar", "list"]
    cadence: VectorSchemaCadence | None = None

    @model_validator(mode="after")
    def _validate_cadence(self) -> "VectorSchemaEntry":
        if self.kind == "scalar" and self.cadence is not None:
            raise ValueError("scalar schema entries cannot define cadence")
        return self


class VectorSchemaArtifact(BaseModel):
    """Typed contract for build/schema.json."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(strict=True, ge=2, le=2)
    features: tuple[VectorSchemaEntry, ...]
    targets: tuple[VectorSchemaEntry, ...]

    @field_validator("features", "targets")
    @classmethod
    def _validate_unique_ids(
        cls,
        entries: tuple[VectorSchemaEntry, ...],
    ) -> tuple[VectorSchemaEntry, ...]:
        identifiers = [entry.id for entry in entries]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("schema entry ids must be unique")
        return entries


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
    sample: SampleMetadata | None = None
