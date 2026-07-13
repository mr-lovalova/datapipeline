from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.utils.time import CADENCE_PATTERN


WindowMode = Literal["union", "intersection", "strict", "relaxed"]


class Window(BaseModel):
    """Typed representation of artifact window bounds."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    start: datetime
    end: datetime
    mode: WindowMode
    size: int = Field(
        strict=True,
        gt=0,
        description="Count of cadence buckets from start to end when known.",
    )

    @model_validator(mode="after")
    def _validate_window(self) -> Self:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("metadata window bounds must be timezone-aware")
        if self.start > self.end:
            raise ValueError("metadata window start cannot be after end")
        return self


class SampleDomainEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    key: list[Any]
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def _validate_window(self) -> Self:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("sample domain bounds must be timezone-aware")
        if self.start > self.end:
            raise ValueError("sample domain start cannot be after end")
        return self


class SampleMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    cadence: str = Field(pattern=CADENCE_PATTERN)
    keys: list[str] = Field(min_length=1)
    domain: list[SampleDomainEntry] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_domain_keys(self) -> Self:
        if any(not key.strip() for key in self.keys):
            raise ValueError("sample metadata keys must not be empty")
        if len(self.keys) != len(set(self.keys)):
            raise ValueError("sample metadata keys must be unique")
        if any(len(entry.key) != len(self.keys) for entry in self.domain):
            raise ValueError(
                "sample domain key length must match the configured sample keys"
            )
        key_contract = SampleKeyContract(self.keys)
        seen: set[tuple[object, ...]] = set()
        for entry in self.domain:
            try:
                key_contract.validate(entry.key)
            except (TypeError, ValueError) as exc:
                raise ValueError(str(exc)) from exc
            key = tuple(entry.key)
            if key in seen:
                raise ValueError("sample metadata domain keys must be unique")
            seen.add(key)
        return self


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
        if self.kind == "list" and self.cadence is None:
            raise ValueError("list schema entries must define cadence")
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

    @model_validator(mode="after")
    def _validate_shared_id_space(self) -> Self:
        feature_ids = {entry.id for entry in self.features}
        target_ids = {entry.id for entry in self.targets}
        if feature_ids & target_ids:
            raise ValueError(
                "schema entry ids must be unique across features and targets"
            )
        return self


class _VectorMetadataEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(min_length=1)
    base_id: str = Field(min_length=1)
    present_count: int = Field(strict=True, ge=0)
    null_count: int = Field(strict=True, ge=0)
    first_observed: datetime | None = None
    last_observed: datetime | None = None

    @model_validator(mode="after")
    def _validate_null_count(self) -> Self:
        if self.null_count > self.present_count:
            raise ValueError("metadata null_count cannot exceed present_count")
        if (self.first_observed is None) != (self.last_observed is None):
            raise ValueError(
                "metadata observation bounds must both be present or both be absent"
            )
        if self.present_count == 0 and self.first_observed is not None:
            raise ValueError("empty metadata entries cannot define observation bounds")
        if self.first_observed is not None and self.last_observed is not None:
            if self.first_observed.tzinfo is None or self.last_observed.tzinfo is None:
                raise ValueError("metadata observation bounds must be timezone-aware")
            if self.first_observed > self.last_observed:
                raise ValueError(
                    "metadata first_observed cannot be after last_observed"
                )
        return self


class ScalarVectorMetadataEntry(_VectorMetadataEntry):
    kind: Literal["scalar"]
    value_types: tuple[str, ...] = ()


class ListVectorMetadataEntry(_VectorMetadataEntry):
    kind: Literal["list"]
    element_types: tuple[str, ...] = ()
    lengths: dict[str, int] = Field(default_factory=dict)
    cadence: VectorSchemaCadence
    observed_elements: int = Field(strict=True, ge=0)

    @model_validator(mode="after")
    def _validate_observed_elements(self) -> Self:
        non_null_count = self.present_count - self.null_count
        expected_lengths = (
            {str(self.cadence.target): non_null_count} if non_null_count else {}
        )
        if self.lengths != expected_lengths:
            raise ValueError(
                "metadata list lengths must match the fixed schema cadence"
            )
        maximum = non_null_count * self.cadence.target
        if self.observed_elements > maximum:
            raise ValueError(
                "metadata observed_elements cannot exceed present sequence capacity"
            )
        return self


VectorMetadataEntry = Annotated[
    ScalarVectorMetadataEntry | ListVectorMetadataEntry,
    Field(discriminator="kind"),
]


class VectorMetadataCounts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    feature_vectors: int = Field(strict=True, ge=0)
    target_vectors: int = Field(strict=True, ge=0)


class VectorMetadata(BaseModel):
    """Typed contract for build/metadata.json."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    generated_at: datetime | None = None
    window: Window | None = None
    meta: dict[str, Any] | None = None
    features: tuple[VectorMetadataEntry, ...] = ()
    targets: tuple[VectorMetadataEntry, ...] = ()
    counts: VectorMetadataCounts
    sample: SampleMetadata | None = None

    @model_validator(mode="after")
    def _validate_counts_and_ids(self) -> Self:
        feature_ids = [entry.id for entry in self.features]
        if len(feature_ids) != len(set(feature_ids)):
            raise ValueError("feature metadata ids must be unique")
        if any(
            entry.present_count > self.counts.feature_vectors for entry in self.features
        ):
            raise ValueError(
                "feature metadata present_count exceeds the feature vector count"
            )

        target_ids = [entry.id for entry in self.targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("target metadata ids must be unique")
        if any(
            entry.present_count > self.counts.target_vectors for entry in self.targets
        ):
            raise ValueError(
                "target metadata present_count exceeds the target vector count"
            )
        duplicate_ids = set(feature_ids) & set(target_ids)
        if duplicate_ids:
            raise ValueError(
                "vector metadata ids must be unique across features and targets"
            )
        if self.generated_at is not None and self.generated_at.tzinfo is None:
            raise ValueError("metadata generated_at must be timezone-aware")
        return self
