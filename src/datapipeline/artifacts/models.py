from datetime import datetime
from typing import Annotated, Any, Final, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.utils.time import CADENCE_PATTERN


WindowMode = Literal["union", "intersection", "strict", "relaxed"]
VECTOR_METADATA_VERSION: Final = 2


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
    length: int = Field(strict=True, gt=0)
    observed_elements: int = Field(strict=True, ge=0)

    @model_validator(mode="after")
    def _validate_observed_elements(self) -> Self:
        non_null_count = self.present_count - self.null_count
        maximum = non_null_count * self.length
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

    schema_version: Literal[2] = VECTOR_METADATA_VERSION
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


class VectorBaseStats(BaseModel):
    """Sample-level availability for one unpartitioned vector ID."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(min_length=1)
    present_samples: int = Field(strict=True, ge=0)
    non_null_samples: int = Field(strict=True, ge=0)

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        if self.non_null_samples > self.present_samples:
            raise ValueError("non_null_samples cannot exceed present_samples")
        return self


class _VectorColumnStats(VectorBaseStats):
    base_id: str = Field(min_length=1)


class ScalarVectorColumnStats(_VectorColumnStats):
    kind: Literal["scalar"]


class ListVectorColumnStats(_VectorColumnStats):
    kind: Literal["list"]
    length: int = Field(strict=True, gt=0)
    observed_elements: int = Field(strict=True, ge=0)

    @model_validator(mode="after")
    def _validate_observed_elements(self) -> Self:
        if self.non_null_samples > self.observed_elements:
            raise ValueError(
                "non_null_samples cannot exceed observed_elements for list vectors"
            )
        maximum = self.present_samples * self.length
        if self.observed_elements > maximum:
            raise ValueError("observed_elements cannot exceed present_samples * length")
        return self


VectorColumnStats = Annotated[
    ScalarVectorColumnStats | ListVectorColumnStats,
    Field(discriminator="kind"),
]


class VectorStatsSection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    bases: tuple[VectorBaseStats, ...] = ()
    columns: tuple[VectorColumnStats, ...] = ()

    @model_validator(mode="after")
    def _validate_ids(self) -> Self:
        base_ids = [entry.id for entry in self.bases]
        if len(base_ids) != len(set(base_ids)):
            raise ValueError("vector stats base IDs must be unique")
        column_ids = [entry.id for entry in self.columns]
        if len(column_ids) != len(set(column_ids)):
            raise ValueError("vector stats column IDs must be unique")
        unknown_bases = {
            entry.base_id for entry in self.columns if entry.base_id not in base_ids
        }
        if unknown_bases:
            raise ValueError("vector stats columns must reference declared base IDs")
        return self


class VectorStatsArtifact(BaseModel):
    """Bounded summary of samples before or after postprocessing."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[3] = 3
    stage: Literal["assembled", "postprocessed"]
    total_samples: int = Field(strict=True, ge=0)
    empty_samples: int = Field(strict=True, ge=0)
    features: VectorStatsSection
    targets: VectorStatsSection

    @model_validator(mode="after")
    def _validate_counts_and_ids(self) -> Self:
        if self.empty_samples > self.total_samples:
            raise ValueError("empty_samples cannot exceed total_samples")
        for section in (self.features, self.targets):
            for entry in (*section.bases, *section.columns):
                if entry.present_samples > self.total_samples:
                    raise ValueError(
                        "vector stats present_samples cannot exceed total_samples"
                    )
        feature_ids = {entry.id for entry in self.features.columns}
        target_ids = {entry.id for entry in self.targets.columns}
        if feature_ids & target_ids:
            raise ValueError(
                "vector stats column IDs must be unique across features and targets"
            )
        return self
