from pydantic import BaseModel, Field, model_validator

from datapipeline.config.dataset.feature import BaseRecordConfig, FeatureRecordConfig
from datapipeline.utils.time import CADENCE_PATTERN


class RecordDatasetConfig(BaseModel):
    features: list[BaseRecordConfig] = Field(default_factory=list)
    targets: list[BaseRecordConfig] = Field(default_factory=list)


class SampleConfig(BaseModel):
    cadence: str = Field(..., pattern=CADENCE_PATTERN)
    keys: list[str] = Field(default_factory=list)


class FeatureDatasetConfig(BaseModel):
    group_by: str | None = Field(default=None, pattern=CADENCE_PATTERN)
    sample: SampleConfig | None = None
    features: list[FeatureRecordConfig] = Field(default_factory=list)
    targets: list[FeatureRecordConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize_sample_config(self) -> "FeatureDatasetConfig":
        if self.sample is None:
            if self.group_by is None:
                raise ValueError("Feature datasets require sample.cadence or group_by")
            self.sample = SampleConfig(cadence=self.group_by)
            return self

        if self.group_by is None:
            self.group_by = self.sample.cadence
            return self

        if self.group_by != self.sample.cadence:
            raise ValueError("group_by must match sample.cadence when both are set")
        return self

    @property
    def sample_keys(self) -> list[str]:
        return list(self.sample.keys if self.sample else [])
