from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from datapipeline.services.bootstrap import _load_by_key


class PartitionedIdsConfig(BaseModel):
    """Configuration for writing the expected partitioned-id list."""

    output: str = Field(
        default="expected.txt",
        description="Artifact path relative to project.paths.artifacts.",
    )
    include_targets: bool = Field(
        default=False,
        description="When true, include dataset.targets in the discovery stream.",
    )


class ScalerArtifactConfig(BaseModel):
    """Configuration for computing standard-scaler statistics."""

    enabled: bool = Field(
        default=True,
        description="Disable to skip generating the scaler statistics artifact.",
    )
    output: str = Field(
        default="scaler.pkl",
        description="Artifact path relative to project.paths.artifacts.",
    )
    include_targets: bool = Field(
        default=False,
        description="Include dataset.targets when fitting scaler statistics.",
    )
    split_label: str = Field(
        default="train",
        description="Split label to use when fitting scaler statistics.",
    )


class BuildConfig(BaseModel):
    """Top-level build configuration describing materialized artifacts."""

    version: int = 1
    partitioned_ids: PartitionedIdsConfig = Field(
        default_factory=PartitionedIdsConfig,
        description="Partitioned-id task settings.",
    )
    scaler: ScalerArtifactConfig = Field(
        default_factory=ScalerArtifactConfig,
        description="Standard-scaler statistics artifact settings.",
    )


def load_build_config(project_yaml: Path) -> BuildConfig:
    """Load build.yaml referenced by project.paths.build and validate it."""

    doc = _load_by_key(project_yaml, "build")
    if not isinstance(doc, dict):
        raise TypeError("build.yaml must define a mapping at the top level.")
    return BuildConfig.model_validate(doc)
