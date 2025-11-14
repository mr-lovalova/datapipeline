from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from datapipeline.services.project_paths import build_config_path
from datapipeline.utils.load import load_yaml


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


def _load_from_artifact_directory(path: Path) -> BuildConfig:
    part = PartitionedIdsConfig()
    scaler = ScalerArtifactConfig()

    for p in sorted(path.rglob("*.y*ml")):
        data = load_yaml(p)
        if not isinstance(data, dict):
            continue
        kind = str(data.get("kind", "")).strip().lower()
        if not kind:
            if "partitioned_ids" in data:
                data = data["partitioned_ids"] or {}
                kind = "partitioned_ids"
            elif "scaler" in data:
                data = data["scaler"] or {}
                kind = "scaler"

        out = data.get("output")
        if isinstance(out, dict) and "path" in out:
            data = dict(data)
            data["output"] = out.get("path")

        if kind == "partitioned_ids":
            try:
                part = PartitionedIdsConfig.model_validate(data)
            except Exception:
                pass
        elif kind == "scaler":
            try:
                scaler = ScalerArtifactConfig.model_validate(data)
            except Exception:
                pass

    return BuildConfig(partitioned_ids=part, scaler=scaler)


def load_build_config(project_yaml: Path) -> BuildConfig:
    """Load build configuration from the artifacts directory."""
    path = build_config_path(project_yaml)
    if not path.is_dir():
        raise TypeError(
            f"project.paths.build must point to an artifacts directory, got: {path}"
        )
    return _load_from_artifact_directory(path)
