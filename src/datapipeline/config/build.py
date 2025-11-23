from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from datapipeline.services.project_paths import build_config_path
from datapipeline.utils.load import load_yaml


class PartitionedIdsConfig(BaseModel):
    """Configuration for writing the expected partitioned-id list."""

    enabled: bool = Field(
        default=True,
        description="Disable to skip generating this partitioned-id artifact.",
    )
    output: str = Field(
        default="expected.txt",
        description="Artifact path relative to project.paths.artifacts.",
    )
    target: Literal["features", "targets"] = Field(
        default="features",
        description="Vector domain to evaluate for expected IDs ('features' or 'targets').",
    )
    source_path: Path | None = Field(default=None, exclude=True)


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


class VectorSchemaConfig(BaseModel):
    """Configuration for writing the vector schema manifest."""

    enabled: bool = Field(
        default=True,
        description="Disable to skip generating the vector schema artifact.",
    )
    output: str = Field(
        default="schema.json",
        description="Artifact path relative to project.paths.artifacts.",
    )
    include_targets: bool = Field(
        default=False,
        description="Include dataset.targets when summarizing the schema.",
    )
    source_path: Path | None = Field(default=None, exclude=True)
    cadence_strategy: Literal["max"] = Field(
        default="max",
        description="Strategy for selecting cadence targets (currently only 'max' is supported).",
    )


class BuildConfig(BaseModel):
    """Top-level build configuration describing materialized artifacts."""

    version: int = 1
    partitioned_ids: list[PartitionedIdsConfig] = Field(
        default_factory=list,
        description="Partitioned-id task settings (one entry per artifact).",
    )
    scaler: ScalerArtifactConfig = Field(
        default_factory=ScalerArtifactConfig,
        description="Standard-scaler statistics artifact settings.",
    )
    vector_schema: VectorSchemaConfig = Field(
        default_factory=lambda: VectorSchemaConfig(),
        description="Vector schema artifact settings.",
    )


def _load_from_artifact_directory(path: Path) -> BuildConfig:
    parts: list[PartitionedIdsConfig] = []
    scaler = ScalerArtifactConfig()
    schema = VectorSchemaConfig()
    partition_declared = False

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
                cfg = PartitionedIdsConfig.model_validate(data)
                cfg.source_path = p
                parts.append(cfg)
                partition_declared = True
            except Exception:
                pass
        elif kind == "scaler":
            try:
                scaler = ScalerArtifactConfig.model_validate(data)
            except Exception:
                pass
        elif kind in {"vector_schema", "schema"}:
            try:
                schema = VectorSchemaConfig.model_validate(data)
                schema.source_path = p  # type: ignore[attr-defined]
            except Exception:
                pass

    return BuildConfig(partitioned_ids=parts, scaler=scaler, vector_schema=schema)


def load_build_config(project_yaml: Path) -> BuildConfig:
    """Load build configuration from the artifacts directory."""
    path = build_config_path(project_yaml)
    if not path.is_dir():
        raise TypeError(
            f"project.paths.build must point to an artifacts directory, got: {path}"
        )
    return _load_from_artifact_directory(path)
