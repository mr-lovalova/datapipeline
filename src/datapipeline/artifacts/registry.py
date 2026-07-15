from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Generic, TypeVar

from datapipeline.artifacts.models import (
    VectorMetadata,
    VectorSchemaArtifact,
    VectorStatsArtifact,
)
from datapipeline.artifacts.scaler import ScalerArtifact, load_scaler_artifact
from datapipeline.artifacts.specs import (
    SCALER_STATISTICS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
    VECTOR_STATS,
)
from datapipeline.utils.json_artifact import read_json_artifact

ArtifactValue = TypeVar("ArtifactValue")


ArtifactLoader = Callable[[Path], ArtifactValue]


@dataclass(frozen=True)
class ArtifactSpec(Generic[ArtifactValue]):
    key: str
    loader: ArtifactLoader[ArtifactValue]


@dataclass(frozen=True)
class ArtifactRecord:
    relative_path: str
    meta: Mapping[str, Any]

    def resolve(self, root: Path) -> Path:
        return root / self.relative_path


class ArtifactNotRegisteredError(RuntimeError):
    """Raised when attempting to use an artifact that is not registered."""


class ArtifactRegistry:
    """Registered build artifacts available to a runtime."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._records: dict[str, ArtifactRecord] = {}
        self._loaded: dict[str, Any] = {}

    @property
    def root(self) -> Path:
        return self._root

    def register(
        self,
        key: str,
        relative_path: str,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        self._loaded.pop(key, None)
        self._records[key] = ArtifactRecord(
            relative_path=relative_path,
            meta=MappingProxyType(dict(meta or {})),
        )

    def clear(self) -> None:
        self._records.clear()
        self._loaded.clear()

    def has(self, key: str) -> bool:
        return key in self._records

    def require(self, key: str) -> ArtifactRecord:
        try:
            return self._records[key]
        except KeyError as exc:
            raise ArtifactNotRegisteredError(
                f"Artifact '{key}' is not registered. "
                "Run `jerry build --project <project.yaml>` first."
            ) from exc

    def optional(self, key: str) -> ArtifactRecord | None:
        return self._records.get(key)

    def resolve_path(self, key: str) -> Path:
        return self.require(key).resolve(self._root)

    def load(self, spec: ArtifactSpec[ArtifactValue]) -> ArtifactValue:
        if spec.key in self._loaded:
            return self._loaded[spec.key]
        path = self.resolve_path(spec.key)
        try:
            value = spec.loader(path)
        except FileNotFoundError as exc:
            message = (
                f"Artifact file not found: {path}. "
                "Run `jerry build --project <project.yaml>` to regenerate it."
            )
            raise RuntimeError(message) from exc
        self._loaded[spec.key] = value
        return value


def _read_vector_schema(path: Path) -> VectorSchemaArtifact:
    return VectorSchemaArtifact.model_validate(read_json_artifact(path))


def _read_vector_metadata(path: Path) -> VectorMetadata:
    return VectorMetadata.model_validate(read_json_artifact(path))


def _read_vector_stats(path: Path) -> VectorStatsArtifact:
    payload = read_json_artifact(path)
    version = payload.get("schema_version")
    if version != 3:
        raise ValueError(
            f"Unsupported vector stats schema version {version!r}. "
            "Rebuild stats in FORCE mode."
        )
    return VectorStatsArtifact.model_validate(payload)


VECTOR_SCHEMA_SPEC = ArtifactSpec[VectorSchemaArtifact](
    key=VECTOR_SCHEMA,
    loader=_read_vector_schema,
)

VECTOR_METADATA_SPEC = ArtifactSpec[VectorMetadata](
    key=VECTOR_METADATA,
    loader=_read_vector_metadata,
)

SCALER_SPEC = ArtifactSpec[ScalerArtifact](
    key=SCALER_STATISTICS,
    loader=load_scaler_artifact,
)

VECTOR_STATS_SPEC = ArtifactSpec[VectorStatsArtifact](
    key=VECTOR_STATS,
    loader=_read_vector_stats,
)
