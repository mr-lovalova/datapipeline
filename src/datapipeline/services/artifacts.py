from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Mapping, Optional, TypeVar

from datapipeline.services.constants import VECTOR_SCHEMA, VECTOR_SCHEMA_METADATA

ArtifactValue = TypeVar("ArtifactValue")


ArtifactLoader = Callable[[Path], ArtifactValue]


@dataclass(frozen=True)
class ArtifactSpec(Generic[ArtifactValue]):
    key: str
    loader: ArtifactLoader[ArtifactValue]


@dataclass(frozen=True)
class ArtifactRecord:
    key: str
    relative_path: str
    meta: Mapping[str, Any]

    def resolve(self, root: Path) -> Path:
        path = Path(self.relative_path)
        return path if path.is_absolute() else (root / path)


class ArtifactNotRegisteredError(RuntimeError):
    """Raised when attempting to use an artifact that is not registered."""


class ArtifactManager:
    """Manage materialized artifact locations and metadata."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._records: Dict[str, ArtifactRecord] = {}

    @property
    def root(self) -> Path:
        return self._root

    def register(self, key: str, *, relative_path: str, meta: Optional[Mapping[str, Any]] = None) -> None:
        self._records[key] = ArtifactRecord(
            key=key,
            relative_path=relative_path,
            meta=dict(meta or {}),
        )

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

    def metadata(self, key: str) -> Dict[str, Any]:
        return self.require(key).meta

    def resolve_path(self, key: str) -> Path:
        return self.require(key).resolve(self._root)

    def load(self, spec: ArtifactSpec[ArtifactValue]) -> ArtifactValue:
        path = self.resolve_path(spec.key)
        try:
            return spec.loader(path)
        except FileNotFoundError as exc:
            message = (
                f"Artifact file not found: {path}. "
                "Run `jerry build --project <project.yaml>` (preferred) or "
                "`jerry inspect expected --project <project.yaml>` to regenerate it."
            )
            raise RuntimeError(message) from exc


def _read_schema(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


VECTOR_SCHEMA_SPEC = ArtifactSpec[dict](
    key=VECTOR_SCHEMA,
    loader=_read_schema,
)

VECTOR_METADATA_SPEC = ArtifactSpec[dict](
    key=VECTOR_SCHEMA_METADATA,
    loader=_read_schema,
)
