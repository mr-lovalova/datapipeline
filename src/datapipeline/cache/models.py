from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Protocol

from datapipeline.sources.models.source import Source


@dataclass(frozen=True)
class MaterializationRef:
    kind: str
    name: str
    stage: str
    signature_hash: str


@dataclass(frozen=True)
class MaterializationManifest:
    version: int
    kind: str
    name: str
    stage: str
    signature_hash: str
    relative_data_path: str
    row_count: int
    created_at: str

    @classmethod
    def create(
        cls,
        ref: MaterializationRef,
        *,
        data_path: Path,
        root: Path,
        row_count: int,
    ) -> "MaterializationManifest":
        return cls(
            version=1,
            kind=ref.kind,
            name=ref.name,
            stage=ref.stage,
            signature_hash=ref.signature_hash,
            relative_data_path=str(data_path.relative_to(root)),
            row_count=row_count,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def as_json(self) -> dict[str, object]:
        return asdict(self)


class MaterializationStore(Protocol):
    def load(self, ref: MaterializationRef) -> Source | None: ...

    def materialize(
        self,
        ref: MaterializationRef,
        items: Iterator[Any],
    ) -> Iterator[Any]: ...


class SourceObserver(Protocol):
    def __call__(self, stream_source: Source, stream_id: str) -> Source: ...
