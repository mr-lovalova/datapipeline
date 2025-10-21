from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Generic, TypeVar

from datapipeline.runtime import Runtime
from datapipeline.services.constants import PARTIONED_IDS

ArtifactValue = TypeVar("ArtifactValue")


ArtifactLoader = Callable[[Path], ArtifactValue]


@dataclass(frozen=True)
class ArtifactSpec(Generic[ArtifactValue]):
    key: str
    loader: ArtifactLoader[ArtifactValue]


def _read_expected_ids(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip()]


PARTITIONED_IDS_SPEC = ArtifactSpec[list[str]](
    key=PARTIONED_IDS,
    loader=_read_expected_ids,
)


def _resolve_artifact_path(runtime: Runtime, spec: ArtifactSpec[ArtifactValue]) -> Path:
    try:
        value = runtime.registries.artifacts.get(spec.key)
    except KeyError as exc:
        raise RuntimeError(
            f"Artifact '{spec.key}' is not registered. "
            "Run `jerry build --project <project.yaml>` first."
        ) from exc
    path = value if isinstance(value, Path) else Path(value)
    return path.resolve()


def get_artifact(runtime: Runtime, spec: ArtifactSpec[ArtifactValue]) -> ArtifactValue:
    """Load an artifact declared by *spec* using the runtime registry."""

    path = _resolve_artifact_path(runtime, spec)
    try:
        return spec.loader(path)
    except FileNotFoundError as exc:
        message = (
            f"Artifact file not found: {path}. "
            "Run `jerry build --project <project.yaml>` (preferred) or "
            "`jerry inspect expected --project <project.yaml>` to regenerate it."
        )
        raise RuntimeError(message) from exc
