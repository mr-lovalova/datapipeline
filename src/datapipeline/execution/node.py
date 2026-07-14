from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, TypeAlias

from datapipeline.execution.events import ProgressSnapshot


SourceOp: TypeAlias = Callable[[], Iterable[Any]]
StageOp: TypeAlias = Callable[[Iterator[Any]], Iterable[Any]]
NodeProgressReader: TypeAlias = Callable[[int], ProgressSnapshot]


@dataclass(frozen=True)
class SourceNode:
    """Open the first stream in a pipeline."""

    name: str
    open: SourceOp
    progress: NodeProgressReader | None = None


@dataclass(frozen=True)
class PipelineNode:
    """Apply one ordered stage to the preceding stream."""

    name: str
    apply: StageOp
    progress: NodeProgressReader | None = None


Node: TypeAlias = SourceNode | PipelineNode
