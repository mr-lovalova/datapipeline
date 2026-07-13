from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, TypeAlias


SourceOp: TypeAlias = Callable[[], Iterable[Any]]
StageOp: TypeAlias = Callable[[Iterator[Any]], Iterable[Any]]


@dataclass(frozen=True)
class SourceNode:
    """Open the first stream in a pipeline."""

    name: str
    open: SourceOp


@dataclass(frozen=True)
class PipelineNode:
    """Apply one ordered stage to the preceding stream."""

    name: str
    apply: StageOp


Node: TypeAlias = SourceNode | PipelineNode
