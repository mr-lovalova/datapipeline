from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

NodeInput = Iterable[Any] | None
NodeOutput = Iterable[Any]
NodeOp = Callable[..., NodeOutput]


@dataclass(frozen=True)
class PipelineNode:
    name: str
    op: NodeOp
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] | None = None
    input: str | None = None
    output: str | None = None
