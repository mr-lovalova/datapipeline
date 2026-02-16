from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from datapipeline.pipeline.context import PipelineContext

NodeInput = Iterable[Any] | None
NodeOutput = Iterable[Any]
NodeCallable = Callable[[PipelineContext, NodeInput], NodeOutput]


@dataclass(frozen=True)
class PipelineNode:
    name: str
    run: NodeCallable
