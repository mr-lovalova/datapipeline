from dataclasses import dataclass
from typing import Any

from datapipeline.dag.node import PipelineStep


@dataclass(frozen=True)
class StageDag:
    name: str
    nodes: tuple[PipelineStep, ...]
    metadata: dict[str, Any] | None = None

    def upto_step(self, step: int | None) -> "StageDag":
        if step is None:
            return self
        if step < 0:
            return StageDag(name=self.name, nodes=(), metadata=self.metadata)
        return StageDag(
            name=self.name,
            nodes=self.nodes[: step + 1],
            metadata=self.metadata,
        )

    @property
    def steps(self) -> tuple[PipelineStep, ...]:
        return self.nodes
