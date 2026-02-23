from dataclasses import dataclass
from typing import Any

from datapipeline.dag.node import PipelineStep


@dataclass(frozen=True)
class StageDag:
    name: str
    nodes: tuple[PipelineStep, ...]
    metadata: dict[str, Any] | None = None

    def upto_stage(self, stage: int | None) -> "StageDag":
        if stage is None:
            return self
        if stage < 0:
            return StageDag(name=self.name, nodes=(), metadata=self.metadata)
        return StageDag(
            name=self.name,
            nodes=self.nodes[: stage + 1],
            metadata=self.metadata,
        )

    @property
    def steps(self) -> tuple[PipelineStep, ...]:
        return self.nodes
