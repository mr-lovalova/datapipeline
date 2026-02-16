from __future__ import annotations

from dataclasses import dataclass

from datapipeline.execution.nodes.spec import PipelineNode


@dataclass(frozen=True)
class StageDag:
    name: str
    nodes: tuple[PipelineNode, ...]

    def upto_stage(self, stage: int | None) -> "StageDag":
        if stage is None:
            return self
        if stage < 0:
            return StageDag(name=self.name, nodes=())
        return StageDag(name=self.name, nodes=self.nodes[: stage + 1])
