from dataclasses import dataclass
from typing import Any

from datapipeline.dag.node import PipelineNode


@dataclass(frozen=True)
class Dag:
    name: str
    nodes: tuple[PipelineNode, ...]
    metadata: dict[str, Any] | None = None

    def upto_node(self, node_index: int | None) -> "Dag":
        if node_index is None:
            return self
        if node_index < 0:
            return Dag(name=self.name, nodes=(), metadata=self.metadata)
        return Dag(
            name=self.name,
            nodes=self.nodes[: node_index + 1],
            metadata=self.metadata,
        )
