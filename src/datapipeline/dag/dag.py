from dataclasses import dataclass
from typing import Any

from datapipeline.dag.node import PipelineNode


@dataclass(frozen=True)
class Dag:
    name: str
    nodes: tuple[PipelineNode, ...]
    metadata: dict[str, Any] | None = None

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    def index_of(self, node_name: str) -> int:
        for index, node in enumerate(self.nodes):
            if node.name == node_name:
                return index
        raise ValueError(f"DAG '{self.name}' has no node named '{node_name}'.")

    def upto_node_named(self, node_name: str) -> "Dag":
        return self.upto_node(self.index_of(node_name))

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
