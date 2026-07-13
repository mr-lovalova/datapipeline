from dataclasses import dataclass

from datapipeline.execution.node import Node, SourceNode


@dataclass(frozen=True)
class Pipeline:
    """A linear, lazily evaluated record pipeline."""

    name: str
    nodes: tuple[Node, ...]
    summary: str | None = None

    def __post_init__(self) -> None:
        names = [node.name for node in self.nodes]
        if len(names) != len(set(names)):
            raise ValueError(f"Pipeline '{self.name}' node names must be unique.")
        for index, node in enumerate(self.nodes):
            if isinstance(node, SourceNode) and index != 0:
                raise ValueError(
                    f"Pipeline '{self.name}' source node '{node.name}' must be first."
                )

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    def index_of(self, node_name: str) -> int:
        for index, node in enumerate(self.nodes):
            if node.name == node_name:
                return index
        raise ValueError(f"Pipeline '{self.name}' has no node named '{node_name}'.")

    def through_node_named(self, node_name: str) -> "Pipeline":
        return self.through_node(self.index_of(node_name))

    def through_node(self, node_index: int) -> "Pipeline":
        if not 0 <= node_index < len(self.nodes):
            raise ValueError(
                f"Pipeline '{self.name}' node index {node_index} is out of range."
            )
        return Pipeline(
            name=self.name,
            nodes=self.nodes[: node_index + 1],
            summary=self.summary,
        )
