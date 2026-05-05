from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping

NodeInput = Iterable[Any] | None
NodeOutput = Iterable[Any]
NodeOp = Callable[..., NodeOutput]
NodeKind = Literal["function", "dag_call", "dag_fanout"]


@dataclass(frozen=True)
class PipelineNode:
    name: str
    op: NodeOp
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] | None = None
    kwinputs: Mapping[str, str] | None = None
    input: str | None = None
    output: str | None = None
    kind: NodeKind = "function"
    calls_dag: str | None = None
    child_dags: tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        if self.kind in {"dag_call", "dag_fanout"} and not self.calls_dag:
            raise ValueError(
                f"Node '{self.name}' kind='{self.kind}' requires calls_dag."
            )
        if self.kind == "function" and self.calls_dag is not None:
            raise ValueError(
                f"Node '{self.name}' kind='function' cannot set calls_dag."
            )
        if self.kind == "function" and self.child_dags:
            raise ValueError(
                f"Node '{self.name}' kind='function' cannot set child_dags."
            )
