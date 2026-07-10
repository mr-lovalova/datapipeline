from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping

NodeInput = Iterable[Any] | None
NodeOutput = Iterable[Any]
NodeOp = Callable[..., NodeOutput]
NodeKind = Literal["function", "dag_call"]


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

    def __post_init__(self) -> None:
        if self.kind not in {"function", "dag_call"}:
            raise ValueError(f"Node '{self.name}' has unsupported kind '{self.kind}'.")
        if self.kind == "dag_call" and not self.calls_dag:
            raise ValueError(f"Node '{self.name}' kind='dag_call' requires calls_dag.")
        if self.kind == "function" and self.calls_dag is not None:
            raise ValueError(
                f"Node '{self.name}' kind='function' cannot set calls_dag."
            )
