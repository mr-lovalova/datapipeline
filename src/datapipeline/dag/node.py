from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping

StepInput = Iterable[Any] | None
StepOutput = Iterable[Any]
StepOp = Callable[..., StepOutput]
StepKind = Literal["function", "dag_call"]


@dataclass(frozen=True)
class PipelineStep:
    name: str
    op: StepOp
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] | None = None
    input: str | None = None
    output: str | None = None
    kind: StepKind = "function"
    calls_dag: str | None = None

    def __post_init__(self) -> None:
        if self.kind == "dag_call" and not self.calls_dag:
            raise ValueError(
                f"Step '{self.name}' kind='dag_call' requires calls_dag."
            )
        if self.kind == "function" and self.calls_dag is not None:
            raise ValueError(
                f"Step '{self.name}' kind='function' cannot set calls_dag."
            )
