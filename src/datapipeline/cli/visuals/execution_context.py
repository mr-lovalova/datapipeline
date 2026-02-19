from contextvars import ContextVar

_CURRENT_DAG_DEPTH: ContextVar[int] = ContextVar(
    "datapipeline_visual_current_dag_depth",
    default=0,
)


def set_current_dag_depth(depth: int) -> None:
    _CURRENT_DAG_DEPTH.set(max(0, int(depth)))


def current_dag_depth() -> int:
    return max(0, int(_CURRENT_DAG_DEPTH.get()))


def current_dag_indent() -> str:
    return "  " * current_dag_depth()
