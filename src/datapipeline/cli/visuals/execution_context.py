from contextvars import ContextVar
import logging
from typing import Any

_CURRENT_DAG_DEPTH: ContextVar[int] = ContextVar(
    "datapipeline_visual_current_dag_depth",
    default=0,
)
_CURRENT_DAG_LABEL: ContextVar[str | None] = ContextVar(
    "datapipeline_visual_current_dag_label",
    default=None,
)
_CURRENT_VISUAL_LOG_LEVEL: ContextVar[int] = ContextVar(
    "datapipeline_visual_current_log_level",
    default=logging.INFO,
)
_CURRENT_EXECUTION_EVENT_SINK: ContextVar[Any | None] = ContextVar(
    "datapipeline_visual_current_execution_event_sink",
    default=None,
)
_CURRENT_TERMINAL_LOG_PROXY_SINK: ContextVar[Any | None] = ContextVar(
    "datapipeline_visual_current_terminal_log_proxy_sink",
    default=None,
)
_CURRENT_EXECUTION_SCOPE: ContextVar[dict[str, str] | None] = ContextVar(
    "datapipeline_visual_current_execution_scope",
    default=None,
)
_CURRENT_SOURCE_VISUAL_PROXY_FACTORY: ContextVar[Any | None] = ContextVar(
    "datapipeline_visual_current_source_visual_proxy_factory",
    default=None,
)


def set_current_dag_depth(depth: int) -> None:
    next_depth = max(0, int(depth))
    _CURRENT_DAG_DEPTH.set(next_depth)
    if next_depth == 0:
        _CURRENT_DAG_LABEL.set(None)


def current_dag_depth() -> int:
    return max(0, int(_CURRENT_DAG_DEPTH.get()))


def current_source_depth() -> int:
    from datapipeline.dag.runner import current_node_progress_context

    node = current_node_progress_context()
    if node is not None:
        return max(0, int(node.depth) - 1)
    return current_dag_depth()


def current_dag_indent() -> str:
    return "  " * current_dag_depth()


def set_current_dag_label(label: str | None):
    return _CURRENT_DAG_LABEL.set(label)


def current_dag_label() -> str | None:
    return _CURRENT_DAG_LABEL.get()


def set_current_visual_log_level(level: int):
    return _CURRENT_VISUAL_LOG_LEVEL.set(int(level))


def reset_current_visual_log_level(token) -> None:
    _CURRENT_VISUAL_LOG_LEVEL.reset(token)


def current_visual_log_level() -> int:
    return int(_CURRENT_VISUAL_LOG_LEVEL.get())


def set_current_execution_event_sink(sink: Any | None):
    return _CURRENT_EXECUTION_EVENT_SINK.set(sink)


def reset_current_execution_event_sink(token) -> None:
    _CURRENT_EXECUTION_EVENT_SINK.reset(token)


def current_execution_event_sink() -> Any | None:
    return _CURRENT_EXECUTION_EVENT_SINK.get()


def set_current_terminal_log_proxy_sink(sink: Any | None):
    return _CURRENT_TERMINAL_LOG_PROXY_SINK.set(sink)


def reset_current_terminal_log_proxy_sink(token) -> None:
    _CURRENT_TERMINAL_LOG_PROXY_SINK.reset(token)


def current_terminal_log_proxy_sink() -> Any | None:
    return _CURRENT_TERMINAL_LOG_PROXY_SINK.get()


def set_current_execution_scope(scope: dict[str, str] | None):
    return _CURRENT_EXECUTION_SCOPE.set(scope)


def reset_current_execution_scope(token) -> None:
    _CURRENT_EXECUTION_SCOPE.reset(token)


def current_execution_scope() -> dict[str, str] | None:
    return _CURRENT_EXECUTION_SCOPE.get()


def set_current_source_visual_proxy_factory(factory: Any | None):
    return _CURRENT_SOURCE_VISUAL_PROXY_FACTORY.set(factory)


def reset_current_source_visual_proxy_factory(token) -> None:
    _CURRENT_SOURCE_VISUAL_PROXY_FACTORY.reset(token)


def current_source_visual_proxy_factory() -> Any | None:
    return _CURRENT_SOURCE_VISUAL_PROXY_FACTORY.get()


def visible_dag_depth(level: int) -> int:
    _ = level
    return current_dag_depth()


def visible_dag_indent(level: int) -> str:
    return "  " * visible_dag_depth(level)


def visible_source_indent(level: int) -> str:
    _ = level
    return "  " * current_source_depth()
