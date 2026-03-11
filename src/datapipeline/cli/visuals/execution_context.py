from contextvars import ContextVar
import logging
from typing import Any

_CURRENT_DAG_DEPTH: ContextVar[int] = ContextVar(
    "datapipeline_visual_current_dag_depth",
    default=0,
)
_CURRENT_VISUAL_LOG_LEVEL: ContextVar[int] = ContextVar(
    "datapipeline_visual_current_log_level",
    default=logging.INFO,
)
_CURRENT_EXECUTION_EVENT_SINK: ContextVar[Any | None] = ContextVar(
    "datapipeline_visual_current_execution_event_sink",
    default=None,
)
_CURRENT_EXECUTION_SCOPE: ContextVar[dict[str, str] | None] = ContextVar(
    "datapipeline_visual_current_execution_scope",
    default=None,
)


def set_current_dag_depth(depth: int) -> None:
    _CURRENT_DAG_DEPTH.set(max(0, int(depth)))


def current_dag_depth() -> int:
    return max(0, int(_CURRENT_DAG_DEPTH.get()))


def current_dag_indent() -> str:
    return "  " * current_dag_depth()


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


def set_current_execution_scope(scope: dict[str, str] | None):
    return _CURRENT_EXECUTION_SCOPE.set(scope)


def reset_current_execution_scope(token) -> None:
    _CURRENT_EXECUTION_SCOPE.reset(token)


def current_execution_scope() -> dict[str, str] | None:
    return _CURRENT_EXECUTION_SCOPE.get()


def visible_dag_depth(level: int) -> int:
    _ = level
    return current_dag_depth()


def visible_dag_indent(level: int) -> str:
    return "  " * visible_dag_depth(level)
