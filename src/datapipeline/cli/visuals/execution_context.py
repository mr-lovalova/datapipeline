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


def visible_dag_depth(level: int) -> int:
    depth = current_dag_depth()
    session_level = current_visual_log_level()
    if session_level <= logging.DEBUG:
        return depth
    # Keep INFO logs compact: show only top-level nesting.
    if level <= logging.DEBUG:
        return depth
    return min(depth, 1)


def visible_dag_indent(level: int) -> str:
    return "  " * visible_dag_depth(level)
