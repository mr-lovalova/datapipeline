from contextvars import ContextVar
from typing import Any

_CURRENT_EXECUTION_EVENT_SINK: ContextVar[Any | None] = ContextVar(
    "datapipeline_visual_current_execution_event_sink",
    default=None,
)
_CURRENT_TERMINAL_LOG_PROXY_SINK: ContextVar[Any | None] = ContextVar(
    "datapipeline_visual_current_terminal_log_proxy_sink",
    default=None,
)


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
