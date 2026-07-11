import logging
import sys
from pathlib import Path

from datapipeline.cli.visuals.execution import ExecutionMessage
from datapipeline.cli.visuals.execution_context import current_execution_event_sink
from datapipeline.cli.visuals.execution_context import current_dag_depth
from datapipeline.cli.visuals.execution_context import current_terminal_log_proxy_sink
from datapipeline.config.resolution import LogOutputSettings


class _TerminalExecutionEventDedupeFilter(logging.Filter):
    """Suppress execution-event log records while visuals are actively rendering."""

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "dp_event_kind", None) is None:
            return True
        return current_execution_event_sink() is None


class _TerminalVisualProxyHandler(logging.StreamHandler):
    """Route plain logger records into active rich visuals sink when available."""

    def emit(self, record: logging.LogRecord) -> None:
        # Execution-event records are already routed via the context sink.
        if getattr(record, "dp_event_kind", None) is None:
            sink = current_terminal_log_proxy_sink()
            if sink is not None:
                sink.emit(
                    ExecutionMessage(
                        depth=max(0, int(current_dag_depth())),
                        message=self.format(record),
                        log_level=int(record.levelno),
                    )
                )
                return
        super().emit(record)


def configure_root_logging(level: int, output: LogOutputSettings) -> None:
    """Configure root logging handlers for the resolved outputs."""
    handlers: list[logging.Handler] = []
    seen: set[tuple[str, str | None, str]] = set()
    for target in output.outputs:
        transport = target.transport.lower()
        destination = target.destination
        key = (
            transport,
            str(destination) if destination is not None else None,
            str(target.scope).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        handler: logging.Handler
        if transport == "stderr":
            handler = _TerminalVisualProxyHandler(sys.stderr)
            handler.addFilter(_TerminalExecutionEventDedupeFilter())
        elif transport == "stdout":
            handler = _TerminalVisualProxyHandler(sys.stdout)
            handler.addFilter(_TerminalExecutionEventDedupeFilter())
        elif transport == "fs":
            if destination is None:
                raise ValueError("log transport 'fs' requires a destination path")
            path = Path(destination).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path, encoding="utf-8")
        else:
            raise ValueError(f"Unsupported log transport: {target.transport!r}")
        handlers.append(handler)

    logging.basicConfig(
        level=int(level),
        format="%(message)s",
        handlers=handlers,
        force=True,
    )
