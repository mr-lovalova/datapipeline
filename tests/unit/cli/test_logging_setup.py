import io
import logging
import sys

from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
)
from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget


def _flush_root_handlers() -> None:
    root = logging.getLogger()
    for handler in root.handlers:
        try:
            handler.flush()
        except Exception:
            pass


def test_configure_root_logging_suppresses_execution_events_on_stderr_when_visual_sink_active(
    monkeypatch,
):
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    configure_root_logging(
        level=logging.INFO,
        output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
    )

    logger = logging.getLogger("datapipeline.tests.logging_setup.stderr")
    token = set_current_execution_event_sink(object())
    try:
        logger.info("DAG started name=demo", extra={"dp_event_kind": "dag_start"})
        logger.info("plain log line")
    finally:
        reset_current_execution_event_sink(token)
        _flush_root_handlers()

    rendered = stream.getvalue()
    assert "plain log line" in rendered
    assert "DAG started name=demo" not in rendered


def test_configure_root_logging_keeps_execution_events_in_file_when_visual_sink_active(
    tmp_path,
):
    log_path = tmp_path / "logs" / "app.log"
    configure_root_logging(
        level=logging.INFO,
        output=LogOutputSettings(
            outputs=(LogOutputTarget(transport="fs", destination=log_path),)
        ),
    )

    logger = logging.getLogger("datapipeline.tests.logging_setup.file")
    token = set_current_execution_event_sink(object())
    try:
        logger.info("DAG started name=demo", extra={"dp_event_kind": "dag_start"})
    finally:
        reset_current_execution_event_sink(token)
        _flush_root_handlers()

    content = log_path.read_text(encoding="utf-8")
    assert "DAG started name=demo" in content


def test_configure_root_logging_proxies_plain_terminal_logs_into_rich_visual_sink(
    monkeypatch,
):
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    configure_root_logging(
        level=logging.INFO,
        output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
    )

    class _RichSink:
        def __init__(self) -> None:
            self.events = []

        def set_live_console(self, _console) -> None:
            return None

        def emit(self, event) -> None:
            self.events.append(event)

    sink = _RichSink()
    logger = logging.getLogger("datapipeline.tests.logging_setup.rich_proxy")
    sink_token = set_current_execution_event_sink(sink)
    proxy_token = set_current_terminal_log_proxy_sink(sink)
    try:
        logger.warning("plain log line")
    finally:
        reset_current_terminal_log_proxy_sink(proxy_token)
        reset_current_execution_event_sink(sink_token)
        _flush_root_handlers()

    rendered = stream.getvalue()
    assert "plain log line" not in rendered
    assert len(sink.events) == 1
    assert sink.events[0].kind == "message"
    assert sink.events[0].message == "plain log line"
    assert sink.events[0].log_level == logging.WARNING


def test_configure_root_logging_does_not_proxy_plain_logs_without_proxy_sink(
    monkeypatch,
):
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stream)
    configure_root_logging(
        level=logging.INFO,
        output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
    )

    logger = logging.getLogger("datapipeline.tests.logging_setup.no_proxy")
    token = set_current_execution_event_sink(object())
    try:
        logger.warning("plain log line")
    finally:
        reset_current_execution_event_sink(token)
        _flush_root_handlers()

    rendered = stream.getvalue()
    assert "plain log line" in rendered
