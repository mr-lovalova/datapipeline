from contextlib import contextmanager
import logging

from datapipeline.cli.visuals.streams import (
    _BasicBackend,
    _OffBackend,
    _RichBackend,
    get_visuals_backend,
)


def test_get_visuals_backend_selects_rich_only_when_live_supported(monkeypatch):
    monkeypatch.setattr("datapipeline.cli.visuals.streams._rich_available", lambda: True)
    monkeypatch.setattr("datapipeline.cli.visuals.streams._is_tty", lambda: True)
    monkeypatch.setattr("datapipeline.cli.visuals.streams._rich_live_supported", lambda: True)

    backend = get_visuals_backend("on")
    assert isinstance(backend, _RichBackend)


def test_get_visuals_backend_falls_back_to_basic_when_live_not_supported(monkeypatch):
    monkeypatch.setattr("datapipeline.cli.visuals.streams._rich_available", lambda: True)
    monkeypatch.setattr("datapipeline.cli.visuals.streams._is_tty", lambda: True)
    monkeypatch.setattr("datapipeline.cli.visuals.streams._rich_live_supported", lambda: False)

    backend = get_visuals_backend("on")
    assert isinstance(backend, _BasicBackend)


def test_get_visuals_backend_off_mode(monkeypatch):
    monkeypatch.setattr("datapipeline.cli.visuals.streams._rich_available", lambda: True)
    monkeypatch.setattr("datapipeline.cli.visuals.streams._is_tty", lambda: True)
    monkeypatch.setattr("datapipeline.cli.visuals.streams._rich_live_supported", lambda: True)

    backend = get_visuals_backend("off")
    assert isinstance(backend, _OffBackend)


def test_off_backend_wraps_sources_with_basic_logging(monkeypatch):
    calls = {"count": 0, "runtime": None, "level": None}

    @contextmanager
    def _fake_basic(runtime, log_level):
        calls["count"] += 1
        calls["runtime"] = runtime
        calls["level"] = log_level
        yield

    monkeypatch.setattr(
        "datapipeline.cli.visuals.streams_basic.visual_sources",
        _fake_basic,
    )

    backend = _OffBackend()
    runtime = object()
    with backend.wrap_sources(runtime, 20):
        pass

    assert calls["count"] == 1
    assert calls["runtime"] is runtime
    assert calls["level"] == 20


def test_rich_backend_wraps_sources_when_level_is_info_or_debug(monkeypatch):
    calls = {"count": 0, "runtime": None, "level": None}

    @contextmanager
    def _fake_rich(runtime, log_level):
        calls["count"] += 1
        calls["runtime"] = runtime
        calls["level"] = log_level
        yield

    monkeypatch.setattr(
        "datapipeline.cli.visuals.streams_rich.visual_sources",
        _fake_rich,
    )

    backend = _RichBackend()
    runtime = object()
    with backend.wrap_sources(runtime, logging.INFO):
        pass
    assert calls["count"] == 1
    assert calls["runtime"] is runtime
    assert calls["level"] == logging.INFO

    with backend.wrap_sources(runtime, logging.DEBUG):
        pass
    assert calls["count"] == 2
    assert calls["runtime"] is runtime
    assert calls["level"] == logging.DEBUG


def test_rich_backend_suppresses_source_visuals_at_warning_or_higher(monkeypatch):
    calls = {"count": 0}

    @contextmanager
    def _fake_rich(runtime, log_level):
        calls["count"] += 1
        yield

    monkeypatch.setattr(
        "datapipeline.cli.visuals.streams_rich.visual_sources",
        _fake_rich,
    )

    backend = _RichBackend()
    with backend.wrap_sources(object(), logging.WARNING):
        pass
    with backend.wrap_sources(object(), logging.ERROR):
        pass

    assert calls["count"] == 0
