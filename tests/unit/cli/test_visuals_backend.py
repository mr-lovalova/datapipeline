from contextlib import contextmanager

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
