from contextlib import contextmanager
import logging

from datapipeline.cli.visuals.backend import (
    _RichBackend,
    VisualsBackend,
    get_visuals_backend,
)


def test_get_visuals_backend_selects_rich_only_when_live_supported(monkeypatch):
    monkeypatch.setattr("datapipeline.cli.visuals.backend._is_tty", lambda: True)
    monkeypatch.setattr(
        "datapipeline.cli.visuals.backend._rich_live_supported", lambda: True
    )

    backend = get_visuals_backend("on")
    assert isinstance(backend, _RichBackend)


def test_get_visuals_backend_falls_back_to_basic_when_live_not_supported(monkeypatch):
    monkeypatch.setattr("datapipeline.cli.visuals.backend._is_tty", lambda: True)
    monkeypatch.setattr(
        "datapipeline.cli.visuals.backend._rich_live_supported", lambda: False
    )

    backend = get_visuals_backend("on")
    assert type(backend) is VisualsBackend


def test_get_visuals_backend_off_mode(monkeypatch):
    monkeypatch.setattr("datapipeline.cli.visuals.backend._is_tty", lambda: True)
    monkeypatch.setattr(
        "datapipeline.cli.visuals.backend._rich_live_supported", lambda: True
    )

    backend = get_visuals_backend("off")
    assert type(backend) is VisualsBackend


def test_off_backend_wrap_execution_is_noop():
    backend = get_visuals_backend("off")
    with backend.wrap_execution(logging.INFO):
        pass


def test_rich_backend_wraps_execution_at_info_or_debug(monkeypatch):
    calls: list[int] = []

    class _Context:
        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_rich(log_level):
        calls.append(log_level)
        return _Context()

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.progress.visual_execution", _fake_rich
    )

    backend = _RichBackend()
    with backend.wrap_execution(logging.INFO):
        pass
    with backend.wrap_execution(logging.DEBUG):
        pass

    assert calls == [logging.INFO, logging.DEBUG]


def test_rich_backend_keeps_visuals_independent_of_log_level(monkeypatch):
    calls: list[int] = []

    @contextmanager
    def _fake_rich(log_level):
        calls.append(log_level)
        yield

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.progress.visual_execution",
        _fake_rich,
    )

    backend = _RichBackend()
    with backend.wrap_execution(logging.WARNING):
        pass
    with backend.wrap_execution(logging.ERROR):
        pass

    assert calls == [logging.WARNING, logging.ERROR]
