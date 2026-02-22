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
