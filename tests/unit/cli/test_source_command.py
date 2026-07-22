from types import SimpleNamespace

import pytest

from datapipeline.cli.commands import source
from datapipeline.services.scaffold.source_yaml import (
    DEFAULT_TEMPORAL_RECORD_PARSER_EP,
)


def test_source_create_resolves_alias_and_builtin_loader(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_source_yaml(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(source, "create_source_yaml", fake_create_source_yaml)

    source.handle(
        subcmd="create",
        provider=None,
        dataset=None,
        alias="demo.weather",
        transport="fs",
        format="csv",
        parser="identity",
    )

    assert captured["source_id"] == "demo.weather"
    loader = captured["loader"]
    assert isinstance(loader, dict)
    assert loader["transport"] == "fs"
    reader = loader["reader"]
    assert isinstance(reader, dict)
    assert reader["format"] == "csv"
    assert captured["parser_ep"] == "identity"


def test_source_create_explicit_loader_overrides_transport(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_source_yaml(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(source, "create_source_yaml", fake_create_source_yaml)

    source.handle(
        subcmd="create",
        provider="demo.weather",
        dataset=None,
        transport="http",
        format="json",
        loader="demo.loaders.weather",
        parser="demo.parsers.weather",
    )

    assert captured["source_id"] == "demo.weather"
    assert captured["loader"] == {
        "entrypoint": "demo.loaders.weather",
        "args": {},
    }
    assert captured["parser_ep"] == "demo.parsers.weather"


def test_source_create_defaults_to_identity_parser_when_noninteractive(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_create_source_yaml(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(source, "create_source_yaml", fake_create_source_yaml)
    monkeypatch.setattr(source.sys, "stdin", SimpleNamespace(isatty=lambda: False))

    source.handle(
        subcmd="create",
        provider="demo",
        dataset="ticks",
        transport="synthetic",
    )

    assert captured["parser_ep"] == "identity"


def test_source_create_rejects_invalid_alias() -> None:
    with pytest.raises(SystemExit):
        source.handle(
            subcmd="create",
            provider=None,
            dataset=None,
            alias="weather",
            transport="synthetic",
            parser="identity",
        )


def test_source_create_rejects_unsafe_source_id_before_loader_selection() -> None:
    with pytest.raises(SystemExit) as exc:
        source.handle(
            subcmd="create",
            provider="demo/vendor",
            dataset="weather",
            transport="synthetic",
            parser="identity",
        )

    assert exc.value.code == 2


def test_source_create_rejects_pickle_format(caplog) -> None:
    with pytest.raises(SystemExit) as exc:
        source.handle(
            subcmd="create",
            provider="demo",
            dataset="weather",
            transport="fs",
            format="pickle",
            parser="identity",
        )

    assert exc.value.code == 2
    assert "Unsupported source format: 'pickle'" in caplog.text


def test_source_create_rejects_http_parquet(caplog) -> None:
    with pytest.raises(SystemExit) as exc:
        source.handle(
            subcmd="create",
            provider="demo",
            dataset="weather",
            transport="http",
            format="parquet",
            parser="identity",
        )

    assert exc.value.code == 2
    assert "HTTP sources do not support parquet" in caplog.text


def test_interactive_parser_menu_can_select_temporal_record(monkeypatch) -> None:
    monkeypatch.setattr(source.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(source, "list_parsers", lambda root=None: {})
    monkeypatch.setattr(
        source,
        "pick_from_menu",
        lambda prompt, options: "temporal_record",
    )

    parser_ep = source._resolve_parser_entrypoint(False, None, None)

    assert parser_ep == DEFAULT_TEMPORAL_RECORD_PARSER_EP
