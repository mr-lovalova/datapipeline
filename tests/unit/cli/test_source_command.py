from types import SimpleNamespace

import pytest

from datapipeline.cli.commands import source
from datapipeline.services.constants import DEFAULT_TEMPORAL_RECORD_PARSER_EP


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

    assert captured["provider"] == "demo"
    assert captured["dataset"] == "weather"
    assert captured["loader_ep"] == "core.io"
    loader_args = captured["loader_args"]
    assert isinstance(loader_args, dict)
    assert loader_args["transport"] == "fs"
    assert loader_args["format"] == "csv"
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

    assert captured["provider"] == "demo"
    assert captured["dataset"] == "weather"
    assert captured["loader_ep"] == "demo.loaders.weather"
    assert captured["loader_args"] == {}
    assert captured["parser_ep"] == "demo.parsers.weather"


def test_source_create_defaults_to_identity_parser_when_noninteractive(monkeypatch) -> None:
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


def test_interactive_parser_menu_can_select_temporal_record(monkeypatch) -> None:
    monkeypatch.setattr(source.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(source, "list_parsers", lambda root=None: {})
    monkeypatch.setattr(
        source,
        "pick_from_menu",
        lambda prompt, options: "temporal_record",
    )

    parser_ep = source._resolve_parser_entrypoint(
        identity=False,
        parser=None,
        plugin_root=None,
    )

    assert parser_ep == DEFAULT_TEMPORAL_RECORD_PARSER_EP
