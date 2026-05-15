from pathlib import Path

from datapipeline.cli.commands.inflow import (
    StreamSelection,
    _build_mapper_plan,
    _build_stream_plan_from_selection,
    _mapper_menu_options,
    _parser_menu_options,
    _select_new_source_loader,
    _select_parser_plan,
)
from datapipeline.services.scaffold.stream_plan import ParserPlan
from datapipeline.services.scaffold.utils import pick_from_menu


def _base_stream_selection() -> StreamSelection:
    return StreamSelection(
        provider="nasa",
        dataset="weather",
        source_id="nasa.weather",
        create_source=True,
        loader_ep="core.io",
        loader_args={"transport": "http", "format": "json"},
        parser=ParserPlan(create=False, parser_ep="identity"),
        domain="weather",
        create_domain=False,
        mchoice="identity",
        mapper_create_dto=False,
        mapper_input_class=None,
        mapper_input_module=None,
        mapper_name=None,
        mapper_ep=None,
        stream_id="weather.weather",
    )


def test_parser_menu_options_include_existing_when_available() -> None:
    options = _parser_menu_options({"custom.parser": "custom.parser"})
    assert [key for key, _ in options] == [
        "create",
        "existing",
        "temporal_record",
        "identity",
    ]


def test_mapper_menu_options_skip_existing_when_empty() -> None:
    options = _mapper_menu_options({})
    assert [key for key, _ in options] == ["create", "identity"]


def test_pick_from_menu_blank_input_keeps_first_option_as_default(monkeypatch) -> None:
    monkeypatch.setattr("builtins.input", lambda _: "")

    choice = pick_from_menu(
        "Parser:",
        [
            ("identity", "Identity parser (default)"),
            ("temporal_record", "Temporal record rehydration"),
            ("custom", "Custom parser"),
        ],
    )

    assert choice == "identity"


def test_select_new_source_loader_uses_builtin_transport(monkeypatch) -> None:
    choices = iter(["http", "json"])
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.pick_from_menu",
        lambda *args, **kwargs: next(choices),
    )

    loader_ep, loader_args = _select_new_source_loader()

    assert loader_ep == "core.io"
    assert loader_args["transport"] == "http"
    assert loader_args["format"] == "json"


def test_select_new_source_loader_accepts_custom_entrypoint(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.pick_from_menu",
        lambda *args, **kwargs: "custom",
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.prompt_required",
        lambda prompt: "custom.loader",
    )

    loader_ep, loader_args = _select_new_source_loader()

    assert loader_ep == "custom.loader"
    assert loader_args == {}


def test_select_parser_plan_identity_defaults(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.list_parsers",
        lambda root=None: {},
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.pick_from_menu",
        lambda *args, **kwargs: "identity",
    )

    plan = _select_parser_plan(Path("/tmp/plugin"), "example_pkg", "nasa", "weather")

    assert plan.create is False
    assert plan.parser_ep == "identity"


def test_select_parser_plan_temporal_record_defaults(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.list_parsers",
        lambda root=None: {},
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.pick_from_menu",
        lambda *args, **kwargs: "temporal_record",
    )

    plan = _select_parser_plan(Path("/tmp/plugin"), "example_pkg", "nasa", "weather")

    assert plan.create is False
    assert plan.parser_ep == "core.temporal_record"


def test_select_parser_plan_uses_existing_entrypoint(monkeypatch) -> None:
    choices = iter(["existing", "custom.parser"])
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.list_parsers",
        lambda root=None: {"custom.parser": "custom.parser"},
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.pick_from_menu",
        lambda *args, **kwargs: next(choices),
    )

    plan = _select_parser_plan(Path("/tmp/plugin"), "example_pkg", "nasa", "weather")

    assert plan.create is False
    assert plan.parser_ep == "custom.parser"


def test_select_parser_plan_creates_parser_with_dto(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.list_parsers",
        lambda root=None: {},
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.pick_from_menu",
        lambda *args, **kwargs: "create",
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.list_dtos",
        lambda root=None: {},
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.choose_existing_or_create_name",
        lambda **kwargs: ("WeatherDTO", True),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.inflow.choose_name",
        lambda *args, **kwargs: "WeatherDTOParser",
    )

    plan = _select_parser_plan(Path("/tmp/plugin"), "example_pkg", "nasa", "weather")

    assert plan.create is True
    assert plan.create_dto is True
    assert plan.dto_class == "WeatherDTO"
    assert plan.dto_module == "example_pkg.dtos.weather_dto"
    assert plan.parser_name == "WeatherDTOParser"


def test_build_mapper_plan_existing_keeps_domain() -> None:
    selection = _base_stream_selection()
    selection.mchoice = "existing"
    selection.mapper_ep = "custom.mapper"

    plan = _build_mapper_plan(selection)

    assert plan.create is False
    assert plan.mapper_ep == "custom.mapper"
    assert plan.domain == "weather"


def test_build_stream_plan_from_selection_wires_identity_defaults() -> None:
    selection = _base_stream_selection()
    plan = _build_stream_plan_from_selection(
        selection,
        Path("/tmp/project.yaml"),
        Path("/tmp/plugin"),
    )

    assert plan.provider == "nasa"
    assert plan.dataset == "weather"
    assert plan.stream_id == "weather.weather"
    assert plan.parser is not None
    assert plan.parser.parser_ep == "identity"
    assert plan.mapper is not None
    assert plan.mapper.mapper_ep == "identity"
