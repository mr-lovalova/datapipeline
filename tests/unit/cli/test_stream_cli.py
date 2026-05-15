from pathlib import Path

from datapipeline.cli.commands.inflow import (
    StreamSelection,
    _build_mapper_plan,
    _build_parser_plan,
    _build_stream_plan_from_selection,
    _mapper_menu_options,
    _parser_menu_options,
)
from datapipeline.services.scaffold.utils import pick_from_menu


def _base_stream_selection() -> StreamSelection:
    return StreamSelection(
        provider="nasa",
        dataset="weather",
        source_id="nasa.weather",
        create_source=True,
        loader_ep="core.io",
        loader_args={"transport": "http", "format": "json"},
        pchoice="identity",
        parser_create_dto=False,
        dto_class=None,
        dto_module=None,
        parser_name=None,
        parser_ep=None,
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


def test_build_parser_plan_identity_defaults() -> None:
    plan = _build_parser_plan(_base_stream_selection())
    assert plan.create is False
    assert plan.parser_ep == "identity"


def test_build_parser_plan_temporal_record_defaults() -> None:
    selection = _base_stream_selection()
    selection.pchoice = "temporal_record"

    plan = _build_parser_plan(selection)

    assert plan.create is False
    assert plan.parser_ep == "core.temporal_record"


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
