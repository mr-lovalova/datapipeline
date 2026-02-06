from pathlib import Path

from datapipeline.cli.commands.stream import (
    StreamSelection,
    _build_mapper_plan,
    _build_parser_plan,
    _build_stream_plan_from_selection,
    _mapper_menu_options,
    _parser_menu_options,
)


def test_parser_menu_options_include_existing_when_available() -> None:
    options = _parser_menu_options({"custom.parser": "custom.parser"})
    assert [key for key, _ in options] == ["create", "existing", "identity"]


def test_mapper_menu_options_skip_existing_when_empty() -> None:
    options = _mapper_menu_options({})
    assert [key for key, _ in options] == ["create", "identity"]


def test_build_parser_plan_identity_defaults() -> None:
    plan = _build_parser_plan(
        choice="identity",
        create_dto=False,
        dto_class=None,
        dto_module=None,
        parser_name=None,
        parser_ep=None,
    )
    assert plan.create is False
    assert plan.parser_ep == "identity"


def test_build_mapper_plan_existing_keeps_domain() -> None:
    plan = _build_mapper_plan(
        choice="existing",
        create_dto=False,
        input_class=None,
        input_module=None,
        mapper_name=None,
        mapper_ep="custom.mapper",
        domain="weather",
    )
    assert plan.create is False
    assert plan.mapper_ep == "custom.mapper"
    assert plan.domain == "weather"


def test_build_stream_plan_from_selection_wires_identity_defaults() -> None:
    selection = StreamSelection(
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

    plan = _build_stream_plan_from_selection(
        selection=selection,
        project_yaml=Path("/tmp/project.yaml"),
        plugin_root=Path("/tmp/plugin"),
    )

    assert plan.provider == "nasa"
    assert plan.dataset == "weather"
    assert plan.stream_id == "weather.weather"
    assert plan.parser is not None
    assert plan.parser.parser_ep == "identity"
    assert plan.mapper is not None
    assert plan.mapper.mapper_ep == "identity"
