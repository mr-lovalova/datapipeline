from pathlib import Path

import pytest

from datapipeline.services.scaffold.stream_plan import (
    DomainCreation,
    DomainReference,
    MapperCreation,
    MapperReference,
    ParserCreation,
    ParserReference,
    PythonType,
    SourceCreation,
    SourceReference,
    StreamPlan,
    execute_stream_plan,
)


def _patch_project(monkeypatch, tmp_path: Path) -> Path:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text("[project]\nname = 'example'\n", encoding="utf-8")
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.pkg_root",
        lambda root: (tmp_path, "example", pyproject),
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.status",
        lambda *args: None,
    )
    return pyproject


def test_custom_source_id_is_used_for_source_and_stream(monkeypatch, tmp_path) -> None:
    _patch_project(monkeypatch, tmp_path)
    created_source: dict[str, object] = {}
    created_stream: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_source_yaml",
        lambda **kwargs: created_source.update(kwargs),
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.write_source_stream",
        lambda **kwargs: created_stream.update(kwargs),
    )
    plan = StreamPlan(
        project_yaml=tmp_path / "project.yaml",
        stream_id="weather.hourly",
        root=tmp_path,
        source=SourceCreation(
            source_id="nasa.weather.hourly",
            loader_entrypoint="custom.loader",
            loader_args={},
            parser=ParserReference("identity"),
        ),
        mapper=MapperReference("identity"),
        domain=DomainReference("weather"),
        dto_to_create=None,
    )

    execute_stream_plan(plan)

    assert created_source["source_id"] == "nasa.weather.hourly"
    assert created_stream["source"] == "nasa.weather.hourly"


def test_invalid_source_id_fails_before_project_mutation(monkeypatch, tmp_path) -> None:
    def fail_project_resolution(root):
        raise AssertionError("invalid plans must fail before project resolution")

    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.pkg_root",
        fail_project_resolution,
    )
    plan = StreamPlan(
        project_yaml=tmp_path / "project.yaml",
        stream_id="weather.weather",
        root=tmp_path,
        source=SourceCreation(
            source_id="weather",
            loader_entrypoint="custom.loader",
            loader_args={},
            parser=ParserReference("identity"),
        ),
        mapper=MapperReference("identity"),
        domain=DomainCreation("weather"),
        dto_to_create=None,
    )

    with pytest.raises(ValueError, match="source_id"):
        execute_stream_plan(plan)


def test_creation_plan_executes_exact_declared_components(
    monkeypatch, tmp_path
) -> None:
    _patch_project(monkeypatch, tmp_path)
    order: list[str] = []
    parser_call: dict[str, object] = {}
    source_call: dict[str, object] = {}
    mapper_call: dict[str, object] = {}
    stream_call: dict[str, object] = {}

    def create_domain(**kwargs):
        order.append("domain")

    def create_dto(**kwargs):
        order.append("dto")

    def create_parser(**kwargs):
        order.append("parser")
        parser_call.update(kwargs)
        return "weather_parser"

    def create_source(**kwargs):
        order.append("source")
        source_call.update(kwargs)

    def create_mapper(**kwargs):
        order.append("mapper")
        mapper_call.update(kwargs)
        return "weather_mapper"

    def create_stream(**kwargs):
        order.append("stream")
        stream_call.update(kwargs)

    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_domain",
        create_domain,
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_dto",
        create_dto,
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_parser",
        create_parser,
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_source_yaml",
        create_source,
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_mapper",
        create_mapper,
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.write_source_stream",
        create_stream,
    )
    dto = PythonType("WeatherDTO", "example.dtos.weather_dto")
    plan = StreamPlan(
        project_yaml=tmp_path / "project.yaml",
        stream_id="weather.weather",
        root=tmp_path,
        source=SourceCreation(
            source_id="nasa.weather",
            loader_entrypoint="core.io",
            loader_args={"transport": "http"},
            parser=ParserCreation("WeatherParser", dto),
        ),
        mapper=MapperCreation(
            "map_weather_to_domain",
            PythonType(dto.class_name, dto.module),
        ),
        domain=DomainCreation("weather"),
        dto_to_create="WeatherDTO",
    )

    execute_stream_plan(plan)

    assert order == [
        "domain",
        "dto",
        "parser",
        "mapper",
        "source",
        "stream",
    ]
    assert parser_call["dto_module"] == "example.dtos.weather_dto"
    assert source_call["parser_ep"] == "weather_parser"
    assert mapper_call["input_module"] == "example.dtos.weather_dto"
    assert stream_call["mapper_entrypoint"] == "weather_mapper"


def test_reference_plan_only_writes_stream(monkeypatch, tmp_path) -> None:
    _patch_project(monkeypatch, tmp_path)

    def fail_source_creation(**kwargs):
        raise AssertionError("source must be reused")

    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_source_yaml",
        fail_source_creation,
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.write_source_stream",
        lambda **kwargs: captured.update(kwargs),
    )
    plan = StreamPlan(
        project_yaml=tmp_path / "project.yaml",
        stream_id="weather.weather",
        root=tmp_path,
        source=SourceReference("prices"),
        mapper=MapperReference("custom.mapper"),
        domain=DomainReference("weather"),
        dto_to_create=None,
    )

    execute_stream_plan(plan)

    assert captured["source"] == "prices"
    assert captured["mapper_entrypoint"] == "custom.mapper"


def test_dto_creation_is_a_single_explicit_plan_action(monkeypatch, tmp_path) -> None:
    _patch_project(monkeypatch, tmp_path)
    created_dto: dict[str, object] = {}
    mapper_call: dict[str, object] = {}

    def create_mapper(**kwargs):
        mapper_call.update(kwargs)
        return "weather_mapper"

    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_dto",
        lambda **kwargs: created_dto.update(kwargs),
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.create_mapper",
        create_mapper,
    )
    monkeypatch.setattr(
        "datapipeline.services.scaffold.stream_plan.write_source_stream",
        lambda **kwargs: None,
    )
    plan = StreamPlan(
        project_yaml=tmp_path / "project.yaml",
        stream_id="weather.weather",
        root=tmp_path,
        source=SourceReference("nasa.weather"),
        mapper=MapperCreation(
            "map_weather_to_domain",
            PythonType("WeatherDTO", "example.dtos.weather_dto"),
        ),
        domain=DomainReference("weather"),
        dto_to_create="WeatherDTO",
    )

    execute_stream_plan(plan)

    assert created_dto["name"] == "WeatherDTO"
    assert mapper_call["input_module"] == "example.dtos.weather_dto"
