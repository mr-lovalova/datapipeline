from pathlib import Path

import pytest
from tomlkit.exceptions import ParseError

from datapipeline.services.scaffold.loader import create_loader
from datapipeline.services.scaffold.mapper import create_mapper
from datapipeline.services.scaffold.parser import create_parser


def _plugin_with_invalid_pyproject(tmp_path: Path) -> Path:
    plugin = tmp_path / "plugin"
    package = plugin / "src" / "plugin"
    package.mkdir(parents=True)
    (package / "__init__.py").touch()
    (plugin / "pyproject.toml").write_text("[project\n", encoding="utf-8")
    return plugin


def _valid_plugin(tmp_path: Path) -> Path:
    plugin = tmp_path / "plugin"
    package = plugin / "src" / "plugin"
    package.mkdir(parents=True)
    (package / "__init__.py").touch()
    (plugin / "pyproject.toml").write_text(
        "[project]\nname = 'plugin'\n",
        encoding="utf-8",
    )
    return plugin


def test_loader_validates_pyproject_before_creating_files(tmp_path: Path) -> None:
    plugin = _plugin_with_invalid_pyproject(tmp_path)

    with pytest.raises(ParseError):
        create_loader(name="WeatherLoader", root=plugin)

    assert not (plugin / "src" / "plugin" / "loaders").exists()


def test_parser_validates_pyproject_before_creating_files(tmp_path: Path) -> None:
    plugin = _plugin_with_invalid_pyproject(tmp_path)

    with pytest.raises(ParseError):
        create_parser(
            name="WeatherParser",
            dto_class="WeatherDTO",
            dto_module="plugin.dtos.weather",
            root=plugin,
        )

    assert not (plugin / "src" / "plugin" / "parsers").exists()


def test_mapper_validates_pyproject_before_creating_files(tmp_path: Path) -> None:
    plugin = _plugin_with_invalid_pyproject(tmp_path)

    with pytest.raises(ParseError):
        create_mapper(
            name="map_weather",
            input_class="WeatherDTO",
            input_module="plugin.dtos.weather",
            domain="weather",
            root=plugin,
        )

    assert not (plugin / "src" / "plugin" / "mappers").exists()


def test_loader_rejects_python_keyword_name(tmp_path: Path) -> None:
    plugin = tmp_path / "plugin"
    package = plugin / "src" / "plugin"
    package.mkdir(parents=True)
    (package / "__init__.py").touch()
    (plugin / "pyproject.toml").write_text(
        "[project]\nname = 'plugin'\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="valid Python identifier"):
        create_loader(name="class", root=plugin)

    assert not (package / "loaders").exists()


def test_loader_removes_created_files_when_registration_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin = _valid_plugin(tmp_path)
    pyproject = plugin / "pyproject.toml"
    original = pyproject.read_bytes()

    def fail_write(*args, **kwargs):
        raise OSError("write failed")

    monkeypatch.setattr(
        "datapipeline.services.scaffold.entrypoints._write_document",
        fail_write,
    )

    with pytest.raises(OSError, match="write failed"):
        create_loader(name="WeatherLoader", root=plugin)

    assert not (plugin / "src" / "plugin" / "loaders").exists()
    assert pyproject.read_bytes() == original
