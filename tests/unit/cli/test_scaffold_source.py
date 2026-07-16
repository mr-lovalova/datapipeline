from pathlib import Path

import pytest
import yaml

from datapipeline.services.scaffold.source_yaml import (
    create_source_yaml,
    default_loader_config,
)


def _create_plugin(tmp_path: Path) -> Path:
    root = tmp_path / "sample_plugin"
    pkg_dir = root / "src" / "sample_plugin"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        '[project]\nname = "sample-plugin"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    return root


def test_create_source_scaffolds_into_default_dataset(tmp_path: Path) -> None:
    plugin_root = _create_plugin(tmp_path)

    loader_ep, loader_args = default_loader_config("fs", "csv")
    assert "glob" not in loader_args
    create_source_yaml(
        source_id="demo.weather",
        loader_ep=loader_ep,
        loader_args=loader_args,
        parser_ep="identity",
        root=plugin_root,
    )

    expected = plugin_root / "your-dataset" / "sources" / "demo.weather.yaml"
    assert expected.exists(), f"expected scaffolded source at {expected}"


def test_create_source_preserves_custom_source_id(tmp_path: Path) -> None:
    plugin_root = _create_plugin(tmp_path)

    create_source_yaml(
        source_id="demo.weather.hourly",
        loader_ep="custom.loader",
        loader_args={},
        parser_ep="identity",
        root=plugin_root,
    )

    path = plugin_root / "your-dataset" / "sources" / "demo.weather.hourly.yaml"
    assert yaml.safe_load(path.read_text(encoding="utf-8"))["id"] == (
        "demo.weather.hourly"
    )


def test_default_loader_config_rejects_unknown_transport() -> None:
    with pytest.raises(ValueError, match="Unsupported source transport"):
        default_loader_config("unknown", None)


def test_create_source_refuses_to_replace_existing_config(tmp_path: Path) -> None:
    plugin_root = _create_plugin(tmp_path)
    path = create_source_yaml(
        source_id="demo.weather",
        loader_ep="custom.loader",
        loader_args={},
        parser_ep="identity",
        root=plugin_root,
    )
    original = path.read_bytes()

    with pytest.raises(FileExistsError, match="demo.weather.yaml"):
        create_source_yaml(
            source_id="demo.weather",
            loader_ep="different.loader",
            loader_args={"changed": True},
            parser_ep="custom.parser",
            root=plugin_root,
        )

    assert path.read_bytes() == original


@pytest.mark.parametrize(
    "source_id",
    ["weather", "demo/weather", "demo..weather", "demo.café"],
)
def test_create_source_rejects_unsafe_source_id(
    source_id: str,
    tmp_path: Path,
) -> None:
    plugin_root = _create_plugin(tmp_path)

    with pytest.raises(ValueError, match="source_id"):
        create_source_yaml(
            source_id=source_id,
            loader_ep="custom.loader",
            loader_args={},
            parser_ep="identity",
            root=plugin_root,
        )
