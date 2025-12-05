from __future__ import annotations

from pathlib import Path

from datapipeline.services.scaffold.source import create_source


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


def test_create_source_scaffolds_into_example_dataset(tmp_path: Path) -> None:
    """create_source should resolve project.yaml within the plugin and scaffold into its sources path."""
    plugin_root = _create_plugin(tmp_path)

    create_source(
        provider="demo",
        dataset="weather",
        transport="fs",
        format="csv",
        root=plugin_root,
        identity=False,
    )

    expected = plugin_root / "example" / "sources" / "demo.weather.yaml"
    assert expected.exists(), f"expected scaffolded source at {expected}"

