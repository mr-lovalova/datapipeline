from pathlib import Path
import textwrap

import pytest
import yaml

from datapipeline.cli.commands.stream import handle as handle_stream_create


def _create_plugin(tmp_path: Path) -> Path:
    root = tmp_path / "plugin"
    pkg_dir = root / "src" / "sample_plugin"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    pyproject = textwrap.dedent(
        """
        [project]
        name = "sample-plugin"
        version = "0.1.0"
        """
    ).strip()
    (root / "pyproject.toml").write_text(pyproject + "\n", encoding="utf-8")

    domain_dir = pkg_dir / "domains" / "weather"
    domain_dir.mkdir(parents=True, exist_ok=True)
    (domain_dir / "__init__.py").write_text("", encoding="utf-8")
    (domain_dir / "model.py").write_text(
        "class WeatherRecord:\n    pass\n", encoding="utf-8"
    )
    return root


def _write_project_yaml(
    project_path: Path, sources_dir: Path, streams_dir: Path
) -> None:
    project_path.parent.mkdir(parents=True, exist_ok=True)
    ingests_dir = project_path.parent / "ingests"
    content = textwrap.dedent(
        f"""
        version: 1
        paths:
          ingests: {ingests_dir}
          streams: {streams_dir}
          sources: {sources_dir}
          dataset: dataset.yaml
          postprocess: postprocess.yaml
          artifacts: {streams_dir.parent / "build"}
          build: builds
          run: runs
        globals: {{}}
        """
    ).strip()
    project_path.write_text(content + "\n", encoding="utf-8")


def _write_source_yaml(path: Path, alias: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = textwrap.dedent(
        f"""
        id: {alias}
        parser:
          entrypoint: identity
          args: {{}}
        loader:
          entrypoint: identity
          args: {{}}
        """
    ).strip()
    path.write_text(content + "\n", encoding="utf-8")


def _write_ingest_yaml(path: Path, stream_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        textwrap.dedent(
            f"""
            id: {stream_id}
            from:
              source: demo.weather
            map:
              entrypoint: identity
              args: {{}}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )


def _input_sequence(monkeypatch: pytest.MonkeyPatch, responses: list[str]) -> None:
    iterator = iter(responses)

    def _fake_input(_: str = "") -> str:
        return next(iterator)

    monkeypatch.setattr("builtins.input", _fake_input)


def test_stream_scaffold_uses_project_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Stream scaffold should use the project's streams/sources paths inside the plugin repo."""
    plugin_root = _create_plugin(tmp_path)
    # Create a project.yaml under example/ with explicit streams/sources paths
    example_root = plugin_root / "example"
    sources_dir = example_root / "sources"
    ingests_dir = example_root / "ingests"
    streams_dir = example_root / "streams"
    _write_project_yaml(example_root / "project.yaml", sources_dir, streams_dir)
    _write_source_yaml(sources_dir / "demo.weather.yaml", "demo.weather")

    _input_sequence(monkeypatch, ["1", "1", "2", "1", "2", "", "1"])

    handle_stream_create(plugin_root=plugin_root)

    stream_path = ingests_dir / "weather.weather.yaml"
    assert stream_path.exists(), f"expected stream at {stream_path}"

    # Mapper creation is now opt-in; identity selection should not scaffold mappers.


def test_stream_identity_mapper_skips_scaffold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plugin_root = _create_plugin(tmp_path)
    example_root = plugin_root / "example"
    sources_dir = example_root / "sources"
    ingests_dir = example_root / "ingests"
    streams_dir = example_root / "streams"
    _write_project_yaml(example_root / "project.yaml", sources_dir, streams_dir)
    _write_source_yaml(sources_dir / "demo.weather.yaml", "demo.weather")

    _input_sequence(monkeypatch, ["1", "1", "2", "1", "", "1"])

    handle_stream_create(plugin_root=plugin_root, use_identity=True)

    stream_path = ingests_dir / "weather.weather.yaml"
    assert stream_path.exists()
    text = stream_path.read_text()
    assert "entrypoint: identity" in text

    # Identity mapper should not scaffold mapper code.


def test_ingest_stream_preserves_source_variant_in_default_stream_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin_root = _create_plugin(tmp_path)
    example_root = plugin_root / "example"
    sources_dir = example_root / "sources"
    ingests_dir = example_root / "ingests"
    streams_dir = example_root / "streams"
    _write_project_yaml(example_root / "project.yaml", sources_dir, streams_dir)
    _write_source_yaml(
        sources_dir / "demo.weather.benchmark.yaml",
        "demo.weather.benchmark",
    )

    _input_sequence(monkeypatch, ["1", "1", "2", "1", "", "1"])

    handle_stream_create(plugin_root=plugin_root, use_identity=True)

    assert (ingests_dir / "weather.weather.benchmark.yaml").exists()


def test_stream_identity_flag_rejects_multistream(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plugin_root = _create_plugin(tmp_path)
    _input_sequence(monkeypatch, ["2"])
    with pytest.raises(SystemExit):
        handle_stream_create(plugin_root=plugin_root, use_identity=True)


def test_aligned_stream_scaffold_writes_ordered_inputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plugin_root = _create_plugin(tmp_path)
    example_root = plugin_root / "example"
    sources_dir = example_root / "sources"
    ingests_dir = example_root / "ingests"
    streams_dir = example_root / "streams"
    _write_project_yaml(example_root / "project.yaml", sources_dir, streams_dir)
    streams_dir.mkdir(parents=True, exist_ok=True)
    _write_ingest_yaml(ingests_dir / "weather.pressure.yaml", "weather.pressure")
    _write_ingest_yaml(
        ingests_dir / "weather.temperature.yaml",
        "weather.temperature",
    )

    _input_sequence(
        monkeypatch,
        [
            "2",  # aligned stream
            "1,2",  # input stream order
            "air_density.processed",
            "1",  # custom mapper
            "map_air_density",
        ],
    )

    handle_stream_create(plugin_root=plugin_root)

    stream_path = streams_dir / "air_density.processed.yaml"
    assert stream_path.exists()
    config = yaml.safe_load(stream_path.read_text(encoding="utf-8"))
    assert config["from"]["align"] == ["weather.pressure", "weather.temperature"]
    assert config["map"] == {"entrypoint": "map_air_density", "args": {}}
