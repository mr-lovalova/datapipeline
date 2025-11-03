from pathlib import Path

import pytest

from datapipeline.config.run import load_named_run_configs, load_run_config


def _write_project(tmp_path: Path, run_ref: str | None = None) -> Path:
    project_yaml = tmp_path / "project.yaml"
    lines = [
        "version: 1",
        "paths:",
        "  streams: streams",
        "  sources: sources",
        "  dataset: dataset.yaml",
        "  postprocess: postprocess.yaml",
        "  artifacts: artifacts",
        "  build: build.yaml",
    ]
    if run_ref:
        lines.append(f"  run: {run_ref}")
    project_yaml.write_text("\n".join(lines), encoding="utf-8")
    return project_yaml


def test_load_run_config_returns_none_when_path_missing(tmp_path):
    project_yaml = _write_project(tmp_path, run_ref=None)

    cfg = load_run_config(project_yaml)

    assert cfg is None


def test_load_run_config_reads_arbitrary_keep_value(tmp_path):
    project_yaml = _write_project(tmp_path, run_ref="run.yaml")
    run_yaml = project_yaml.parent / "run.yaml"
    run_yaml.write_text("version: 1\nkeep: shadow\n", encoding="utf-8")

    cfg = load_run_config(project_yaml)

    assert cfg is not None
    assert cfg.keep == "shadow"


def test_load_run_config_parses_optional_defaults(tmp_path):
    project_yaml = _write_project(tmp_path, run_ref="run.yaml")
    run_yaml = project_yaml.parent / "run.yaml"
    run_yaml.write_text(
        """version: 1
keep: holdout
output:
  transport: stdout
  format: json-lines
limit: 5
include_targets: true
throttle_ms: 250
""",
        encoding="utf-8",
    )

    cfg = load_run_config(project_yaml)

    assert cfg.output is not None
    assert cfg.output.transport == "stdout"
    assert cfg.output.format == "json-lines"
    assert cfg.limit == 5
    assert cfg.include_targets is True
    assert cfg.throttle_ms == 250


def test_load_run_config_uses_first_file_when_directory(tmp_path):
    project_yaml = _write_project(tmp_path, run_ref="runs")
    runs_dir = project_yaml.parent / "runs"
    runs_dir.mkdir()
    (runs_dir / "b.yaml").write_text("version: 1\nkeep: bar\n", encoding="utf-8")
    (runs_dir / "a.yaml").write_text("version: 1\nkeep: foo\n", encoding="utf-8")

    cfg = load_run_config(project_yaml)

    assert cfg.keep == "foo"


def test_load_named_run_configs_returns_all_entries(tmp_path):
    project_yaml = _write_project(tmp_path, run_ref="runs")
    runs_dir = project_yaml.parent / "runs"
    runs_dir.mkdir()
    (runs_dir / "train.yaml").write_text("version: 1\nkeep: train\n", encoding="utf-8")
    (runs_dir / "val.yaml").write_text("version: 1\nkeep: val\n", encoding="utf-8")

    entries = load_named_run_configs(project_yaml)

    assert [name for name, _ in entries] == ["train", "val"]
    assert entries[0][1].keep == "train"
    assert entries[1][1].keep == "val"


def test_load_named_run_configs_raises_when_directory_empty(tmp_path):
    project_yaml = _write_project(tmp_path, run_ref="runs")
    runs_dir = project_yaml.parent / "runs"
    runs_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        load_named_run_configs(project_yaml)
