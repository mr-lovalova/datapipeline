from pathlib import Path

from datapipeline.config.run import load_run_config


def _write_project(tmp_path: Path, include_run: bool = False) -> Path:
    project_yaml = tmp_path / "project.yaml"
    lines = [
        "version: 1",
        "paths:",
        "  streams: streams",
        "  sources: sources",
        "  dataset: dataset.yaml",
        "  postprocess: postprocess.yaml",
        "  artifacts: artifacts",
    ]
    if include_run:
        lines.append("  run: run.yaml")
    project_yaml.write_text("\n".join(lines), encoding="utf-8")
    return project_yaml


def test_load_run_config_returns_none_when_path_missing(tmp_path):
    project_yaml = _write_project(tmp_path, include_run=False)

    cfg = load_run_config(project_yaml)

    assert cfg is None


def test_load_run_config_reads_arbitrary_keep_value(tmp_path):
    project_yaml = _write_project(tmp_path, include_run=True)
    run_yaml = project_yaml.parent / "run.yaml"
    run_yaml.write_text("version: 1\nkeep: shadow\n", encoding="utf-8")

    cfg = load_run_config(project_yaml)

    assert cfg is not None
    assert cfg.keep == "shadow"


def test_load_run_config_parses_optional_defaults(tmp_path):
    project_yaml = _write_project(tmp_path, include_run=True)
    run_yaml = project_yaml.parent / "run.yaml"
    run_yaml.write_text(
        """version: 1
keep: holdout
output: stream
limit: 5
include_targets: true
throttle_ms: 250
""",
        encoding="utf-8",
    )

    cfg = load_run_config(project_yaml)

    assert cfg.output == "stream"
    assert cfg.limit == 5
    assert cfg.include_targets is True
    assert cfg.throttle_ms == 250
