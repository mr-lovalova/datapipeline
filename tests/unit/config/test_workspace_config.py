import textwrap
from pathlib import Path

from datapipeline.config.workspace import load_workspace_context


def _write_jerry(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "jerry.yaml"
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    return path


def test_shared_visuals_config(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visuals: OFF
          progress: bars
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visuals == "OFF"
    assert shared.progress == "BARS"


def test_shared_visuals_defaults_when_missing(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          visuals: auto
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.visuals == "AUTO"
    assert shared.progress is None


def test_workspace_serve_defaults_limit(tmp_path):
    _write_jerry(
        tmp_path,
        """
        serve:
          limit: 25
          throttle_ms: 100
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    assert context.config.serve.limit == 25
    assert context.config.serve.throttle_ms == 100


def test_workspace_resolve_dataset_alias(tmp_path: Path):
    _write_jerry(
        tmp_path,
        """
        datasets:
          example: example/project.yaml
        default_dataset: example
        """,
    )
    (tmp_path / "example").mkdir(parents=True, exist_ok=True)
    (tmp_path / "example" / "project.yaml").write_text("version: 1\nname: x\npaths:\n  streams: ./contracts\n  sources: ./sources\n  dataset: dataset.yaml\n  postprocess: postprocess.yaml\n  artifacts: ./artifacts\n  tasks: ./tasks\n", encoding="utf-8")

    context = load_workspace_context(tmp_path)
    assert context
    resolved = context.resolve_dataset_alias("example")
    assert resolved is not None
    assert resolved.name == "project.yaml"
