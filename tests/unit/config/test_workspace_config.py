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
          observability:
            visuals: OFF
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.observability is not None
    assert shared.observability.visuals == "OFF"


def test_shared_visuals_defaults_when_missing(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          observability:
            visuals: on
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared = context.config.shared
    assert shared.observability is not None
    assert shared.observability.visuals == "ON"


def test_workspace_rejects_serve_defaults_block(tmp_path):
    _write_jerry(
        tmp_path,
        """
        serve:
          limit: 25
        """,
    )

    try:
        load_workspace_context(tmp_path)
    except Exception as exc:
        assert "serve" in str(exc)
    else:
        raise AssertionError("Expected workspace validation failure for serve defaults")


def test_workspace_log_outputs_normalize(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          observability:
            logging:
              outputs:
                - transport: stderr
                - transport: fs
                  path: ./logs/jerry.log
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared_obs = context.config.shared.observability
    assert shared_obs and shared_obs.logging and shared_obs.logging.outputs
    assert shared_obs.logging.outputs[0].transport == "STDERR"
    assert shared_obs.logging.outputs[1].transport == "FS"
    assert shared_obs.logging.outputs[1].path == "./logs/jerry.log"


def test_workspace_log_outputs_run_scope_defaults_path(tmp_path):
    _write_jerry(
        tmp_path,
        """
        shared:
          observability:
            logging:
              outputs:
                - transport: fs
                  scope: run
        """,
    )

    context = load_workspace_context(tmp_path)
    assert context
    shared_obs = context.config.shared.observability
    assert shared_obs and shared_obs.logging and shared_obs.logging.outputs
    assert shared_obs.logging.outputs[0].transport == "FS"
    assert shared_obs.logging.outputs[0].scope == "RUN"
    assert shared_obs.logging.outputs[0].path is None


def test_workspace_rejects_build_defaults_block(tmp_path):
    _write_jerry(
        tmp_path,
        """
        build:
          mode: AUTO
        """,
    )

    try:
        load_workspace_context(tmp_path)
    except Exception as exc:
        assert "build" in str(exc)
    else:
        raise AssertionError("Expected workspace validation failure for build defaults")


def test_workspace_rejects_serve_observability_block(tmp_path):
    _write_jerry(
        tmp_path,
        """
        serve:
          observability:
            logging:
              outputs:
                - transport: fs
                  scope: run
        """,
    )

    try:
        load_workspace_context(tmp_path)
    except Exception as exc:
        assert "serve" in str(exc)
    else:
        raise AssertionError("Expected workspace validation failure for serve observability")


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
