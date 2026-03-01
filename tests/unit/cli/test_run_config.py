from pathlib import Path

import pytest

from datapipeline.cli.commands.run_config import resolve_runtime_entries


def _write_project(tmp_path: Path) -> Path:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: artifacts",
                "  tasks: tasks",
                "  profiles: profiles",
            ]
        ),
        encoding="utf-8",
    )
    return project_yaml


def _write_op(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def test_serve_entries_reject_inspect_target(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "serve.yaml",
        "id: serve\nentrypoint: core.serve_pipeline\n",
    )
    _write_op(
        ops / "report.yaml",
        "id: report\nentrypoint: core.inspect.report\n",
    )
    (profiles / "serve.report.yaml").write_text(
        "type: serve\nname: report\ntarget: report\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        resolve_runtime_entries(project_yaml, run_name="report", kind="serve")
    assert exc.value.code == 2


def test_inspect_entries_resolve_inspect_operations(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "serve.yaml",
        "id: serve\nentrypoint: core.serve_pipeline\n",
    )
    _write_op(
        ops / "report.yaml",
        "id: report\nentrypoint: core.inspect.report\n",
    )
    (profiles / "inspect.report.yaml").write_text(
        "type: inspect\nname: report\ntarget: report\n",
        encoding="utf-8",
    )

    entries = resolve_runtime_entries(project_yaml, run_name="report", kind="inspect")
    assert len(entries) == 1
    assert entries[0].operation.id == "report"
    assert entries[0].operation.entrypoint == "core.inspect.report"


def test_inspect_entries_default_to_enabled_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "report.yaml",
        "id: report\nentrypoint: core.inspect.report\n",
    )
    _write_op(
        ops / "matrix.yaml",
        "id: matrix\nentrypoint: core.inspect.matrix\n",
    )
    (profiles / "inspect.report.yaml").write_text(
        "type: inspect\nname: report\ntarget: report\nenabled: false\n",
        encoding="utf-8",
    )
    (profiles / "inspect.matrix.yaml").write_text(
        "type: inspect\nname: matrix\ntarget: matrix\nenabled: true\n",
        encoding="utf-8",
    )

    entries = resolve_runtime_entries(project_yaml, run_name=None, kind="inspect")
    assert len(entries) == 1
    assert entries[0].name == "matrix"
    assert entries[0].operation.id == "matrix"
