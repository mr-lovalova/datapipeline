from pathlib import Path

import pytest

from datapipeline.services.runtime_entries import resolve_runtime_entries


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
        ops / "pipeline.yaml",
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\nruntime_kind: serve\n",
    )
    _write_op(
        ops / "coverage.yaml",
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\nruntime_kind: inspect\n",
    )
    (profiles / "serve.coverage.yaml").write_text(
        "type: serve\nname: coverage\ntarget: coverage\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        resolve_runtime_entries(project_yaml, run_name="coverage", kind="serve")
    assert exc.value.code == 2


def test_inspect_entries_resolve_inspect_operations(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "pipeline.yaml",
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\nruntime_kind: serve\n",
    )
    _write_op(
        ops / "coverage.yaml",
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\nruntime_kind: inspect\n",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        "type: inspect\nname: coverage\ntarget: coverage\n",
        encoding="utf-8",
    )

    entries = resolve_runtime_entries(project_yaml, run_name="coverage", kind="inspect")
    assert len(entries) == 1
    assert entries[0].operation.id == "coverage"
    assert entries[0].operation.entrypoint == "core.runtime.coverage"


def test_inspect_entries_default_to_enabled_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "coverage.yaml",
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\nruntime_kind: inspect\n",
    )
    _write_op(
        ops / "matrix.yaml",
        "id: matrix\nkind: runtime\nentrypoint: core.runtime.matrix\nruntime_kind: inspect\n",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        "type: inspect\nname: coverage\ntarget: coverage\nenabled: false\n",
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
