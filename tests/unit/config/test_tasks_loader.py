from pathlib import Path

import pytest

from datapipeline.config.tasks import (
    artifact_tasks,
    build_tasks,
    default_serve_task,
    serve_operation_tasks,
    serve_tasks,
)


def _write_project(tmp_path: Path, tasks_ref: str | None = None) -> Path:
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
    if tasks_ref:
        lines.append(f"  tasks: {tasks_ref}")
    project_yaml.write_text("\n".join(lines), encoding="utf-8")
    return project_yaml


def test_artifact_tasks_load_configs(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "schema.yaml").write_text(
        "kind: schema\noutput: schema.json\n", encoding="utf-8"
    )
    (tasks_dir / "scaler.yaml").write_text(
        "kind: scaler\nsplit_label: all\noutput: stats.pkl\n", encoding="utf-8"
    )

    tasks = artifact_tasks(project_yaml)

    kinds = sorted(task.kind for task in tasks)
    assert kinds == ["metadata", "scaler", "schema"]
    schema = next(task for task in tasks if task.kind == "schema")
    assert schema.output == "schema.json"
    scaler = next(task for task in tasks if task.kind == "scaler")
    assert scaler.split_label == "all"
    assert scaler.output == "stats.pkl"


def test_serve_tasks_respect_name_and_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "serve.train.yaml").write_text(
        "kind: serve\nname: train\ntarget: serve\nkeep: train\n", encoding="utf-8"
    )
    (tasks_dir / "serve.val.yaml").write_text(
        "kind: serve\ntarget: serve\nkeep: val\nenabled: false\n", encoding="utf-8"
    )

    tasks = serve_tasks(project_yaml)

    assert [task.effective_name() for task in tasks] == ["train", "serve.val"]
    default = default_serve_task(project_yaml)
    assert default is not None
    assert default.keep == "train"


def test_default_serve_task_returns_none_when_missing(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()

    assert default_serve_task(project_yaml) is None


def test_build_profiles_load_and_respect_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "build.fast.yaml").write_text(
        "kind: build\nname: fast\nenabled: true\nmode: auto\ntarget: schema\n",
        encoding="utf-8",
    )
    (tasks_dir / "build.full.yaml").write_text(
        "kind: build\nname: full\nenabled: false\nmode: force\ntarget: metadata\n",
        encoding="utf-8",
    )

    tasks = build_tasks(project_yaml)
    assert [task.effective_name() for task in tasks] == ["fast", "full"]
    assert tasks[0].target == "schema"
    assert tasks[0].enabled is True
    assert tasks[1].enabled is False


def test_artifact_task_dependencies_are_normalized(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "metadata.yaml").write_text(
        "kind: metadata\ndependencies:\n  - schema\n  - SCHEMA\n",
        encoding="utf-8",
    )

    tasks = artifact_tasks(project_yaml)
    metadata = next(task for task in tasks if task.kind == "metadata")
    assert metadata.dependencies == ["schema"]


def test_serve_operation_tasks_load(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "serve_operation.yaml").write_text(
        "kind: serve_operation\nname: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )

    tasks = serve_operation_tasks(project_yaml)
    assert [task.effective_name() for task in tasks] == ["serve"]
    assert tasks[0].entrypoint == "core.serve_pipeline"


def test_duplicate_artifact_kinds_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "schema.a.yaml").write_text(
        "kind: schema\nname: schema_a\noutput: schema-a.json\n", encoding="utf-8"
    )
    (tasks_dir / "schema.b.yaml").write_text(
        "kind: schema\nname: schema_b\noutput: schema-b.json\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Duplicate artifact task kinds"):
        artifact_tasks(project_yaml)


def test_duplicate_serve_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "serve.a.yaml").write_text(
        "kind: serve\nname: train\ntarget: serve\n", encoding="utf-8"
    )
    (tasks_dir / "serve.b.yaml").write_text(
        "kind: serve\nname: train\ntarget: serve\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Duplicate serve profile names"):
        serve_tasks(project_yaml)


def test_duplicate_serve_operation_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "serve_operation.a.yaml").write_text(
        "kind: serve_operation\nname: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )
    (tasks_dir / "serve_operation.b.yaml").write_text(
        "kind: serve_operation\nname: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate serve operation names"):
        serve_operation_tasks(project_yaml)


def test_duplicate_build_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "build.a.yaml").write_text(
        "kind: build\nname: nightly\ntarget: schema\n",
        encoding="utf-8",
    )
    (tasks_dir / "build.b.yaml").write_text(
        "kind: build\nname: nightly\ntarget: metadata\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate build profile names"):
        build_tasks(project_yaml)
