from pathlib import Path

from datapipeline.config.tasks import (
    artifact_tasks,
    default_serve_task,
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
        "kind: schema\noutput: schema.json\ninclude_targets: true\n", encoding="utf-8"
    )
    (tasks_dir / "scaler.yaml").write_text(
        "kind: scaler\nsplit_label: all\noutput: stats.pkl\n", encoding="utf-8"
    )

    tasks = artifact_tasks(project_yaml)

    kinds = sorted(task.kind for task in tasks)
    assert kinds == ["scaler", "schema"]
    schema = next(task for task in tasks if task.kind == "schema")
    assert schema.include_targets is True
    scaler = next(task for task in tasks if task.kind == "scaler")
    assert scaler.split_label == "all"
    assert scaler.output == "stats.pkl"


def test_serve_tasks_respect_name_and_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = project_yaml.parent / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "serve.train.yaml").write_text(
        "kind: serve\nname: train\nkeep: train\n", encoding="utf-8"
    )
    (tasks_dir / "serve.val.yaml").write_text(
        "kind: serve\nkeep: val\nenabled: false\n", encoding="utf-8"
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
