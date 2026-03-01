from pathlib import Path

import pytest

from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import profile_specs


def _artifact_tasks(project_yaml: Path):
    artifact_specs, _ = operation_specs(project_yaml)
    return list(artifact_specs)


def _all_tasks(project_yaml: Path):
    artifact_specs, operation_task_specs = operation_specs(project_yaml)
    return list(artifact_specs) + list(operation_task_specs)


def _serve_profiles(project_yaml: Path):
    serve_specs, _, _ = profile_specs(project_yaml)
    return list(serve_specs)


def _build_profiles(project_yaml: Path):
    _, build_specs, _ = profile_specs(project_yaml)
    return list(build_specs)


def _inspect_profiles(project_yaml: Path):
    _, _, inspect_specs = profile_specs(project_yaml)
    return list(inspect_specs)


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


def _operations_dir(project_yaml: Path) -> Path:
    path = project_yaml.parent / "tasks" / "operations"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _profiles_dir(project_yaml: Path) -> Path:
    path = project_yaml.parent / "profiles"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _profile_kind_dir(project_yaml: Path) -> Path:
    path = _profiles_dir(project_yaml)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_artifact_tasks_load_configs(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "schema.yaml").write_text(
        "id: schema\noutput: schema.json\n", encoding="utf-8"
    )
    (tasks_dir / "scaler.yaml").write_text(
        "id: scaler\nsplit_label: all\noutput: stats.pkl\n", encoding="utf-8"
    )

    tasks = _artifact_tasks(project_yaml)

    ids = sorted(task.id for task in tasks)
    assert ids == ["scaler", "schema"]
    schema = next(task for task in tasks if task.id == "schema")
    assert schema.output == "schema.json"
    scaler = next(task for task in tasks if task.id == "scaler")
    assert scaler.split_label == "all"
    assert scaler.output == "stats.pkl"


def test_stats_task_loads_configs(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "stats.yaml").write_text(
        "id: stats\noutput: build/custom-stats.json\nmode: raw\n",
        encoding="utf-8",
    )

    tasks = _artifact_tasks(project_yaml)
    assert [task.id for task in tasks] == ["stats"]
    stats = tasks[0]
    assert stats.output == "build/custom-stats.json"
    assert stats.mode == "raw"


def test_stats_task_requires_explicit_mode(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "stats.yaml").write_text(
        "id: stats\noutput: build/custom-stats.json\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="mode|Field required"):
        _artifact_tasks(project_yaml)


def test_serve_tasks_respect_name_and_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "serve.train.yaml").write_text(
        "type: serve\nname: train\ntarget: serve\nkeep: train\n", encoding="utf-8"
    )
    (tasks_dir / "serve.val.yaml").write_text(
        "type: serve\nname: val\ntarget: serve\nkeep: val\nenabled: false\n", encoding="utf-8"
    )

    tasks = _serve_profiles(project_yaml)

    assert [task.name for task in tasks] == ["train", "val"]
    assert [task.keep for task in tasks if task.enabled] == ["train"]


def test_profile_name_is_required(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "serve.train.yaml").write_text(
        "type: serve\ntarget: serve\nkeep: train\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="serve\\.name|Field required"):
        _serve_profiles(project_yaml)


def test_build_profiles_load_and_respect_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "build.fast.yaml").write_text(
        "type: build\nname: fast\nenabled: true\nmode: auto\ntarget: schema\n",
        encoding="utf-8",
    )
    (tasks_dir / "build.full.yaml").write_text(
        "type: build\nname: full\nenabled: false\nmode: force\ntarget: metadata\n",
        encoding="utf-8",
    )

    tasks = _build_profiles(project_yaml)
    assert [task.name for task in tasks] == ["fast", "full"]
    assert tasks[0].target == "schema"
    assert tasks[0].enabled is True
    assert tasks[1].enabled is False


def test_inspect_profiles_load_and_respect_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "inspect.default.yaml").write_text(
        "type: inspect\nname: default\nenabled: true\ntarget: coverage\n",
        encoding="utf-8",
    )
    (tasks_dir / "inspect.extra.yaml").write_text(
        "type: inspect\nname: extra\nenabled: false\ntarget: coverage\n",
        encoding="utf-8",
    )

    tasks = _inspect_profiles(project_yaml)
    assert [task.name for task in tasks] == ["default", "extra"]
    assert tasks[0].target == "coverage"
    assert tasks[0].enabled is True
    assert tasks[1].enabled is False


def test_artifact_task_dependencies_are_normalized(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "metadata.yaml").write_text(
        "id: metadata\ndependencies:\n  - schema\n  - SCHEMA\n",
        encoding="utf-8",
    )

    tasks = _artifact_tasks(project_yaml)
    metadata = next(task for task in tasks if task.id == "metadata")
    assert metadata.dependencies == ["schema"]


def test_serve_operation_tasks_load(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "serve.yaml").write_text(
        "id: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )

    tasks = _all_tasks(project_yaml)
    assert [task.id for task in tasks] == ["serve"]
    assert tasks[0].entrypoint == "core.serve_pipeline"


def test_operation_tasks_do_not_inject_missing_artifacts(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "serve.yaml").write_text(
        "id: serve\nentrypoint: core.serve_pipeline\ndependencies:\n  - missing_artifact\n",
        encoding="utf-8",
    )

    tasks = _all_tasks(project_yaml)
    ids = {task.id for task in tasks}
    assert "serve" in ids
    assert "missing_artifact" not in ids


def test_operation_tasks_load_declared_artifact_dependency(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "schema.yaml").write_text(
        "id: schema\noutput: schema.json\n",
        encoding="utf-8",
    )
    (tasks_dir / "serve.yaml").write_text(
        "id: serve\nentrypoint: core.serve_pipeline\ndependencies:\n  - schema\n",
        encoding="utf-8",
    )

    tasks = _all_tasks(project_yaml)
    ids = {task.id for task in tasks}
    assert "serve" in ids
    assert "schema" in ids


def test_duplicate_artifact_kinds_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "schema.a.yaml").write_text(
        "id: schema\noutput: schema-a.json\n", encoding="utf-8"
    )
    (tasks_dir / "schema.b.yaml").write_text(
        "id: schema\noutput: schema-b.json\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Duplicate artifact task ids"):
        _artifact_tasks(project_yaml)


def test_duplicate_serve_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "serve.a.yaml").write_text(
        "type: serve\nname: train\ntarget: serve\n", encoding="utf-8"
    )
    (tasks_dir / "serve.b.yaml").write_text(
        "type: serve\nname: train\ntarget: serve\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Duplicate serve profile names"):
        _serve_profiles(project_yaml)


def test_duplicate_serve_operation_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "serve.a.yaml").write_text(
        "id: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )
    (tasks_dir / "serve.b.yaml").write_text(
        "id: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate operation task ids"):
        _all_tasks(project_yaml)


def test_duplicate_build_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "build.a.yaml").write_text(
        "type: build\nname: nightly\ntarget: schema\n",
        encoding="utf-8",
    )
    (tasks_dir / "build.b.yaml").write_text(
        "type: build\nname: nightly\ntarget: metadata\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate build profile names"):
        _build_profiles(project_yaml)


def test_duplicate_inspect_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "inspect.a.yaml").write_text(
        "type: inspect\nname: inspect\ntarget: coverage\n",
        encoding="utf-8",
    )
    (tasks_dir / "inspect.b.yaml").write_text(
        "type: inspect\nname: inspect\ntarget: coverage\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate inspect profile names"):
        _inspect_profiles(project_yaml)


def test_root_level_task_files_are_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_root = project_yaml.parent / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)
    (tasks_root / "serve.yaml").write_text(
        "id: serve\nentrypoint: core.serve_pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="tasks/operations"):
        _all_tasks(project_yaml)


def test_nested_profile_files_are_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_root = _profiles_dir(project_yaml) / "serve"
    profiles_root.mkdir(parents=True, exist_ok=True)
    (profiles_root / "train.yaml").write_text(
        "type: serve\nname: train\ntarget: serve\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="flat under profiles/"):
        _serve_profiles(project_yaml)


def test_profile_kind_directory_mismatch_is_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    build_dir = _profile_kind_dir(project_yaml)
    (build_dir / "build.oops.yaml").write_text(
        "type: serve\nname: oops\ntarget: serve\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="declares profile type 'serve'"):
        _build_profiles(project_yaml)


def test_profile_filename_prefix_is_required(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_root = _profiles_dir(project_yaml)
    (profiles_root / "oops.yaml").write_text(
        "type: serve\nname: oops\ntarget: serve\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must use \\{serve,build,inspect\\}"):
        _serve_profiles(project_yaml)
