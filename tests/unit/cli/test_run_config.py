from pathlib import Path
from types import SimpleNamespace

from datapipeline.profiles.execution import resolve_task_order, runtime_task_ids_for_order
from datapipeline.profiles.request_builder import build_profile_run_request


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


def _patch_runtime_resolution(monkeypatch) -> None:
    def _fake_iter_runtime_runs(project_path, run_entries, keep_override):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=SimpleNamespace(keep=None))
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs",
        _fake_iter_runtime_runs,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.load_dataset",
        lambda project_path, dataset_name: SimpleNamespace(name=dataset_name),
    )


def test_serve_request_resolves_targeted_profile(monkeypatch, tmp_path: Path):
    _patch_runtime_resolution(monkeypatch)
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "pipeline.yaml",
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
    )
    _write_op(
        ops / "coverage.yaml",
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\n",
    )
    (profiles / "serve.coverage.yaml").write_text(
        "cmd: serve\nname: coverage\ntarget: coverage\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="coverage",
    )
    assert request is not None
    assert request.command == "serve"
    assert len(request.profiles) == 1
    profile = request.profiles[0]
    assert profile.name == "coverage"
    assert profile.target_id == "coverage"
    assert any(task.id == "coverage" for task in request.tasks)


def test_inspect_request_defaults_to_enabled_profiles(monkeypatch, tmp_path: Path):
    _patch_runtime_resolution(monkeypatch)
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "coverage.yaml",
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\n",
    )
    _write_op(
        ops / "matrix.yaml",
        "id: matrix\nkind: runtime\nentrypoint: core.runtime.matrix\n",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        "cmd: inspect\nname: coverage\ntarget: coverage\nenabled: false\n",
        encoding="utf-8",
    )
    (profiles / "inspect.matrix.yaml").write_text(
        "cmd: inspect\nname: matrix\ntarget: matrix\nenabled: true\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="inspect",
        project=str(project_yaml),
    )
    assert request is not None
    assert request.command == "inspect"
    assert len(request.profiles) == 1
    profile = request.profiles[0]
    assert profile.name == "matrix"
    assert profile.target_id == "matrix"


def test_serve_profile_can_target_artifact_only_graph(monkeypatch, tmp_path: Path):
    _patch_runtime_resolution(monkeypatch)
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "schema.yaml",
        "id: schema\nkind: artifact\noutput: schema.json\n",
    )
    (profiles / "serve.schema.yaml").write_text(
        "cmd: serve\nname: schema\ntarget: schema\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="schema",
    )
    assert request is not None
    assert request.command == "serve"
    assert len(request.profiles) == 1
    profile = request.profiles[0]
    assert profile.target_id == "schema"

    tasks_by_id = {task.id: task for task in request.tasks}
    ordered_ids = resolve_task_order(profile, tasks_by_id)
    runtime_ids = runtime_task_ids_for_order(ordered_ids, tasks_by_id)
    assert runtime_ids == []
