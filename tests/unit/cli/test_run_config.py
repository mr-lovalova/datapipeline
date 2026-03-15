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


def test_serve_request_resolves_cache_cli_override(monkeypatch, tmp_path: Path):
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
    (profiles / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\ncache: false\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        cli_cache=True,
    )
    assert request is not None
    assert request.profiles[0].cache_enabled is True


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


def test_serve_request_orders_enabled_profiles_and_run_targets_only_named_profile(
    monkeypatch,
    tmp_path: Path,
):
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
    _write_op(
        ops / "pipeline.yaml",
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
    )
    (profiles / "serve.schema.yaml").write_text(
        "cmd: serve\nname: schema\norder: 10\ntarget: schema\nenabled: true\n",
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\norder: 40\ntarget: pipeline\nenabled: true\n",
        encoding="utf-8",
    )

    request_all = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
    )
    assert request_all is not None
    assert [profile.name for profile in request_all.profiles] == ["schema", "train"]
    assert [profile.target_id for profile in request_all.profiles] == ["schema", "pipeline"]

    request_train = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request_train is not None
    assert [profile.name for profile in request_train.profiles] == ["train"]
    assert [profile.target_id for profile in request_train.profiles] == ["pipeline"]


def test_serve_defaults_apply_when_profile_omits_fields(monkeypatch, tmp_path: Path):
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
    (profiles / "serve.defaults.yaml").write_text(
        (
            "cmd: serve\n"
            "output:\n"
            "  transport: fs\n"
            "  format: jsonl\n"
            "  directory: ./artifacts/serve\n"
            "observability:\n"
            "  logging:\n"
            "    outputs:\n"
            "      - transport: stdout\n"
        ),
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request is not None
    profile = request.profiles[0]
    assert profile.output is not None
    assert profile.output.transport == "fs"
    assert profile.output.run is not None
    assert profile.log_output.outputs[0].transport == "stdout"


def test_serve_profile_fields_override_serve_defaults(monkeypatch, tmp_path: Path):
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
    (profiles / "serve.defaults.yaml").write_text(
        (
            "cmd: serve\n"
            "output:\n"
            "  transport: fs\n"
            "  format: jsonl\n"
            "  directory: ./artifacts/serve\n"
        ),
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        (
            "cmd: serve\n"
            "name: train\n"
            "target: pipeline\n"
            "output:\n"
            "  transport: stdout\n"
            "  format: jsonl\n"
        ),
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request is not None
    profile = request.profiles[0]
    assert profile.output is not None
    assert profile.output.transport == "stdout"
    assert profile.output.run is None


def test_serve_profile_nested_observability_deep_merges_defaults(
    monkeypatch,
    tmp_path: Path,
):
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
    (profiles / "serve.defaults.yaml").write_text(
        (
            "cmd: serve\n"
            "observability:\n"
            "  logging:\n"
            "    outputs:\n"
            "      - transport: stdout\n"
        ),
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        (
            "cmd: serve\n"
            "name: train\n"
            "target: pipeline\n"
            "observability:\n"
            "  logging:\n"
            "    level: debug\n"
        ),
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request is not None
    profile = request.profiles[0]
    assert profile.log_decision.name == "DEBUG"
    assert profile.log_output.outputs[0].transport == "stdout"


def test_build_defaults_apply_to_build_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)

    _write_op(
        ops / "schema.yaml",
        "id: schema\nkind: artifact\noutput: schema.json\n",
    )
    (profiles / "build.defaults.yaml").write_text(
        (
            "cmd: build\n"
            "mode: force\n"
            "observability:\n"
            "  visuals: off\n"
            "  logging:\n"
            "    level: debug\n"
        ),
        encoding="utf-8",
    )
    (profiles / "build.schema.yaml").write_text(
        "cmd: build\nname: schema\ntarget: schema\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="build",
        project=str(project_yaml),
        run_name="schema",
    )
    assert request is not None
    profile = request.profiles[0]
    assert profile.build_settings is not None
    assert profile.build_settings.mode == "FORCE"
    assert profile.build_settings.visuals == "off"
    assert profile.build_settings.log_decision.name == "DEBUG"
