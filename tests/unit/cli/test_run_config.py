from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.profiles.request_builder import build_profile_run_request


def _write_project(tmp_path: Path) -> Path:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  ingests: ./ingests",
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
    (tmp_path / "dataset.yaml").write_text("{}\n", encoding="utf-8")
    (tmp_path / "postprocess.yaml").write_text("{}\n", encoding="utf-8")
    return project_yaml


def _write_op(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def _patch_runtime_resolution(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.bootstrap_build_runtime",
        lambda _project_path: SimpleNamespace(split=None, execution=None),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.load_dataset",
        lambda project_path: SimpleNamespace(),
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
        "target: coverage\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="coverage",
    )
    assert request is not None
    assert request.command == "serve"
    assert len(request.jobs) == 1
    job = request.jobs[0]
    assert job.name == "coverage"
    assert job.task.id == "coverage"
    assert request.artifact_settings is not None
    assert request.artifact_settings.mode == "AUTO"


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
        "target: coverage\nenabled: false\n",
        encoding="utf-8",
    )
    (profiles / "inspect.matrix.yaml").write_text(
        "target: matrix\nenabled: true\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="inspect",
        project=str(project_yaml),
    )
    assert request is not None
    assert request.command == "inspect"
    assert len(request.jobs) == 1
    job = request.jobs[0]
    assert job.name == "matrix"
    assert job.task.id == "matrix"


def test_serve_profile_rejects_artifact_target(monkeypatch, tmp_path: Path, caplog):
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
        "target: schema\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(
            kind="serve",
            project=str(project_yaml),
            run_name="schema",
        )

    assert exc.value.code == 2
    assert "must target a runtime task; 'schema' is an artifact task" in caplog.text


def test_inspect_profile_rejects_artifact_target(tmp_path: Path, caplog):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    _write_op(
        ops / "stats.yaml",
        "id: stats\nkind: artifact\nmode: raw\noutput: stats.json\n",
    )
    (profiles / "inspect.stats.yaml").write_text(
        "target: stats\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(kind="inspect", project=str(project_yaml))

    assert exc.value.code == 2
    assert "must target a runtime task; 'stats' is an artifact task" in caplog.text


def test_build_profile_rejects_runtime_target(tmp_path: Path, caplog):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    _write_op(
        ops / "pipeline.yaml",
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
    )
    (profiles / "build.pipeline.yaml").write_text(
        "target: pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(kind="build", project=str(project_yaml))

    assert exc.value.code == 2
    assert "must target an artifact task; 'pipeline' is a runtime task" in caplog.text


def test_build_profile_rejects_unknown_target(tmp_path: Path, caplog):
    project_yaml = _write_project(tmp_path)
    (tmp_path / "tasks" / "operations").mkdir(parents=True, exist_ok=True)
    profiles = tmp_path / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    (profiles / "build.typo.yaml").write_text(
        "target: scheam\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(kind="build", project=str(project_yaml))

    assert exc.value.code == 2
    assert "references unknown task target 'scheam'" in caplog.text


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
        ops / "pipeline.yaml",
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
    )
    (profiles / "serve.early.yaml").write_text(
        "order: 10\ntarget: pipeline\nenabled: true\n",
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        "order: 40\ntarget: pipeline\nenabled: true\n",
        encoding="utf-8",
    )

    request_all = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
    )
    assert request_all is not None
    assert [job.name for job in request_all.jobs] == ["early", "train"]
    assert [job.task.id for job in request_all.jobs] == [
        "pipeline",
        "pipeline",
    ]

    request_train = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request_train is not None
    assert [job.name for job in request_train.jobs] == ["train"]
    assert [job.task.id for job in request_train.jobs] == ["pipeline"]


def test_cli_artifact_mode_overrides_selected_profiles(monkeypatch, tmp_path: Path):
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
            'artifact_mode: "OFF"\n'
            "observability:\n"
            "  visuals: off\n"
            "  heartbeat_interval_seconds: 30\n"
            "  logging:\n"
            "    level: warning\n"
            "    outputs:\n"
            "      - transport: stderr\n"
            "      - transport: fs\n"
            "        scope: execution\n"
        ),
        encoding="utf-8",
    )
    (profiles / "serve.first.yaml").write_text(
        'target: pipeline\nartifact_mode: "OFF"\n',
        encoding="utf-8",
    )
    (profiles / "serve.second.yaml").write_text(
        "target: pipeline\nartifact_mode: AUTO\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        artifact_mode="force",
        cli_heartbeat_interval_seconds=0,
        cli_log_level="debug",
        cli_visuals="on",
    )

    assert request is not None
    settings = request.artifact_settings
    assert settings is not None
    assert settings.mode == "FORCE"
    assert settings.observability.heartbeat_interval_seconds == 0
    assert settings.observability.visuals == "on"
    assert settings.observability.log_decision.name == "DEBUG"
    assert [
        (
            output.transport,
            output.destination.name if output.destination is not None else None,
        )
        for output in settings.observability.log_output.outputs
    ] == [
        ("stderr", None),
        ("fs", "serve.artifacts.log"),
    ]
    assert {
        job.name: [
            (
                output.transport,
                output.destination.name if output.destination is not None else None,
            )
            for output in job.observability.log_output.outputs
        ]
        for job in request.jobs
    } == {
        "first": [("stderr", None), ("fs", "serve.first.log")],
        "second": [("stderr", None), ("fs", "serve.second.log")],
    }


def test_selected_profiles_reject_conflicting_artifact_modes(
    monkeypatch,
    tmp_path: Path,
    caplog,
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
    (profiles / "serve.first.yaml").write_text(
        'target: pipeline\nartifact_mode: "OFF"\n',
        encoding="utf-8",
    )
    (profiles / "serve.second.yaml").write_text(
        "target: pipeline\nartifact_mode: AUTO\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(kind="serve", project=str(project_yaml))

    assert exc.value.code == 2
    assert (
        "Selected serve profiles disagree on artifact_mode: first=OFF, second=AUTO."
        in caplog.text
    )


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
            "output:\n"
            "  transport: fs\n"
            "  format: jsonl\n"
            "  directory: ./artifacts/serve\n"
            "observability:\n"
            "  heartbeat_interval_seconds: 30\n"
            "  logging:\n"
            "    outputs:\n"
            "      - transport: stdout\n"
        ),
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        "target: pipeline\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request is not None
    job = request.jobs[0]
    assert job.output.transport == "fs"
    assert job.output.run is not None
    assert job.observability.log_output.outputs[0].transport == "stdout"
    assert job.observability.heartbeat_interval_seconds == 30
    assert request.artifact_settings is not None
    assert request.artifact_settings.observability.heartbeat_interval_seconds == 30
    assert len(request.serve_run_plans) == 1
    assert request.serve_run_plans[0].paths == job.output.run
    assert not (tmp_path / "artifacts" / "_system" / "executions").exists()
    assert not job.output.run.dataset_dir.exists()
    assert not job.output.run.metadata_path.exists()


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
        ("output:\n  transport: fs\n  format: jsonl\n  directory: ./artifacts/serve\n"),
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        ("target: pipeline\noutput:\n  transport: stdout\n  format: jsonl\n"),
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request is not None
    job = request.jobs[0]
    assert job.output.transport == "stdout"
    assert job.output.run is None


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
        ("observability:\n  logging:\n    outputs:\n      - transport: stdout\n"),
        encoding="utf-8",
    )
    (profiles / "serve.train.yaml").write_text(
        ("target: pipeline\nobservability:\n  logging:\n    level: debug\n"),
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="serve",
        project=str(project_yaml),
        run_name="train",
    )
    assert request is not None
    job = request.jobs[0]
    assert job.observability.log_decision.name == "DEBUG"
    assert job.observability.log_output.outputs[0].transport == "stdout"


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
            "mode: force\n"
            "execution:\n"
            "  sort_buffer_mb: 256\n"
            "observability:\n"
            "  visuals: off\n"
            "  logging:\n"
            "    level: debug\n"
        ),
        encoding="utf-8",
    )
    (profiles / "build.schema.yaml").write_text(
        "target: schema\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="build",
        project=str(project_yaml),
        run_name="schema",
    )
    assert request is not None
    assert request.config_hash is not None
    assert request.execution.sort_buffer_mb == 256
    job = request.jobs[0]
    assert job.settings.mode == "FORCE"
    assert job.settings.observability.visuals == "off"
    assert job.settings.observability.log_decision.name == "DEBUG"
