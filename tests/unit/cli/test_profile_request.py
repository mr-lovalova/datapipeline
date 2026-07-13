from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.profiles.request_builder import build_profile_run_request
from datapipeline.config.resolution import LogOutputTarget


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


def test_build_request_requires_declared_build_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    (tmp_path / "tasks" / "operations").mkdir(parents=True, exist_ok=True)
    (tmp_path / "profiles").mkdir(parents=True, exist_ok=True)

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(
            kind="build",
            project=str(project_yaml),
        )
    assert exc.value.code == 2


def test_inspect_request_requires_declared_inspect_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    (tmp_path / "tasks" / "operations").mkdir(parents=True, exist_ok=True)
    (tmp_path / "profiles").mkdir(parents=True, exist_ok=True)

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(
            kind="inspect",
            project=str(project_yaml),
        )
    assert exc.value.code == 2


def test_inspect_request_materializes_execution_scoped_log_output(
    tmp_path: Path, monkeypatch
):
    execution_dir = tmp_path / "execution"
    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.execution_root",
        lambda _project: execution_dir,
    )
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    (ops / "coverage.yaml").write_text(
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\n",
        encoding="utf-8",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        (
            "cmd: inspect\n"
            "name: coverage\n"
            "target: coverage\n"
            "enabled: true\n"
            "artifact_mode: AUTO\n"
        ),
        encoding="utf-8",
    )
    (tmp_path / "sources").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.bootstrap_build_runtime",
        lambda _project_path: SimpleNamespace(split=None, execution=None),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.load_dataset",
        lambda project_path: SimpleNamespace(),
    )

    request = build_profile_run_request(
        kind="inspect",
        project=str(project_yaml),
        cli_log_outputs=[LogOutputTarget(transport="fs", scope="execution")],
    )
    assert request is not None
    job = request.jobs[0]
    assert job.observability.log_output.outputs[0].scope == "global"
    assert job.observability.log_output.outputs[0].destination == (
        execution_dir / "logs" / "inspect.coverage.log"
    )
    assert not execution_dir.exists()


def test_disabled_profiles_do_not_create_execution_directory(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    (ops / "coverage.yaml").write_text(
        "id: coverage\nkind: runtime\nentrypoint: core.runtime.coverage\n",
        encoding="utf-8",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        ("cmd: inspect\nname: coverage\ntarget: coverage\nenabled: false\n"),
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="inspect",
        project=str(project_yaml),
    )

    assert request is None
    assert not (tmp_path / "artifacts" / "_system" / "executions").exists()
