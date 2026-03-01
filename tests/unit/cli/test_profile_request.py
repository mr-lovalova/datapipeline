from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.cli.commands.profile_request import build_profile_run_request
from datapipeline.config.resolution import LogOutputTarget


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


def test_inspect_request_rejects_run_scoped_log_output(tmp_path: Path, monkeypatch):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "tasks" / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    (ops / "report.yaml").write_text(
        "id: report\nentrypoint: core.inspect.report\n",
        encoding="utf-8",
    )
    (profiles / "inspect.report.yaml").write_text(
        "type: inspect\nname: report\ntarget: report\nenabled: true\n",
        encoding="utf-8",
    )
    (tmp_path / "sources").mkdir(parents=True, exist_ok=True)

    def _fake_iter_runtime_runs(project_path, run_entries, keep_override):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=SimpleNamespace(keep=None))
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs",
        _fake_iter_runtime_runs,
    )

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(
            kind="inspect",
            project=str(project_yaml),
            cli_log_outputs=[LogOutputTarget(transport="fs", scope="run")],
        )
    assert exc.value.code == 2
